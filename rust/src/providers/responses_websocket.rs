use super::openai_responses_streaming::{ResponsesToolAccumulator, parse_openai_responses_event};
use super::{StreamChunk, responses_control::ResponsesControl};
use crate::error::{Error, Result};
use futures::{SinkExt, Stream, StreamExt};
use serde_json::{Value, json};
use std::{pin::Pin, time::Duration};

#[derive(Default)]
struct WireOutputDiagnostics {
    whitespace_chars: usize,
    frames: usize,
    previous_sequence: Option<u64>,
    non_increasing_sequences: usize,
    reported: bool,
}

impl WireOutputDiagnostics {
    fn observe(&mut self, event: &Value, response_id: Option<&str>) -> Option<Value> {
        if event["type"] == "response.created" {
            *self = Self::default();
        }
        if !matches!(
            event["type"].as_str(),
            Some("response.output_text.delta" | "response.content_part.delta")
        ) {
            return None;
        }
        let delta = event["delta"].as_str()?;
        self.frames += 1;
        if let Some(sequence) = event["sequence_number"].as_u64() {
            if self
                .previous_sequence
                .is_some_and(|previous| sequence <= previous)
            {
                self.non_increasing_sequences += 1;
            }
            self.previous_sequence = Some(sequence);
        }
        for character in delta.chars() {
            self.whitespace_chars = if character.is_whitespace() {
                self.whitespace_chars + 1
            } else {
                0
            };
            if self.whitespace_chars >= 1024 && !self.reported {
                self.reported = true;
                return Some(json!({
                    "responseId": response_id,
                    "itemId": event["item_id"],
                    "wireEvent": event["type"],
                    "sequenceNumber": self.previous_sequence,
                    "nonIncreasingSequences": self.non_increasing_sequences,
                    "contentFrames": self.frames,
                    "consecutiveWhitespaceChars": self.whitespace_chars
                }));
            }
        }
        None
    }
}

pub async fn connect(
    builder: tokio_websockets::ClientBuilder<'static>,
    mut body: Value,
    timeout: Option<Duration>,
    control: ResponsesControl,
) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
    let timeout = timeout.unwrap_or(Duration::from_secs(180));
    let (mut socket, handshake) = tokio::time::timeout(timeout, builder.connect())
        .await
        .map_err(|_| Error::Inference("Responses WebSocket connection timed out".into()))?
        .map_err(|error| Error::Inference(error.to_string()))?;
    body.as_object_mut().unwrap().remove("stream");
    body["type"] = json!("response.create");
    socket
        .send(tokio_websockets::Message::text(body.to_string()))
        .await
        .map_err(|error| Error::Inference(error.to_string()))?;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let rate_limits =
        super::openai_codex_responses::codex_rate_limits_from_headers(handshake.headers());
    if !rate_limits.is_empty() {
        let _ = sender.send(Ok(StreamChunk::RateLimits(rate_limits)));
    }
    tokio::spawn(async move {
        let mut commands = control.receiver.lock().await;
        control
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let result: Result<()> = async {
            let mut accumulator = ResponsesToolAccumulator::new();
            let mut response_id: Option<String> = None;
            let mut pending = 0usize;
            let mut accepted = Vec::<Value>::new();
            let mut terminal_seen = false;
            let mut completed_response = false;
            let mut wire_diagnostics = WireOutputDiagnostics::default();
            loop {
                tokio::select! {
                    _ = sender.closed() => return Ok(()),
                    command = commands.recv() => {
                        let Some(command) = command else { return Ok(()); };
                        let Some(current_id) = &response_id else {
                            let _ = command.reply.send(Err(Error::NonRetryable("Wait for response.created before steering".into())));
                            continue;
                        };
                        let valid = if command.kind == "response.steer" {
                            command.input.as_str().is_some_and(|text| !text.is_empty()) || command.input.as_array().is_some_and(|items| !items.is_empty())
                        } else { command.input.is_array() };
                        if !valid {
                            let _ = command.reply.send(Err(Error::NonRetryable("Invalid Responses control input".into())));
                            continue;
                        }
                        let mut request = if command.kind == "response.create" { body.clone() } else { json!({}) };
                        request["type"] = json!(command.kind);
                        request["previous_response_id"] = json!(current_id);
                        request["input"] = command.input;
                        if let Err(error) = socket.send(tokio_websockets::Message::text(request.to_string())).await {
                            let _ = command.reply.send(Err(Error::Inference(error.to_string())));
                            return Err(Error::Inference("Responses control send failed".into()));
                        }
                        if command.kind == "response.steer" { pending += 1; }
                        if command.kind == "response.create" { completed_response = false; }
                        let _ = command.reply.send(Ok(()));
                    }
                    incoming = tokio::time::timeout(timeout, socket.next()) => {
                        let message = match incoming {
                            Ok(Some(Ok(message))) if !message.is_close() => message,
                            _ if completed_response => return Ok(()),
                            Err(_) => return Err(Error::Inference("Responses WebSocket idle timeout".into())),
                            Ok(Some(Err(error))) => return Err(Error::Inference(error.to_string())),
                            _ => return Err(Error::Inference("Responses WebSocket closed before completion".into())),
                        };
                        let Some(text) = message.as_text() else { continue; };
                        let mut event: Value = serde_json::from_str(text)?;
                        let kind = event["type"].as_str().unwrap_or("").to_owned();
                        if let Some(diagnostic) = wire_diagnostics.observe(&event, response_id.as_deref()) {
                            eprintln!("Responses wire whitespace anomaly: {diagnostic}");
                        }
                        let rate_limits = super::openai_codex_responses::codex_rate_limits_from_event(&event);
                        if !rate_limits.is_empty() { let _ = sender.send(Ok(StreamChunk::RateLimits(rate_limits))); }
                        if kind == "response.created" {
                            terminal_seen = false;
                            completed_response = false;
                            let next_id = event["response"]["id"].as_str().map(str::to_owned);
                            if response_id.is_some() && next_id != response_id {
                                for steer in accepted.drain(..) {
                                    let _ = sender.send(Ok(StreamChunk::Steering(json!({"status":"applied", "steer":steer, "responseId":next_id}))));
                                    pending = pending.saturating_sub(1);
                                }
                                accumulator = ResponsesToolAccumulator::new();
                            }
                            response_id = next_id;
                            if let Some(id) = &response_id { let _ = sender.send(Ok(StreamChunk::ResponseId(id.clone()))); }
                        }
                        if let Some(status) = kind.strip_prefix("response.steer.") {
                            if status == "accepted" { accepted.push(event["steer"].clone()); }
                            if status == "failed" {
                                pending = pending.saturating_sub(1);
                                accepted.retain(|steer| steer["id"] != event["steer"]["id"]);
                            }
                            event["status"] = json!(status);
                            let _ = sender.send(Ok(StreamChunk::Steering(event)));
                            if terminal_seen && pending == 0 { return Ok(()); }
                            continue;
                        }
                        if kind == "error" || kind == "response.failed" {
                            return Err(Error::NonRetryable(event.to_string()));
                        }
                        let terminal = matches!(kind.as_str(), "response.completed" | "response.done" | "response.incomplete");
                        if terminal {
                            terminal_seen = true;
                            completed_response = kind != "response.incomplete";
                            if kind == "response.incomplete" && event["response"]["incomplete_details"]["reason"] != "steered" {
                                return Err(Error::Inference(event.to_string()));
                            }
                            event["type"] = json!("response.done");
                            event["response"]["model"] = body["model"].clone();
                        }
                        for chunk in parse_openai_responses_event(&event, &mut accumulator, true) {
                            if sender.send(Ok(chunk)).is_err() { return Ok(()); }
                        }
                        if terminal && pending == 0 { return Ok(()); }
                    }
                }
            }
        }.await;
        commands.close();
        control
            .ready
            .store(false, std::sync::atomic::Ordering::Release);
        while let Ok(command) = commands.try_recv() {
            let _ = command.reply.send(Err(Error::NonRetryable(
                "Responses control is closed".into(),
            )));
        }
        if let Err(error) = result {
            let _ = sender.send(Err(error));
        }
    });
    Ok(Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::openai_responses::OpenAIResponsesClient;
    use crate::providers::responses_control::ResponsesOptions;
    use crate::providers::{GenerationConfig, InferenceClient};
    use tokio::net::TcpListener;
    use tokio_websockets::{Message, ServerBuilder};

    #[test]
    fn wire_diagnostics_identify_upstream_whitespace_without_logging_content() {
        let mut diagnostic = WireOutputDiagnostics::default();
        for sequence in 1..1024 {
            assert!(diagnostic.observe(&json!({"type":"response.output_text.delta", "sequence_number":sequence, "delta":" "}), Some("resp_fixture")).is_none());
        }
        let report = diagnostic.observe(&json!({"type":"response.output_text.delta", "sequence_number":1024, "delta":"\n", "item_id":"msg_fixture"}), Some("resp_fixture")).unwrap();
        assert_eq!(report["responseId"], "resp_fixture");
        assert_eq!(report["contentFrames"], 1024);
        assert_eq!(report["nonIncreasingSequences"], 0);
        assert!(report.get("delta").is_none());
        assert!(diagnostic.observe(&json!({"type":"response.output_text.delta", "sequence_number":1025, "delta":" "}), Some("resp_fixture")).is_none());
    }

    #[tokio::test]
    async fn wire_content_is_forwarded_once_without_synthesizing_whitespace() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fragments = vec!["Before".to_owned(), " \n\n".repeat(400), "After".to_owned()];
        let expected = fragments.concat();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::text(
                    json!({"type":"response.created","response":{"id":"resp_wire"}}).to_string(),
                ))
                .await
                .unwrap();
            for (index, fragment) in fragments.iter().enumerate() {
                socket.send(Message::text(json!({"type":"response.output_text.delta","sequence_number":index + 1,"item_id":"msg_wire","delta":fragment}).to_string())).await.unwrap();
            }
            let item = json!({"type":"message","id":"msg_wire","role":"assistant","content":[{"type":"output_text","text":fragments.concat()}]});
            socket
                .send(Message::text(
                    json!({"type":"response.output_item.done","output_index":0,"item":item})
                        .to_string(),
                ))
                .await
                .unwrap();
            socket.send(Message::text(json!({"type":"response.completed","response":{"id":"resp_wire","output":[item]}}).to_string())).await.unwrap();
        });
        let client = OpenAIResponsesClient::new("fixture", "gpt-6-astra")
            .with_api_url(format!("http://{address}/v1/responses"));
        let mut config = GenerationConfig::new("gpt-6-astra");
        config.responses = Some(ResponsesOptions {
            websocket: true,
            control: Some(ResponsesControl::default()),
            ..Default::default()
        });
        let mut stream = client.connect_and_listen(&[], &config).await.unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(chunk) = stream.next().await {
                if let StreamChunk::Content(text) = chunk.unwrap() {
                    received.push(text);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received.len(), 3);
        assert_eq!(received.concat(), expected);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_preserves_completed_work_but_not_an_unfinished_continuation() {
        for (mode, ending) in ["completed", "unfinished", "continuation", "steered"]
            .into_iter()
            .flat_map(|mode| ["close", "drop", "timeout"].map(|ending| (mode, ending)))
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
                socket.next().await.unwrap().unwrap();
                socket
                    .send(Message::text(
                        json!({"type":"response.created","response":{"id":"resp_1"}}).to_string(),
                    ))
                    .await
                    .unwrap();
                socket.next().await.unwrap().unwrap();
                socket
                    .send(Message::text(
                        json!({"type":"response.steer.accepted","steer":{"id":"steer_1"}})
                            .to_string(),
                    ))
                    .await
                    .unwrap();
                if mode != "unfinished" {
                    let terminal = if mode == "steered" {
                        "response.incomplete"
                    } else {
                        "response.completed"
                    };
                    socket.send(Message::text(json!({"type":terminal,"response":{"id":"resp_1","incomplete_details":{"reason":"steered"},"output":[{"type":"function_call","call_id":"call_1","name":"todo_list","arguments":"{}"}]}}).to_string())).await.unwrap();
                }
                if mode == "continuation" {
                    socket.send(Message::text(json!({"type":"response.steer.pending","steer":{"id":"steer_1"},"required_input":[]}).to_string())).await.unwrap();
                    socket.next().await.unwrap().unwrap();
                }
                if ending == "close" {
                    socket.send(Message::close(None, "")).await.unwrap();
                }
                if ending == "timeout" {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
            let control = ResponsesControl::default();
            let mut config = GenerationConfig::new("gpt-6-astra");
            config.timeout = Some(Duration::from_millis(250));
            config.responses = Some(ResponsesOptions {
                websocket: true,
                control: Some(control.clone()),
                ..Default::default()
            });
            let client = OpenAIResponsesClient::new("fixture", "gpt-6-astra")
                .with_api_url(format!("http://{address}/v1/responses"));
            let mut stream = client.connect_and_listen(&[], &config).await.unwrap();
            let mut completed = false;
            let mut failed = false;
            let mut applied = false;
            let mut sent = false;
            tokio::time::timeout(Duration::from_secs(3), async {
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(StreamChunk::ResponseId(_)) if !sent => {
                            sent = true;
                            control
                                .send("response.steer", json!("Subagent completed"))
                                .await
                                .unwrap();
                        }
                        Ok(StreamChunk::ResponseItems(_)) => completed = true,
                        Ok(StreamChunk::Steering(event)) => {
                            applied |= event["status"] == "applied";
                            if event["status"] == "pending" {
                                control.send("response.create", json!([])).await.unwrap();
                            }
                        }
                        Err(_) => failed = true,
                        _ => {}
                    }
                }
            })
            .await
            .unwrap();
            server.await.unwrap();
            assert_eq!(completed, mode != "unfinished", "{mode}");
            assert_eq!(failed, mode != "completed", "{mode}/{ending}");
            assert!(!applied, "unacknowledged steering must remain undelivered");
        }
    }

    #[tokio::test]
    async fn steering_keeps_the_same_connection_through_acceptance_required_input_and_application()
    {
        for mode in ["automatic", "pending", "failed"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
                let request = socket.next().await.unwrap().unwrap();
                let request: Value = serde_json::from_str(request.as_text().unwrap()).unwrap();
                assert_eq!(request["type"], "response.create");
                assert!(request.get("temperature").is_none());
                assert!(request.get("context_management").is_none());
                socket
                    .send(Message::text(
                        json!({"type":"response.created", "response":{"id":"resp_1"}}).to_string(),
                    ))
                    .await
                    .unwrap();
                let steer = socket.next().await.unwrap().unwrap();
                let steer: Value = serde_json::from_str(steer.as_text().unwrap()).unwrap();
                assert_eq!(
                    steer,
                    json!({"type":"response.steer", "previous_response_id":"resp_1", "input":"Change direction"})
                );
                let identity = json!({"id":"steer_1", "previous_response_id":"resp_1"});
                socket
                    .send(Message::text(
                        json!({"type":"response.steer.accepted", "steer":identity}).to_string(),
                    ))
                    .await
                    .unwrap();
                socket.send(Message::text(json!({"type":"response.completed", "response":{"id":"resp_1", "output":[]}}).to_string())).await.unwrap();
                if mode == "failed" {
                    socket.send(Message::text(json!({"type":"response.steer.failed", "steer":identity, "error":{"code":"steering_not_supported"}}).to_string())).await.unwrap();
                } else {
                    if mode == "pending" {
                        socket.send(Message::text(json!({"type":"response.steer.pending", "steer":identity, "required_input":[{"type":"function_call_output", "call_id":"call_1"}]}).to_string())).await.unwrap();
                        let continuation = socket.next().await.unwrap().unwrap();
                        let continuation: Value =
                            serde_json::from_str(continuation.as_text().unwrap()).unwrap();
                        assert_eq!(continuation["type"], "response.create");
                        assert_eq!(continuation["previous_response_id"], "resp_1");
                        assert_eq!(
                            continuation["input"],
                            json!([{"type":"function_call_output", "call_id":"call_1", "output":"done"}])
                        );
                    }
                    socket
                        .send(Message::text(
                            json!({"type":"response.created", "response":{"id":"resp_2"}})
                                .to_string(),
                        ))
                        .await
                        .unwrap();
                    socket
                        .send(Message::text(
                            json!({"type":"response.output_text.delta", "delta":"Updated"})
                                .to_string(),
                        ))
                        .await
                        .unwrap();
                    socket.send(Message::text(json!({"type":"response.completed", "response":{"id":"resp_2", "model":"gpt-6-astra", "output":[{"type":"reasoning", "encrypted_content":"opaque", "summary":[]} ]}}).to_string())).await.unwrap();
                }
                tokio::time::timeout(Duration::from_secs(2), socket.next())
                    .await
                    .expect("client must close completed socket");
            });
            let control = ResponsesControl::default();
            let mut config = GenerationConfig::new("gpt-6-astra");
            config.responses = Some(ResponsesOptions {
                websocket: true,
                control: Some(control.clone()),
                ..Default::default()
            });
            let client = OpenAIResponsesClient::new("fixture", "gpt-6-astra")
                .with_api_url(format!("http://{address}/v1/responses"));
            let mut stream = client.connect_and_listen(&[], &config).await.unwrap();
            let mut statuses = Vec::new();
            let mut texts = Vec::new();
            tokio::time::timeout(Duration::from_secs(3), async {
                while let Some(chunk) = stream.next().await {
                    match chunk.unwrap() {
                        StreamChunk::ResponseId(id) if id == "resp_1" && statuses.is_empty() => {
                            control.send("response.steer", json!("Change direction")).await.unwrap();
                        }
                        StreamChunk::Steering(data) => {
                            let status = data["status"].as_str().unwrap().to_owned();
                            if status == "pending" {
                                assert_eq!(data["required_input"][0]["call_id"], "call_1");
                                control.send("response.create", json!([{"type":"function_call_output", "call_id":"call_1", "output":"done"}])).await.unwrap();
                            }
                            statuses.push(status);
                        }
                        StreamChunk::Content(text) => texts.push(text),
                        _ => {}
                    }
                }
            }).await.expect("steering must settle without a second socket or indefinite wait");
            assert_eq!(
                statuses,
                match mode {
                    "automatic" => vec!["accepted", "applied"],
                    "pending" => vec!["accepted", "pending", "applied"],
                    _ => vec!["accepted", "failed"],
                }
            );
            if mode != "failed" {
                assert_eq!(texts, vec!["Updated"]);
            }
            assert!(
                control
                    .send("response.steer", json!("too late"))
                    .await
                    .is_err()
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn dropping_a_silent_stream_closes_the_provider_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::text(
                    json!({"type":"response.created", "response":{"id":"resp_1"}}).to_string(),
                ))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("cancel must close the socket without waiting for provider output");
        });
        let control = ResponsesControl::default();
        let mut config = GenerationConfig::new("gpt-6-astra");
        config.responses = Some(ResponsesOptions {
            websocket: true,
            control: Some(control.clone()),
            ..Default::default()
        });
        let client = OpenAIResponsesClient::new("fixture", "gpt-6-astra")
            .with_api_url(format!("http://{address}/v1/responses"));
        let mut stream = client.connect_and_listen(&[], &config).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            StreamChunk::ResponseId(_)
        ));
        drop(stream);
        server.await.unwrap();
        assert!(control.send("response.steer", json!("late")).await.is_err());
    }

    #[tokio::test]
    #[ignore = "requires the freshly built ts native binding and JavaScript output"]
    async fn native_binding_preserves_tool_execution_after_completed_response_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../ts/test-fixtures/astra-steering.cjs");
        let node = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio::process::Command::new("node")
                    .arg(script)
                    .env(
                        "CHEVALIER_ASTRA_FIXTURE",
                        format!("http://{address}/v1/responses"),
                    )
                    .env("CHEVALIER_ASTRA_FIXTURE_MODE", "completed")
                    .output(),
            )
            .await
            .unwrap()
            .unwrap()
        });
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::text(
                json!({"type":"response.created","response":{"id":"resp_1"}}).to_string(),
            ))
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        let item = json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{}"});
        for event in [
            json!({"type":"response.steer.accepted","steer":{"id":"steer_1"}}),
            json!({"type":"response.output_item.done","item":item}),
            json!({"type":"response.completed","response":{"id":"resp_1","output":[item]}}),
        ] {
            socket.send(Message::text(event.to_string())).await.unwrap();
        }
        socket.send(Message::close(None, "")).await.unwrap();
        let output = node.await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    #[ignore = "requires the freshly built ts native binding and JavaScript output"]
    async fn native_binding_controls_the_same_socket_and_aborts_silent_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../ts/test-fixtures/astra-steering.cjs");
        let node = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio::process::Command::new("node")
                    .arg(script)
                    .env(
                        "CHEVALIER_ASTRA_FIXTURE",
                        format!("http://{address}/v1/responses"),
                    )
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap()
            .unwrap()
        });
        let (socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
        let request = socket.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(request.as_text().unwrap()).unwrap();
        assert_eq!(request["type"], "response.create");
        socket
            .send(Message::text(
                json!({"type":"response.created", "response":{"id":"resp_1"}}).to_string(),
            ))
            .await
            .unwrap();
        let steer = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(steer.as_text().unwrap()).unwrap(),
            json!({"type":"response.steer", "previous_response_id":"resp_1", "input":"Change direction"})
        );
        for event in [
            json!({"type":"response.steer.accepted", "steer":{"id":"steer_1", "previous_response_id":"resp_1"}}),
            json!({"type":"response.completed", "response":{"id":"resp_1", "output":[]}}),
            json!({"type":"response.created", "response":{"id":"resp_2"}}),
            json!({"type":"response.output_text.delta", "delta":"Working on the update"}),
        ] {
            socket.send(Message::text(event.to_string())).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("TS AbortSignal must close the real provider socket");
        let result = node.await.unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[tokio::test]
    #[ignore = "requires the freshly built ts native binding and JavaScript output"]
    async fn native_binding_executes_registered_async_handler_before_stream_completion() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../ts/test-fixtures/astra-steering.cjs");
        let mut node = tokio::process::Command::new("node")
            .arg(script)
            .env(
                "CHEVALIER_ASTRA_FIXTURE",
                format!("http://{address}/v1/responses"),
            )
            .env("CHEVALIER_ASTRA_FIXTURE_MODE", "async")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(node.stdout.take().unwrap());
        let (socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = ServerBuilder::new().accept(socket).await.unwrap();
        let request = socket.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(request.as_text().unwrap()).unwrap();
        assert_eq!(request["tools"][0]["async"], true);
        let call = json!({"type":"function_call", "id":"fc_1", "call_id":"call_1", "name":"lookup", "arguments":"{}", "async":true});
        for event in [
            json!({"type":"response.created", "response":{"id":"resp_1"}}),
            json!({"type":"response.output_item.added", "output_index":0, "item":call}),
            json!({"type":"response.function_call_arguments.done", "output_index":0, "arguments":"{}"}),
        ] {
            socket.send(Message::text(event.to_string())).await.unwrap();
        }
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), stdout.read_line(&mut line))
            .await
            .expect(
                "registered handler must execute while provider waits, not after stream completion",
            )
            .unwrap();
        assert_eq!(line.trim(), "EXECUTED");
        for event in [
            json!({"type":"response.output_item.done", "output_index":0, "item":call}),
            json!({"type":"response.output_text.delta", "delta":"DONE"}),
            json!({"type":"response.completed", "response":{"id":"resp_1", "output":[]}}),
        ] {
            socket.send(Message::text(event.to_string())).await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(3), node.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
