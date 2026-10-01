use super::{ClaudeSessionError, ToolContent, ToolOutput, UserTurn};
use crate::runtime::ToolSchemaInfo;
use crate::types::{MediaPart, MediaSource};
use claude_codes::{ClaudeOutput, ControlResponse};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
pub(crate) struct InitProjection {
    pub session_id: String,
    pub model: String,
    pub tools: Vec<String>,
    pub mcp_servers: Vec<Value>,
    #[serde(rename = "apiKeySource")]
    pub api_key_source: String,
    pub claude_code_version: String,
}

#[derive(Serialize)]
struct Initialize<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    request_id: &'a str,
    request: InitializeRequest<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InitializeRequest<'a> {
    subtype: &'static str,
    system_prompt: [&'a str; 1],
    sdk_mcp_servers: [&'a str; 1],
    sdk_mcp_server_manifests: BTreeMap<&'a str, ServerManifest>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerManifest {
    initialize_result: Value,
    tools_list_result: ToolListResult,
}

#[derive(Serialize)]
struct ToolListResult {
    tools: Vec<ToolManifest>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolManifest {
    name: String,
    description: String,
    input_schema: Value,
    execution: Value,
}

#[derive(Serialize)]
struct McpResponsePayload {
    mcp_response: JsonRpcResponse,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    result: Value,
}

pub(crate) fn initialize(
    id: &str,
    server: &str,
    tools: &[ToolSchemaInfo],
    prompt: Option<&str>,
) -> Value {
    let manifest_tools: Vec<_> = tools
        .iter()
        .map(|tool| ToolManifest {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: tool
                .raw_schema
                .clone()
                .unwrap_or_else(|| tool.parameters.to_json_schema()),
            execution: json!({"taskSupport": "forbidden"}),
        })
        .collect();
    let manifests = BTreeMap::from([(
        server,
        ServerManifest {
            initialize_result: json!({"protocolVersion": "2025-11-25", "capabilities": {"tools": {"listChanged": true}}, "serverInfo": {"name": server, "version": env!("CARGO_PKG_VERSION")}}),
            tools_list_result: ToolListResult {
                tools: manifest_tools,
            },
        },
    )]);
    serde_json::to_value(Initialize {
        kind: "control_request",
        request_id: id,
        request: InitializeRequest {
            subtype: "initialize",
            system_prompt: [prompt.unwrap_or("")],
            sdk_mcp_servers: [server],
            sdk_mcp_server_manifests: manifests,
        },
    })
    .expect("initialize is serializable")
}

pub(crate) fn user(turn: UserTurn) -> Result<Value, ClaudeSessionError> {
    let mut content = vec![json!({"type":"text", "text": turn.text})];
    for image in turn.images {
        let MediaPart::Image {
            source: MediaSource::Base64 { data, mime_type },
            ..
        } = image
        else {
            return Err(ClaudeSessionError::Protocol(
                "Claude user images require base64 image media".into(),
            ));
        };
        content.push(json!({"type":"image", "source": {"type":"base64", "media_type": mime_type, "data": data}}));
    }
    Ok(
        json!({"type":"user", "session_id":"", "message":{"role":"user", "content":content}, "parent_tool_use_id":null}),
    )
}

pub(crate) fn control_response(id: &str, rpc_id: Value, output: ToolOutput) -> Value {
    let content: Vec<Value> = output
        .content
        .into_iter()
        .map(|block| match block {
            ToolContent::Text(text) => json!({"type":"text", "text":text}),
            ToolContent::Image {
                data_base64,
                mime_type,
            } => json!({"type":"image", "data":data_base64, "mimeType":mime_type}),
        })
        .collect();
    let payload = serde_json::to_value(McpResponsePayload {
        mcp_response: JsonRpcResponse {
            jsonrpc: "2.0",
            id: rpc_id,
            result: json!({"content":content, "isError":output.is_error}),
        },
    })
    .expect("MCP response is serializable");
    let mut value = serde_json::to_value(ControlResponse::success(id, payload))
        .expect("control response is serializable");
    value["type"] = json!("control_response");
    value
}

pub(crate) fn empty_response(id: &str, rpc_id: Value) -> Value {
    let mut value = serde_json::to_value(ControlResponse::success(
        id,
        serde_json::to_value(McpResponsePayload {
            mcp_response: JsonRpcResponse {
                jsonrpc: "2.0",
                id: rpc_id,
                result: json!({}),
            },
        })
        .expect("MCP response is serializable"),
    ))
    .expect("control response is serializable");
    value["type"] = json!("control_response");
    value
}

pub(crate) fn decode(line: &str) -> Result<(ClaudeOutput, Value), ClaudeSessionError> {
    let value: Value =
        serde_json::from_str(line).map_err(|e| ClaudeSessionError::Protocol(e.to_string()))?;
    let typed = serde_json::from_value(value.clone()).map_err(|e| {
        ClaudeSessionError::Protocol(format!(
            "claude-codes could not decode {}: {e}",
            value["type"]
        ))
    })?;
    Ok((typed, value))
}

pub(crate) fn acted_shape(line: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    match value["type"].as_str() {
        Some("system") => matches!(value["subtype"].as_str(), Some("init" | "api_retry")),
        Some(
            "assistant" | "result" | "rate_limit_event" | "stream_event" | "control_request"
            | "control_response",
        ) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{MediaPart, MediaSource};
    use crate::runtime::{Runtime, ToolFunction};
    #[test]
    fn captured_stdout_decodes() {
        let root = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/claude-subscription/"
        );
        for name in ["happy", "steer", "resume", "interrupt"] {
            let path = format!("{root}{name}.stdout.jsonl");
            let lines = std::fs::read_to_string(&path).unwrap();
            for (index, line) in lines.lines().enumerate() {
                super::decode(line)
                    .unwrap_or_else(|error| panic!("{name} line {}: {error}", index + 1));
            }
        }
    }

    #[test]
    fn captured_init_decodes_with_pinned_type() {
        let first_init =
            include_str!("../../tests/fixtures/claude-subscription/happy.stdout.jsonl")
                .lines()
                .find(|line| line.contains("\"subtype\": \"init\""))
                .unwrap();
        let (output, _) = super::decode(first_init).unwrap();
        let claude_codes::ClaudeOutput::System(system) = output else {
            panic!("expected system/init")
        };
        let _: claude_codes::InitMessage = serde_json::from_value(system.data).unwrap();
    }

    #[tokio::test]
    async fn manifest_preserves_registration_order_filter_and_schema() {
        let runtime = Runtime::new();
        let first = serde_json::json!({"$schema":"http://json-schema.org/draft-07/schema#", "type":"object", "properties":{"path":{"type":"string"}}, "required":["path"]});
        let second =
            serde_json::json!({"type":"object", "properties":{"visible":{"type":"boolean"}}});
        for (name, schema) in [("read_file", first.clone()), ("screenshot", second)] {
            runtime
                .register_tool_with_schema(
                    name,
                    name,
                    schema,
                    ToolFunction::Sync(Box::new(|_| Ok(String::new()))),
                )
                .await
                .unwrap();
        }
        let schemas = runtime.get_tool_schemas().await;
        let ordered: Vec<_> = runtime
            .claude_tool_names()
            .await
            .iter()
            .map(|name| schemas[name].clone())
            .collect();
        let value = super::initialize("id", "ob", &ordered, Some("prompt"));
        let tools = value["request"]["sdkMcpServerManifests"]["ob"]["toolsListResult"]["tools"]
            .as_array()
            .unwrap();
        assert_eq!(tools[0]["name"], "read_file");
        assert_eq!(tools[1]["name"], "screenshot");
        assert_eq!(tools[0]["inputSchema"], first);
        assert_eq!(
            value["request"]["systemPrompt"],
            serde_json::json!(["prompt"])
        );
        runtime
            .set_model_tool_names(Some(vec!["screenshot".into()]))
            .await
            .unwrap();
        assert_eq!(runtime.claude_tool_names().await, vec!["screenshot"]);
    }

    #[test]
    fn user_image_uses_base64_wire_shape() {
        let value = super::user(super::UserTurn {
            text: "look".into(),
            images: vec![MediaPart::image(MediaSource::base64("AA==", "image/png"))],
        })
        .unwrap();
        assert_eq!(
            value["message"]["content"][1]["source"]["media_type"],
            "image/png"
        );
        assert_eq!(value["message"]["content"][1]["source"]["data"], "AA==");
    }
}
