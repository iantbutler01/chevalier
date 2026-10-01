use super::{ClaudeSessionEvent, ToolContent, ToolOutput, wire};
use crate::{runtime::ToolExecutor, types::ToolCall};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

pub(crate) struct PendingCall {
    pub rpc_id: Value,
    pub host: bool,
    pub cancel: CancellationToken,
}
pub(crate) type Pending = Arc<Mutex<HashMap<String, PendingCall>>>;

#[derive(Clone)]
pub(crate) struct DispatchContext {
    pub server: String,
    pub schemas: Arc<HashMap<String, bool>>,
    pub executor: ToolExecutor,
    pub pending: Pending,
    pub events: mpsc::UnboundedSender<crate::error::Result<ClaudeSessionEvent>>,
    pub outbound: mpsc::UnboundedSender<Value>,
}

pub(crate) async fn dispatch(request_id: String, message: Value, context: DispatchContext) {
    let DispatchContext {
        server,
        schemas,
        executor,
        pending,
        events,
        outbound,
    } = context;
    let rpc_id = message.get("id").cloned().unwrap_or_else(|| json!(0));
    let method = message["method"].as_str().unwrap_or("");
    if method == "notifications/cancelled" {
        let cancelled = message["params"]["requestId"].clone();
        let mut calls = pending.lock().await;
        if let Some((call_id, call, cancel)) = calls
            .iter()
            .find(|(_, call)| call.rpc_id == cancelled)
            .map(|(id, call)| (id.clone(), call.host, call.cancel.clone()))
        {
            calls.remove(&call_id);
            cancel.cancel();
            if call {
                let _ = events.send(Ok(ClaudeSessionEvent::ToolCancelled { call_id }));
            }
        }
        let _ = outbound.send(wire::empty_response(&request_id, rpc_id));
        return;
    }
    if method != "tools/call" {
        let _ = outbound.send(wire::empty_response(&request_id, rpc_id));
        return;
    }
    let name = message["params"]["name"].as_str().unwrap_or("");
    let call = ToolCall {
        tool_use_id: message["params"]["_meta"]["claudecode/toolUseId"]
            .as_str()
            .unwrap_or("")
            .into(),
        tool_name: name.into(),
        args: message["params"]["arguments"].clone(),
        raw_arguments: None,
        signature: None,
        tool_obj: None,
    };
    if server.is_empty() || !schemas.contains_key(name) {
        let _ = outbound.send(wire::control_response(
            &request_id,
            rpc_id,
            ToolOutput {
                content: vec![ToolContent::Text(format!("Unknown tool: {name}"))],
                is_error: true,
            },
        ));
        return;
    }
    let host = schemas[name];
    let cancel = CancellationToken::new();
    pending.lock().await.insert(
        request_id.clone(),
        PendingCall {
            rpc_id: rpc_id.clone(),
            host,
            cancel: cancel.clone(),
        },
    );
    if host {
        let _ = events.send(Ok(ClaudeSessionEvent::ToolCall {
            call_id: request_id,
            call,
        }));
    } else {
        let output = match tokio::select! { _ = cancel.cancelled() => return, result = executor.execute(&call) => result }
        {
            Ok(text) => ToolOutput {
                content: vec![ToolContent::Text(text)],
                is_error: false,
            },
            Err(error) => ToolOutput {
                content: vec![ToolContent::Text(error.to_string())],
                is_error: true,
            },
        };
        if pending.lock().await.remove(&request_id).is_some() {
            let _ = outbound.send(wire::control_response(&request_id, rpc_id, output.clone()));
            let _ = events.send(Ok(ClaudeSessionEvent::ToolExecuted { call, output }));
        }
    }
}
