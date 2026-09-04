//! TS-facing data types (napi objects) mirroring the Chevalier engine types,
//! plus conversions. Kept as `#[napi(object)]` so napi generates clean TS
//! interfaces rather than `any`.

use chevalier_core::runtime::ToolSchemaInfo;
use chevalier_core::types::{AssistantResponse, ToolCall};
use napi_derive::napi;

/// A tool call emitted by the model.
#[napi(object)]
pub struct ToolCallJs {
    pub r#async: Option<bool>,
    pub provider_metadata: Option<serde_json::Value>,
    pub tool_use_id: String,
    pub tool_name: String,
    /// Parsed arguments (JSON object).
    pub args: serde_json::Value,
}

impl From<&ToolCall> for ToolCallJs {
    fn from(tc: &ToolCall) -> Self {
        Self {
            tool_use_id: tc.tool_use_id.clone(),
            tool_name: tc.tool_name.clone(),
            args: tc.args.clone(),
            r#async: tc
                .tool_obj
                .as_ref()
                .and_then(|value| value.get("async"))
                .and_then(serde_json::Value::as_bool),
            provider_metadata: tc.tool_obj.clone(),
        }
    }
}

/// Result of a non-streaming `Runtime.run`.
#[napi(object)]
pub struct RunResult {
    pub provider_response: Option<serde_json::Value>,
    /// Concatenated assistant text (for structured output this is the JSON
    /// string to decode against your schema).
    pub text: String,
    /// Concatenated extended-thinking / reasoning text, if any.
    pub reasoning: String,
    /// Tool calls the model requested.
    pub tool_calls: Vec<ToolCallJs>,
    /// Provider reasoning signatures, if any.
    pub signatures: Vec<String>,
}

/// Registered tool schema (for introspection / sending to a provider).
#[napi(object)]
pub struct ToolSchemaJs {
    pub r#async: bool,
    pub name: String,
    pub description: String,
    /// JSON Schema for the tool's parameters.
    pub parameters: serde_json::Value,
}

impl From<ToolSchemaInfo> for ToolSchemaJs {
    fn from(s: ToolSchemaInfo) -> Self {
        Self {
            name: s.name,
            r#async: s.asynchronous,
            description: s.description,
            parameters: s.parameters.to_json_schema(),
        }
    }
}

impl From<AssistantResponse> for RunResult {
    fn from(resp: AssistantResponse) -> Self {
        let tool_calls = resp
            .tool_calls()
            .iter()
            .map(|tc| ToolCallJs::from(*tc))
            .collect();
        let signatures = resp.signatures().iter().map(|s| s.to_string()).collect();
        Self {
            text: resp.text(),
            reasoning: resp.reasoning(),
            tool_calls,
            signatures,
            provider_response: resp.provider_response,
        }
    }
}
