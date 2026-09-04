//! Conversation message inputs (typed napi objects) → engine types.
//! Covers chat (user/assistant/system), tool results, and reasoning segments —
//! the message kinds a multi-turn agent loop needs. (Multimodal history is a
//! follow-up.)

use chevalier_core::types::{
    AssistantResponse, ChatMessage, ChatRole, MediaPart, MediaSource, MultimodalMessage,
    ReasoningSegment, ResponsePart, ToolCall, ToolResult,
};
use chevalier_core::utils::ConversationMessage;
use napi_derive::napi;

/// A part of a multimodal message. `type` is `text` or `image`.
#[napi(object)]
pub struct MediaPartInput {
    #[napi(js_name = "type")]
    pub kind: String,
    pub text: Option<String>,
    pub image_base64: Option<String>,
    pub mime_type: Option<String>,
    pub image_url: Option<String>,
}

/// A tool call within an `assistantResponse` history message. `args` is the
/// JSON-encoded arguments object (parsed back into a value here).
#[napi(object)]
pub struct ToolCallInput {
    pub tool_use_id: String,
    pub tool_name: String,
    pub args: String,
}

/// A conversation message. `type` selects the variant:
/// - `chat` → `{ role: "user"|"assistant"|"system", content }`
/// - `assistantResponse` → `{ content?, toolCalls: [{ toolUseId, toolName, args }] }`
///   — a SINGLE assistant turn carrying ordered text + tool_use blocks, so
///   providers that require the assistant `tool_use` to precede a `tool_result`
///   (Anthropic/Bedrock) get a well-formed message.
/// - `toolResult` → `{ toolUseId, content, isError?, toolName? }`
/// - `reasoning` → `{ content }`
/// - `multimodal` → `{ parts: [{ type:"text", text } | { type:"image", imageBase64, mimeType }] }`
#[napi(object)]
pub struct Message {
    #[napi(js_name = "type")]
    pub kind: String,
    pub role: Option<String>,
    pub content: Option<String>,
    pub tool_use_id: Option<String>,
    pub tool_name: Option<String>,
    pub is_error: Option<bool>,
    pub parts: Option<Vec<MediaPartInput>>,
    pub tool_calls: Option<Vec<ToolCallInput>>,
    pub provider_response: Option<serde_json::Value>,
}

fn to_media_part(p: &MediaPartInput) -> MediaPart {
    match p.kind.as_str() {
        "image" => {
            if let Some(url) = &p.image_url {
                MediaPart::image(MediaSource::url(url.clone()))
            } else {
                MediaPart::image(MediaSource::base64(
                    p.image_base64.clone().unwrap_or_default(),
                    p.mime_type
                        .clone()
                        .unwrap_or_else(|| "image/png".to_string()),
                ))
            }
        }
        _ => MediaPart::text(p.text.clone().unwrap_or_default()),
    }
}

fn role_from(s: &str) -> ChatRole {
    match s {
        "system" => ChatRole::System,
        "assistant" => ChatRole::Assistant,
        "tool" => ChatRole::Tool,
        _ => ChatRole::User,
    }
}

/// Build a `ChatMessage` from a message (used for system-message prefixes).
/// Honors every role, including `tool` (which previously fell through to user).
pub fn to_chat_message(m: &Message) -> ChatMessage {
    let content = m.content.clone().unwrap_or_default();
    let mut msg = ChatMessage::user(content);
    msg.role = role_from(m.role.as_deref().unwrap_or("user"));
    msg
}

/// Build a `ConversationMessage` (history item) from a message.
pub fn to_conversation_message(m: &Message) -> ConversationMessage {
    match m.kind.as_str() {
        "toolResult" => {
            let id = m.tool_use_id.clone().unwrap_or_default();
            let content = m.content.clone().unwrap_or_default();
            let mut tr = if m.is_error.unwrap_or(false) {
                ToolResult::error(id, content)
            } else {
                ToolResult::success(id, content)
            };
            if let Some(name) = &m.tool_name {
                tr = tr.with_tool_name(name.clone());
            }
            ConversationMessage::ToolResult(tr)
        }
        "assistantResponse" | "assistant_response" => {
            let mut parts: Vec<ResponsePart> = Vec::new();
            if let Some(text) = &m.content
                && !text.is_empty()
            {
                parts.push(ResponsePart::Text { text: text.clone() });
            }
            if let Some(calls) = &m.tool_calls {
                for c in calls {
                    let args: serde_json::Value = serde_json::from_str(&c.args)
                        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
                    parts.push(ResponsePart::Tool {
                        call: ToolCall {
                            tool_use_id: c.tool_use_id.clone(),
                            tool_name: c.tool_name.clone(),
                            args,
                            raw_arguments: Some(c.args.clone()),
                            signature: None,
                            tool_obj: None,
                        },
                    });
                }
            }
            let mut response = AssistantResponse::new(parts);
            response.provider_response = m.provider_response.clone();
            ConversationMessage::AssistantResponse(response)
        }
        "reasoning" => ConversationMessage::Reasoning(ReasoningSegment::new(
            m.content.clone().unwrap_or_default(),
        )),
        "multimodal" => {
            let parts = m
                .parts
                .as_ref()
                .map(|ps| ps.iter().map(to_media_part).collect())
                .unwrap_or_default();
            let mut mm = MultimodalMessage::user(parts);
            mm.role = role_from(m.role.as_deref().unwrap_or("user"));
            ConversationMessage::Multimodal(mm)
        }
        _ => ConversationMessage::Chat(to_chat_message(m)),
    }
}
