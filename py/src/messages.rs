use chevalier_core::types::{
    AssistantResponse, ChatMessage, ChatRole, MediaPart, MediaSource, MultimodalMessage,
    ReasoningSegment, ResponsePart, ToolCall, ToolResult,
};
use chevalier_core::utils::ConversationMessage;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct MediaPartInput {
    document_url: Option<String>,
    document_base64: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    image_base64: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    image_url: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct ToolCallInput {
    tool_use_id: String,
    tool_name: String,
    args: String,
}

#[derive(Clone, Deserialize)]
pub struct Message {
    provider_response: Option<serde_json::Value>,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_use_id: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    is_error: Option<bool>,
    #[serde(default)]
    parts: Option<Vec<MediaPartInput>>,
    #[serde(default)]
    tool_calls: Option<Vec<ToolCallInput>>,
}

fn to_media_part(part: &MediaPartInput) -> MediaPart {
    match part.kind.as_str() {
        "document" => {
            if let Some(url) = &part.document_url {
                MediaPart::document(MediaSource::url(url.clone()))
            } else {
                MediaPart::document(MediaSource::base64(
                    part.document_base64.clone().unwrap_or_default(),
                    part.mime_type
                        .clone()
                        .unwrap_or_else(|| "application/pdf".into()),
                ))
            }
        }
        "image" => {
            if let Some(url) = &part.image_url {
                MediaPart::image(MediaSource::url(url.clone()))
            } else {
                MediaPart::image(MediaSource::base64(
                    part.image_base64.clone().unwrap_or_default(),
                    part.mime_type
                        .clone()
                        .unwrap_or_else(|| "image/png".to_string()),
                ))
            }
        }
        _ => MediaPart::text(part.text.clone().unwrap_or_default()),
    }
}

fn role_from(value: &str) -> ChatRole {
    match value {
        "system" => ChatRole::System,
        "assistant" => ChatRole::Assistant,
        "tool" => ChatRole::Tool,
        _ => ChatRole::User,
    }
}

pub fn to_chat_message(message: &Message) -> ChatMessage {
    let mut result = ChatMessage::user(message.content.clone().unwrap_or_default());
    result.role = role_from(message.role.as_deref().unwrap_or("user"));
    result
}

pub fn to_conversation_message(message: &Message) -> ConversationMessage {
    match message.kind.as_str() {
        "toolResult" => {
            let id = message.tool_use_id.clone().unwrap_or_default();
            let content = message.content.clone().unwrap_or_default();
            let mut result = if message.is_error.unwrap_or(false) {
                ToolResult::error(id, content)
            } else {
                ToolResult::success(id, content)
            };
            if let Some(name) = &message.tool_name {
                result = result.with_tool_name(name.clone());
            }
            ConversationMessage::ToolResult(result)
        }
        "assistantResponse" | "assistant_response" => {
            let mut parts = Vec::new();
            if let Some(text) = &message.content
                && !text.is_empty()
            {
                parts.push(ResponsePart::Text { text: text.clone() });
            }
            if let Some(calls) = &message.tool_calls {
                for call in calls {
                    let args = serde_json::from_str(&call.args)
                        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
                    parts.push(ResponsePart::Tool {
                        call: ToolCall {
                            tool_use_id: call.tool_use_id.clone(),
                            tool_name: call.tool_name.clone(),
                            args,
                            raw_arguments: Some(call.args.clone()),
                            signature: None,
                            tool_obj: None,
                        },
                    });
                }
            }
            {
                let mut response = AssistantResponse::new(parts);
                response.provider_response = message.provider_response.clone();
                ConversationMessage::AssistantResponse(response)
            }
        }
        "reasoning" => ConversationMessage::Reasoning(ReasoningSegment::new(
            message.content.clone().unwrap_or_default(),
        )),
        "multimodal" => {
            let parts = message
                .parts
                .as_ref()
                .map(|parts| parts.iter().map(to_media_part).collect())
                .unwrap_or_default();
            let mut result = MultimodalMessage::user(parts);
            result.role = role_from(message.role.as_deref().unwrap_or("user"));
            ConversationMessage::Multimodal(result)
        }
        _ => ConversationMessage::Chat(to_chat_message(message)),
    }
}
