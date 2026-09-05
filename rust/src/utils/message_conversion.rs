//! Message conversion utilities
//!
//! Converts mixed ChatMessage/ToolResult/ToolCall/ReasoningSegment lists to provider-specific formats.
//! Handles message coalescing for Anthropic/Google providers.

use crate::error::{Error, Result};
use crate::types::{
    AssistantResponse, CacheMarker, ChatMessage, ChatRole, MediaPart, MediaSource,
    MultimodalMessage, Provider, ReasoningSegment, ResponsePart, ToolCall, ToolResult,
    VideoMetadata,
};
use serde_json::{Value, json};
use uuid::Uuid;

/// Message type that can appear in conversation history
#[derive(Debug, Clone, serde::Serialize)]
pub enum ConversationMessage {
    /// Regular chat message (user, assistant, system)
    Chat(ChatMessage),
    /// Durable assistant response with ordered output.
    AssistantResponse(AssistantResponse),
    /// Tool call from the assistant
    ToolCall(ToolCall),
    /// Tool result from execution
    ToolResult(ToolResult),
    /// Reasoning/thinking segment
    Reasoning(ReasoningSegment),
    /// Multimodal message with media parts (images, video, audio)
    Multimodal(MultimodalMessage),
}

impl From<ChatMessage> for ConversationMessage {
    fn from(msg: ChatMessage) -> Self {
        ConversationMessage::Chat(msg)
    }
}

impl From<AssistantResponse> for ConversationMessage {
    fn from(response: AssistantResponse) -> Self {
        ConversationMessage::AssistantResponse(response)
    }
}

impl From<ToolCall> for ConversationMessage {
    fn from(tool_call: ToolCall) -> Self {
        ConversationMessage::ToolCall(tool_call)
    }
}

impl From<ToolResult> for ConversationMessage {
    fn from(result: ToolResult) -> Self {
        ConversationMessage::ToolResult(result)
    }
}

impl From<ReasoningSegment> for ConversationMessage {
    fn from(segment: ReasoningSegment) -> Self {
        ConversationMessage::Reasoning(segment)
    }
}

impl From<MultimodalMessage> for ConversationMessage {
    fn from(msg: MultimodalMessage) -> Self {
        ConversationMessage::Multimodal(msg)
    }
}

/// Convert a MediaPart to Google's parts format
pub fn media_part_to_google_format(part: &MediaPart, metadata: Option<&VideoMetadata>) -> Value {
    match part {
        MediaPart::Text { text } => json!({ "text": text }),

        MediaPart::Image { source, .. } => media_source_to_google_format(source, None),

        MediaPart::Audio { source, .. } => media_source_to_google_format(source, None),

        MediaPart::Video {
            source,
            metadata: video_meta,
        } => {
            let meta = video_meta.as_ref().or(metadata);
            media_source_to_google_format(source, meta)
        }

        MediaPart::Document { source } => media_source_to_google_format(source, None),
    }
}

/// Convert a MediaSource to Google's format
fn media_source_to_google_format(source: &MediaSource, metadata: Option<&VideoMetadata>) -> Value {
    let mut part = match source {
        MediaSource::Base64 { data, mime_type } => {
            json!({
                "inline_data": {
                    "data": data,
                    "mime_type": mime_type
                }
            })
        }
        MediaSource::Url { url } => {
            // Google doesn't support arbitrary URLs directly - must use file_data
            // This is a fallback that may not work for all cases
            json!({
                "file_data": {
                    "file_uri": url
                }
            })
        }
        MediaSource::FileId { file_id } => {
            json!({
                "file_data": {
                    "file_uri": file_id
                }
            })
        }
        MediaSource::FileUri { uri, mime_type } => {
            let mut fd = json!({
                "file_data": {
                    "file_uri": uri
                }
            });
            if let Some(mt) = mime_type {
                fd["file_data"]["mime_type"] = json!(mt);
            }
            fd
        }
    };

    // Add video metadata if present
    if let Some(meta) = metadata {
        let mut video_metadata = json!({});
        if let Some(ref start) = meta.start_offset {
            video_metadata["start_offset"] = json!(start);
        }
        if let Some(ref end) = meta.end_offset {
            video_metadata["end_offset"] = json!(end);
        }
        if let Some(fps) = meta.fps {
            video_metadata["fps"] = json!(fps);
        }
        if video_metadata
            .as_object()
            .map(|o| !o.is_empty())
            .unwrap_or(false)
        {
            part["video_metadata"] = video_metadata;
        }
    }

    part
}

/// Convert a MediaPart to Anthropic's content format
fn media_part_to_anthropic_format(part: &MediaPart) -> Value {
    match part {
        MediaPart::Text { text } => json!({
            "type": "text",
            "text": text
        }),

        MediaPart::Image { source, .. } => match source {
            MediaSource::Base64 { data, mime_type } => json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": mime_type,
                    "data": data
                }
            }),
            MediaSource::Url { url } => json!({
                "type": "image",
                "source": {
                    "type": "url",
                    "url": url
                }
            }),
            MediaSource::FileId { file_id } => json!({
                "type": "image",
                "source": {
                    "type": "file",
                    "file_id": file_id
                }
            }),
            MediaSource::FileUri { uri, .. } => json!({
                "type": "image",
                "source": {
                    "type": "url",
                    "url": uri
                }
            }),
        },

        MediaPart::Document { source } => match source {
            MediaSource::Base64 { data, mime_type } => json!({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": mime_type,
                    "data": data
                }
            }),
            MediaSource::FileId { file_id } => json!({
                "type": "document",
                "source": {
                    "type": "file",
                    "file_id": file_id
                }
            }),
            _ => json!({
                "type": "text",
                "text": "[Unsupported document source for Anthropic]"
            }),
        },

        // Anthropic doesn't support audio/video directly
        MediaPart::Audio { .. } | MediaPart::Video { .. } => json!({
            "type": "text",
            "text": "[Audio/Video not supported by this provider]"
        }),
    }
}

/// Convert a MediaPart to OpenAI's content format
fn media_part_to_openai_format(part: &MediaPart) -> Value {
    match part {
        MediaPart::Text { text } => json!({
            "type": "text",
            "text": text
        }),

        MediaPart::Image { source, detail } => {
            let mut img = match source {
                MediaSource::Base64 { data, mime_type } => json!({
                    "type": "image_url",
                    "image_url": {
                        "url": format!("data:{};base64,{}", mime_type, data)
                    }
                }),
                MediaSource::Url { url } => json!({
                    "type": "image_url",
                    "image_url": {
                        "url": url
                    }
                }),
                MediaSource::FileId { file_id } => json!({
                    "type": "image_file",
                    "image_file": {
                        "file_id": file_id
                    }
                }),
                MediaSource::FileUri { uri, .. } => json!({
                    "type": "image_url",
                    "image_url": {
                        "url": uri
                    }
                }),
            };
            if let Some(d) = detail
                && let Some(obj) = img.get_mut("image_url")
            {
                obj["detail"] = json!(d);
            }
            img
        }

        MediaPart::Audio { source, format } => match source {
            MediaSource::Base64 { data, .. } => {
                let fmt = format.as_deref().unwrap_or("wav");
                json!({
                    "type": "input_audio",
                    "input_audio": {
                        "data": data,
                        "format": fmt
                    }
                })
            }
            _ => json!({
                "type": "text",
                "text": "[Unsupported audio source for OpenAI - use base64]"
            }),
        },

        // Video URL support (vLLM-compatible format for Qwen2-VL, etc.)
        MediaPart::Video {
            source: MediaSource::Url { url },
            ..
        } => json!({
            "type": "video_url",
            "video_url": { "url": url }
        }),
        MediaPart::Video { .. } | MediaPart::Document { .. } => json!({
            "type": "text",
            "text": "[Video/Document not supported by this provider]"
        }),
    }
}

/// Convert a MediaPart to OpenAI Responses API format
fn media_part_to_openai_responses_format(part: &MediaPart) -> Value {
    match part {
        MediaPart::Text { text } => json!({
            "type": "input_text",
            "text": text
        }),

        MediaPart::Image { source, detail } => {
            let mut image = match source {
                MediaSource::Base64 { data, mime_type } => json!({
                    "type": "input_image",
                    "image_url": format!("data:{};base64,{}", mime_type, data)
                }),
                MediaSource::Url { url } => json!({
                    "type": "input_image",
                    "image_url": url
                }),
                MediaSource::FileId { file_id } => json!({
                    "type": "input_image",
                    "file_id": file_id
                }),
                MediaSource::FileUri { uri, .. } => json!({
                    "type": "input_image",
                    "image_url": uri
                }),
            };
            if let Some(detail) = detail {
                image["detail"] = json!(detail);
            }
            image
        }

        MediaPart::Audio { source, format } => match source {
            MediaSource::Base64 { data, .. } => {
                let fmt = format.as_deref().unwrap_or("wav");
                json!({
                    "type": "input_audio",
                    "input_audio": {
                        "data": data,
                        "format": fmt
                    }
                })
            }
            _ => json!({
                "type": "input_text",
                "text": "[Unsupported audio source for OpenAI Responses - use base64]"
            }),
        },

        MediaPart::Document { source } => match source {
            MediaSource::Base64 { data, mime_type } => json!({
                "type": "input_file",
                "filename": "attachment.pdf",
                "file_data": format!("data:{};base64,{}", mime_type, data)
            }),
            MediaSource::FileId { file_id } => json!({ "type": "input_file", "file_id": file_id }),
            MediaSource::Url { url } => json!({ "type": "input_file", "file_url": url }),
            MediaSource::FileUri { uri, .. } => json!({ "type": "input_file", "file_url": uri }),
        },
        MediaPart::Video { .. } => json!({
            "type": "input_text",
            "text": "[Video/Document not supported by this provider]"
        }),
    }
}

/// Return true when the conversation contains user-supplied image input.
pub fn messages_contain_image_input(messages: &[ConversationMessage]) -> bool {
    messages.iter().any(|message| match message {
        ConversationMessage::Multimodal(multimodal) => multimodal
            .parts
            .iter()
            .any(|part| matches!(part, MediaPart::Image { .. })),
        _ => false,
    })
}

/// Fail before dispatch when a provider/model cannot carry image input.
pub fn validate_image_input_supported(
    messages: &[ConversationMessage],
    provider: Provider,
    model: &str,
) -> Result<()> {
    if messages_contain_image_input(messages) && !provider.supports_image_input(model) {
        return Err(Error::Validation(
            "This Nym's current model cannot inspect images.".to_string(),
        ));
    }
    Ok(())
}

/// Convert a MultimodalMessage to provider-specific format
fn multimodal_to_provider_format(msg: &MultimodalMessage, provider: Provider) -> Value {
    let role = match msg.role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool => "tool",
    };

    let parts: Vec<Value> = msg
        .parts
        .iter()
        .map(|part| match provider {
            Provider::GoogleGenAI => media_part_to_google_format(part, None),
            Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                media_part_to_anthropic_format(part)
            }
            Provider::OpenAI
            | Provider::OpenRouter
            | Provider::OpenAIResponses
            | Provider::OpenRouterResponses => media_part_to_openai_format(part),
        })
        .collect();

    // Google uses "parts", others use "content" array
    match provider {
        Provider::GoogleGenAI => json!({
            "role": if role == "assistant" { "model" } else { role },
            "parts": parts
        }),
        _ => json!({
            "role": role,
            "content": parts
        }),
    }
}

fn tool_call_to_provider_part(tool_call: &ToolCall, provider: Provider) -> Value {
    match provider {
        Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => json!({
            "type": "tool_use",
            "id": tool_call.tool_use_id,
            "name": tool_call.tool_name,
            "input": tool_call.args
        }),
        Provider::GoogleGenAI => {
            let mut part = json!({
                "functionCall": {
                    "name": tool_call.tool_name,
                    "args": tool_call.args
                }
            });
            if let Some(ref signature) = tool_call.signature {
                part["thoughtSignature"] = json!(signature);
            } else if let Some(ref obj) = tool_call.tool_obj
                && let Some(signature) = obj.get("thoughtSignature")
            {
                part["thoughtSignature"] = signature.clone();
            }
            part
        }
        _ => json!({}),
    }
}

fn anthropic_text_block(text: &str, cache_marker: Option<&CacheMarker>) -> Value {
    let mut block = json!({
        "type": "text",
        "text": text
    });
    if let Some(marker) = cache_marker {
        block["cache_control"] = marker.anthropic_cache_control();
    }
    block
}

fn attach_signature_to_value(value: &mut Value, signature: &str, provider: Provider) {
    match provider {
        Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
            value["signature"] = json!(signature);
        }
        Provider::GoogleGenAI => {
            value["thoughtSignature"] = json!(signature);
        }
        Provider::OpenAIResponses | Provider::OpenRouterResponses => {
            value["signature"] = json!(signature);
        }
        Provider::OpenAI | Provider::OpenRouter => {}
    }
}

fn assistant_response_to_openai_chat_message(response: &AssistantResponse) -> Value {
    let content = response.text();
    let tool_calls: Vec<Value> = response
        .tool_calls()
        .into_iter()
        .map(|tool_call| {
            let args_str = tool_call.raw_arguments.clone().unwrap_or_else(|| {
                serde_json::to_string(&tool_call.args).unwrap_or_else(|_| "{}".to_string())
            });
            json!({
                "id": tool_call.tool_use_id,
                "type": "function",
                "function": {
                    "name": tool_call.tool_name,
                    "arguments": args_str
                }
            })
        })
        .collect();

    let mut message = json!({
        "role": "assistant",
        "content": content
    });
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }
    message
}

fn assistant_response_to_anthropic_message(
    response: &AssistantResponse,
    provider: Provider,
) -> Value {
    let mut content = Vec::new();
    let mut idx = 0;
    while idx < response.output.len() {
        match &response.output[idx] {
            ResponsePart::Text { text } => content.push(json!({
                "type": "text",
                "text": text
            })),
            ResponsePart::Reasoning { text } => {
                let mut block = ReasoningSegment::new(text.clone()).to_provider_format(provider);
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut block, value, provider);
                    idx += 1;
                }
                content.push(block);
            }
            ResponsePart::Tool { call } => {
                let mut block = tool_call_to_provider_part(call, provider);
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut block, value, provider);
                    idx += 1;
                }
                content.push(block);
            }
            ResponsePart::Signature { .. } => {}
        }
        idx += 1;
    }

    json!({
        "role": "assistant",
        "content": content
    })
}

fn assistant_response_to_google_message(response: &AssistantResponse) -> Value {
    let mut parts = Vec::new();
    let mut idx = 0;
    while idx < response.output.len() {
        match &response.output[idx] {
            ResponsePart::Text { text } => parts.push(json!({ "text": text })),
            ResponsePart::Reasoning { text } => {
                let mut part = json!({
                    "thought": true,
                    "text": text
                });
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut part, value, Provider::GoogleGenAI);
                    idx += 1;
                }
                parts.push(part);
            }
            ResponsePart::Tool { call } => {
                let mut part = tool_call_to_provider_part(call, Provider::GoogleGenAI);
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut part, value, Provider::GoogleGenAI);
                    idx += 1;
                }
                parts.push(part);
            }
            ResponsePart::Signature { .. } => {}
        }
        idx += 1;
    }

    json!({
        "role": "model",
        "parts": parts
    })
}

fn assistant_response_to_responses_items(
    response: &AssistantResponse,
    preserve_native: bool,
) -> Vec<Value> {
    if preserve_native
        && let Some(items) = response
            .provider_response
            .as_ref()
            .and_then(|value| value.get("items"))
            .and_then(Value::as_array)
    {
        return items.clone();
    }
    let mut items = Vec::new();
    let mut idx = 0;
    while idx < response.output.len() {
        match &response.output[idx] {
            ResponsePart::Text { text } => {
                items.push(json!({
                    "type": "message",
                    "role": "assistant",
                    "id": format!("msg_{}", Uuid::new_v4()),
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": text,
                        "annotations": []
                    }]
                }));
            }
            ResponsePart::Reasoning { text } => {
                let mut item = json!({
                    "type": "reasoning",
                    "content": [{
                        "type": "output_text",
                        "text": text
                    }]
                });
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut item, value, Provider::OpenAIResponses);
                    idx += 1;
                }
                items.push(item);
            }
            ResponsePart::Tool { call } => {
                let mut item = call.to_provider_assistant_message(Provider::OpenAIResponses);
                if let Some(ResponsePart::Signature { value }) = response.output.get(idx + 1) {
                    attach_signature_to_value(&mut item, value, Provider::OpenAIResponses);
                    idx += 1;
                }
                items.push(item);
            }
            ResponsePart::Signature { .. } => {}
        }
        idx += 1;
    }
    items
}

/// Convert messages to OpenAI/OpenRouter Responses API input format.
///
/// Returns (instructions, input_items).
pub fn convert_messages_to_responses_input(
    messages: &[ConversationMessage],
    provider: Provider,
) -> Result<(Option<String>, Vec<Value>)> {
    responses_input(messages, provider, None)
}

fn responses_input(
    messages: &[ConversationMessage],
    provider: Provider,
    model: Option<&str>,
) -> Result<(Option<String>, Vec<Value>)> {
    let mut instructions: Vec<String> = Vec::new();
    let mut input_items: Vec<Value> = Vec::new();

    for msg in messages.iter() {
        match msg {
            ConversationMessage::Chat(chat_msg) => match chat_msg.role {
                ChatRole::System => {
                    instructions.push(chat_msg.content.clone());
                }
                ChatRole::User => {
                    input_items.push(json!({
                        "type": "message",
                        "role": "user",
                        "content": [{
                            "type": "input_text",
                            "text": chat_msg.content
                        }]
                    }));
                }
                ChatRole::Assistant => {
                    input_items.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "id": format!("msg_{}", Uuid::new_v4()),
                        "status": "completed",
                        "content": [{
                            "type": "output_text",
                            "text": chat_msg.content,
                            "annotations": []
                        }]
                    }));
                }
                ChatRole::Tool => {
                    input_items.push(json!({
                        "type": "message",
                        "role": "tool",
                        "content": [{
                            "type": "input_text",
                            "text": chat_msg.content
                        }]
                    }));
                }
            },
            ConversationMessage::AssistantResponse(response) => {
                let preserve_native = provider == Provider::OpenAIResponses
                    && model.is_some()
                    && response
                        .provider_response
                        .as_ref()
                        .and_then(|value| value.get("model"))
                        .and_then(Value::as_str)
                        == model;
                input_items.extend(assistant_response_to_responses_items(
                    response,
                    preserve_native,
                ));
            }
            ConversationMessage::ToolCall(tool_call) => {
                let args_str = tool_call.raw_arguments.clone().unwrap_or_else(|| {
                    serde_json::to_string(&tool_call.args).unwrap_or_else(|_| "{}".to_string())
                });
                input_items.push(json!({
                    "type": "function_call",
                    "id": format!("fc_{}", tool_call.tool_use_id),
                    "call_id": tool_call.tool_use_id,
                    "name": tool_call.tool_name,
                    "arguments": args_str
                }));
            }
            ConversationMessage::ToolResult(tool_result) => {
                input_items.push(json!({
                    "type": "function_call_output",
                    "call_id": tool_result.tool_use_id,
                    "output": tool_result.content
                }));
            }
            ConversationMessage::Reasoning(reasoning_segment) => {
                input_items.push(json!({
                    "type": "reasoning",
                    "content": [{
                        "type": "output_text",
                        "text": reasoning_segment.content
                    }]
                }));
            }
            ConversationMessage::Multimodal(multimodal_msg) => {
                let role = match multimodal_msg.role {
                    ChatRole::System => "system",
                    ChatRole::User => "user",
                    ChatRole::Assistant => "assistant",
                    ChatRole::Tool => "tool",
                };

                let parts: Vec<Value> = multimodal_msg
                    .parts
                    .iter()
                    .map(|part| {
                        if matches!(
                            provider,
                            Provider::OpenAIResponses | Provider::OpenRouterResponses
                        ) {
                            match part {
                                MediaPart::Text { text } if role == "assistant" => json!({
                                    "type": "output_text",
                                    "text": text,
                                    "annotations": []
                                }),
                                _ => media_part_to_openai_responses_format(part),
                            }
                        } else {
                            media_part_to_openai_format(part)
                        }
                    })
                    .collect();

                let mut item = json!({
                    "type": "message",
                    "role": role,
                    "content": parts
                });

                if role == "assistant" {
                    item["id"] = json!(format!("msg_{}", Uuid::new_v4()));
                    item["status"] = json!("completed");
                }

                input_items.push(item);
            }
        }
    }

    let instructions = if instructions.is_empty() {
        None
    } else {
        Some(instructions.join("\n\n"))
    };

    Ok((instructions, input_items))
}

pub fn responses_input_for_model(
    messages: &[ConversationMessage],
    provider: Provider,
    model: &str,
) -> Result<(Option<String>, Vec<Value>)> {
    responses_input(messages, provider, Some(model))
}

/// Convert messages to provider-specific format with coalescing
///
/// Provider-aware behavior:
/// - Anthropic/Bedrock/GoogleAnthropic: Coalesce consecutive ToolResults into a single user turn
///   with a content array of tool_result blocks. If the immediate next user ChatMessage exists,
///   append it as a trailing {"type":"text","text":"..."} block in the same message.
///
/// - Google GenAI: Coalesce consecutive ToolResults into a single user turn where 'content' is a list
///   of {"functionResponse": {...}} dicts. If the immediate next user ChatMessage exists, append
///   {"text": "..."} as a trailing entry.
///
/// - OpenAI/OpenRouter: Map each ToolResult to a proper tool-role message:
///   {"role":"tool","tool_call_id": tool_use_id, "content": result}
///   Keep subsequent user messages separate (no coalescing).
pub fn convert_messages_to_provider_format(
    messages: &[ConversationMessage],
    provider: Provider,
) -> Result<Vec<Value>> {
    let mut converted_messages: Vec<Value> = Vec::new();
    let mut pending_anthropic_blocks: Vec<Value> = Vec::new();
    let mut pending_google_parts: Vec<Value> = Vec::new();

    let flush_pending = |converted: &mut Vec<Value>,
                         anthropic_blocks: &mut Vec<Value>,
                         google_parts: &mut Vec<Value>| {
        match provider {
            Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic
                if !anthropic_blocks.is_empty() =>
            {
                converted.push(json!({
                    "role": "user",
                    "content": anthropic_blocks.clone()
                }));
                anthropic_blocks.clear();
            }
            Provider::GoogleGenAI if !google_parts.is_empty() => {
                converted.push(json!({
                    "role": "user",
                    "content": google_parts.clone()
                }));
                google_parts.clear();
            }
            _ => {}
        }
    };

    for msg in messages.iter() {
        match msg {
            ConversationMessage::Chat(chat_msg) => {
                // Check if this is a user message that should be merged with pending tool results
                if chat_msg.role == ChatRole::User
                    && matches!(
                        provider,
                        Provider::Anthropic
                            | Provider::Bedrock
                            | Provider::GoogleAnthropic
                            | Provider::GoogleGenAI
                    )
                {
                    // Merge with pending tool results
                    if matches!(
                        provider,
                        Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic
                    ) && !pending_anthropic_blocks.is_empty()
                    {
                        // Only add text block if content is non-empty
                        // (Anthropic rejects "text content blocks must be non-empty")
                        if !chat_msg.content.is_empty() {
                            pending_anthropic_blocks.push(anthropic_text_block(
                                &chat_msg.content,
                                chat_msg.cache_marker.as_ref(),
                            ));
                        }
                        flush_pending(
                            &mut converted_messages,
                            &mut pending_anthropic_blocks,
                            &mut pending_google_parts,
                        );
                        continue;
                    } else if provider == Provider::GoogleGenAI && !pending_google_parts.is_empty()
                    {
                        // Only add text part if content is non-empty
                        if !chat_msg.content.is_empty() {
                            pending_google_parts.push(json!({
                                "text": chat_msg.content
                            }));
                        }
                        flush_pending(
                            &mut converted_messages,
                            &mut pending_anthropic_blocks,
                            &mut pending_google_parts,
                        );
                        continue;
                    }
                }

                // Flush pending before adding this message
                flush_pending(
                    &mut converted_messages,
                    &mut pending_anthropic_blocks,
                    &mut pending_google_parts,
                );

                // Add regular chat message
                let role = match chat_msg.role {
                    ChatRole::System => "system",
                    ChatRole::User => "user",
                    ChatRole::Assistant => "assistant",
                    ChatRole::Tool => "tool",
                };

                if matches!(
                    provider,
                    Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic
                ) && chat_msg.cache_marker.is_some()
                {
                    converted_messages.push(json!({
                        "role": role,
                        "content": [anthropic_text_block(&chat_msg.content, chat_msg.cache_marker.as_ref())]
                    }));
                } else {
                    converted_messages.push(json!({
                        "role": role,
                        "content": chat_msg.content
                    }));
                }
            }

            ConversationMessage::AssistantResponse(response) => {
                flush_pending(
                    &mut converted_messages,
                    &mut pending_anthropic_blocks,
                    &mut pending_google_parts,
                );

                match provider {
                    Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                        converted_messages
                            .push(assistant_response_to_anthropic_message(response, provider));
                    }
                    Provider::GoogleGenAI => {
                        converted_messages.push(assistant_response_to_google_message(response));
                    }
                    Provider::OpenAI | Provider::OpenRouter => {
                        converted_messages
                            .push(assistant_response_to_openai_chat_message(response));
                    }
                    Provider::OpenAIResponses | Provider::OpenRouterResponses => {
                        converted_messages
                            .extend(assistant_response_to_responses_items(response, false));
                    }
                }
            }

            ConversationMessage::ToolCall(tool_call) => {
                // Flush pending before adding tool call
                flush_pending(
                    &mut converted_messages,
                    &mut pending_anthropic_blocks,
                    &mut pending_google_parts,
                );

                // Convert tool call to provider-specific assistant message format
                converted_messages.push(tool_call.to_provider_assistant_message(provider));
            }

            ConversationMessage::ToolResult(tool_result) => {
                match provider {
                    Provider::OpenAI
                    | Provider::OpenRouter
                    | Provider::OpenAIResponses
                    | Provider::OpenRouterResponses => {
                        // OpenAI: Each ToolResult becomes a separate tool message
                        flush_pending(
                            &mut converted_messages,
                            &mut pending_anthropic_blocks,
                            &mut pending_google_parts,
                        );
                        converted_messages.push(tool_result.to_provider_format(provider));
                    }
                    Provider::Anthropic | Provider::Bedrock | Provider::GoogleAnthropic => {
                        // Anthropic: Accumulate tool results for coalescing
                        pending_anthropic_blocks.push(tool_result.to_provider_format(provider));
                    }
                    Provider::GoogleGenAI => {
                        // Google GenAI: Accumulate function responses
                        pending_google_parts.push(tool_result.to_provider_format(provider));
                    }
                }
            }

            ConversationMessage::Reasoning(reasoning_segment) => {
                // Flush pending before adding reasoning
                flush_pending(
                    &mut converted_messages,
                    &mut pending_anthropic_blocks,
                    &mut pending_google_parts,
                );

                // Add reasoning segment as-is (providers handle differently)
                converted_messages.push(reasoning_segment.to_provider_format(provider));
            }

            ConversationMessage::Multimodal(multimodal_msg) => {
                // Flush pending before adding multimodal message
                flush_pending(
                    &mut converted_messages,
                    &mut pending_anthropic_blocks,
                    &mut pending_google_parts,
                );

                // Convert multimodal message to provider format
                converted_messages.push(multimodal_to_provider_format(multimodal_msg, provider));
            }
        }
    }

    // Flush any trailing pending tool results
    flush_pending(
        &mut converted_messages,
        &mut pending_anthropic_blocks,
        &mut pending_google_parts,
    );

    Ok(converted_messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openai_no_coalescing() {
        // OpenAI should NOT coalesce tool results
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Use the weather tool")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_1", "Sunny, 72°F")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_2", "Rainy, 55°F")),
            ConversationMessage::Chat(ChatMessage::user("Thanks!")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::OpenAI).unwrap();

        // Should be 4 separate messages
        assert_eq!(result.len(), 4);

        // First message: user
        assert_eq!(result[0]["role"], "user");
        assert_eq!(result[0]["content"], "Use the weather tool");

        // Second message: tool result
        assert_eq!(result[1]["role"], "tool");
        assert_eq!(result[1]["tool_call_id"], "toolu_1");

        // Third message: tool result
        assert_eq!(result[2]["role"], "tool");
        assert_eq!(result[2]["tool_call_id"], "toolu_2");

        // Fourth message: user
        assert_eq!(result[3]["role"], "user");
        assert_eq!(result[3]["content"], "Thanks!");
    }

    #[test]
    fn test_anthropic_coalesces_tool_results() {
        // Anthropic SHOULD coalesce consecutive tool results
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Use the weather tool")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_1", "Sunny, 72°F")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_2", "Rainy, 55°F")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();

        // Should be 2 messages: original user message + coalesced tool results
        assert_eq!(result.len(), 2);

        // First message: user
        assert_eq!(result[0]["role"], "user");

        // Second message: coalesced tool results
        assert_eq!(result[1]["role"], "user");
        let content = result[1]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "tool_result");
        assert_eq!(content[0]["tool_use_id"], "toolu_1");
        assert_eq!(content[1]["type"], "tool_result");
        assert_eq!(content[1]["tool_use_id"], "toolu_2");
    }

    #[test]
    fn document_bytes_use_each_providers_native_document_contract() {
        let messages = vec![ConversationMessage::Multimodal(MultimodalMessage::user(
            vec![MediaPart::document(MediaSource::base64(
                "JVBERi0xLjQ=",
                "application/pdf",
            ))],
        ))];
        let google = convert_messages_to_provider_format(&messages, Provider::GoogleGenAI).unwrap();
        assert_eq!(
            google[0]["parts"][0]["inline_data"]["mime_type"],
            "application/pdf"
        );
        assert_eq!(google[0]["parts"][0]["inline_data"]["data"], "JVBERi0xLjQ=");
        let anthropic =
            convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();
        assert_eq!(anthropic[0]["content"][0]["type"], "document");
        assert_eq!(anthropic[0]["content"][0]["source"]["data"], "JVBERi0xLjQ=");
    }

    #[test]
    fn test_anthropic_merges_trailing_user_message() {
        // Anthropic SHOULD merge trailing user message with tool results
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Use the weather tool")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_1", "Sunny, 72°F")),
            ConversationMessage::Chat(ChatMessage::user("Thanks!")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();

        // Should be 2 messages: original user + coalesced (tool result + trailing user text)
        assert_eq!(result.len(), 2);

        // Second message should have both tool result AND text
        let content = result[1]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "tool_result");
        assert_eq!(content[1]["type"], "text");
        assert_eq!(content[1]["text"], "Thanks!");
    }

    #[test]
    fn test_google_genai_coalescing() {
        // Google GenAI should coalesce tool results as functionResponse parts
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Use the weather tool")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_1", "Sunny, 72°F")),
            ConversationMessage::ToolResult(ToolResult::success("toolu_2", "Rainy, 55°F")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::GoogleGenAI).unwrap();

        // Should be 2 messages
        assert_eq!(result.len(), 2);

        // Second message: coalesced function responses
        assert_eq!(result[1]["role"], "user");
        let content = result[1]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert!(content[0].get("functionResponse").is_some());
        assert!(content[1].get("functionResponse").is_some());
    }

    #[test]
    fn test_responses_input_conversion() {
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
            ConversationMessage::ToolResult(ToolResult::success("call_1", "Result")),
        ];

        let (instructions, input) =
            convert_messages_to_responses_input(&messages, Provider::OpenAIResponses).unwrap();

        assert_eq!(instructions.unwrap(), "You are helpful");
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[1]["type"], "function_call_output");
        assert_eq!(input[1]["call_id"], "call_1");
    }

    #[test]
    fn test_google_genai_merges_trailing_user_message() {
        // Google GenAI should merge trailing user message as text part
        let messages = vec![
            ConversationMessage::ToolResult(ToolResult::success("toolu_1", "Sunny, 72°F")),
            ConversationMessage::Chat(ChatMessage::user("Thanks!")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::GoogleGenAI).unwrap();

        // Should be 1 message with both functionResponse and text
        assert_eq!(result.len(), 1);

        let content = result[0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert!(content[0].get("functionResponse").is_some());
        assert_eq!(content[1]["text"], "Thanks!");
    }

    #[test]
    fn test_system_message_preserved() {
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::OpenAI).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["role"], "system");
        assert_eq!(result[0]["content"], "You are helpful");
    }

    #[test]
    fn test_reasoning_segments_not_coalesced() {
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Think about this")),
            ConversationMessage::Reasoning(ReasoningSegment::with_index("Hmm...", 0)),
            ConversationMessage::Chat(ChatMessage::assistant("I understand")),
        ];

        let result = convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();

        // Reasoning segments should be separate messages
        assert_eq!(result.len(), 3);
        assert_eq!(result[1]["type"], "thinking");
    }

    #[test]
    fn test_anthropic_chat_cache_marker_becomes_cache_control_block() {
        let messages = vec![ConversationMessage::Chat(
            ChatMessage::user("Large reusable context").with_cache_marker(CacheMarker::Ephemeral),
        )];

        let result = convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["role"], "user");
        let content = result[0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "Large reusable context");
        assert_eq!(content[0]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn test_anthropic_chat_one_hour_cache_marker_adds_ttl() {
        let messages = vec![ConversationMessage::Chat(
            ChatMessage::user("Long-lived reusable context")
                .with_cache_marker(CacheMarker::Ephemeral1h),
        )];

        let formatted =
            convert_messages_to_provider_format(&messages, Provider::Anthropic).unwrap();
        let content = formatted[0]["content"].as_array().unwrap();
        assert_eq!(content[0]["cache_control"]["type"], "ephemeral");
        assert_eq!(content[0]["cache_control"]["ttl"], "1h");
    }
}
