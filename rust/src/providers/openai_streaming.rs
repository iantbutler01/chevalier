//! OpenAI streaming response parser
//!
//! Handles OpenAI's delta-based streaming format with index-based tool call accumulation.
//! Key differences from Anthropic:
//! - Tool calls use index-based deltas (not content_block_* events)
//! - Multiple parallel tools supported via index changes
//! - Reasoning can come alongside content in same delta
//! - Usage arrives in separate final chunk

use crate::providers::StreamChunk;
use crate::utils::parse_json_value_strict_str;
use serde_json::Value;
use std::collections::HashMap;

/// Accumulates partial tool calls by index for OpenAI streaming
#[derive(Debug, Default)]
pub struct OpenAIToolAccumulator {
    current_tool_calls: HashMap<usize, PartialToolCall>,
}

#[derive(Debug, Clone)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
}

impl OpenAIToolAccumulator {
    pub fn new() -> Self {
        Self {
            current_tool_calls: HashMap::new(),
        }
    }

    /// Start or update a tool call at given index
    pub fn update_tool(&mut self, index: usize, id: Option<&str>, name: Option<&str>, args: &str) {
        let entry = self
            .current_tool_calls
            .entry(index)
            .or_insert_with(|| PartialToolCall {
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            });

        if let Some(id) = id {
            entry.id = id.to_string();
        }
        if let Some(name) = name {
            entry.name = name.to_string();
        }
        entry.arguments.push_str(args);
    }

    /// Complete a tool call at given index, returning the JSON
    pub fn complete_tool(&mut self, index: usize) -> Option<Value> {
        self.current_tool_calls.remove(&index).and_then(|tool| {
            // Parse accumulated JSON
            parse_json_value_strict_str(&tool.arguments)
                .ok()
                .map(|args: Value| {
                    serde_json::json!({
                        "id": tool.id,
                        "type": "function",
                        "function": {
                            "name": tool.name,
                            "arguments": args
                        }
                    })
                })
        })
    }

    /// Return partial tool call data if name is known
    pub fn tool_partial(&self, index: usize) -> Option<Value> {
        self.current_tool_calls.get(&index).and_then(|tool| {
            if tool.name.is_empty() {
                return None;
            }
            let call_id = if tool.id.is_empty() {
                format!("call_{}", index)
            } else {
                tool.id.clone()
            };
            Some(serde_json::json!({
                "id": call_id,
                "type": "function",
                "function": {
                    "name": tool.name,
                    "arguments": tool.arguments
                }
            }))
        })
    }

    /// Check if tool at index is ready (has id and name)
    pub fn is_tool_ready(&self, index: usize) -> bool {
        self.current_tool_calls
            .get(&index)
            .map(|t| !t.id.is_empty() && !t.name.is_empty())
            .unwrap_or(false)
    }
}

/// Parse OpenAI streaming chunk into StreamChunks
///
/// Handles:
/// - delta.content → Content chunk
/// - delta.reasoning → Reasoning chunk
/// - delta.signature → Signature chunk
/// - delta.tool_calls → Index-based tool accumulation with parallel support
/// - usage → Usage chunk (final message)
pub fn parse_openai_chunk(
    chunk_json: &Value,
    accumulator: &mut OpenAIToolAccumulator,
    has_tools: bool,
) -> Vec<StreamChunk> {
    let mut chunks = Vec::new();

    // OpenRouter may attach usage to its final choice-bearing chunk rather than
    // sending a separate chunk with no choices.
    if let Some(usage) = chunk_json.get("usage") {
        chunks.push(StreamChunk::Usage {
            input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
            output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
            cached_tokens: usage
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_write_input_tokens: 0,
            reasoning_tokens: usage
                .get("completion_tokens_details")
                .and_then(|d| d.get("reasoning_tokens"))
                .and_then(|v| v.as_u64()),
            provider_cost_dollars: usage.get("cost").and_then(|v| v.as_f64()),
        });
    }

    // Extract delta from choices[0]
    let delta = match chunk_json
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("delta"))
    {
        Some(d) => d,
        None => return chunks,
    };

    // Handle content
    if let Some(content) = delta.get("content").and_then(|c| c.as_str())
        && !content.is_empty()
    {
        chunks.push(StreamChunk::Content(content.to_string()));
    }

    // Handle reasoning (o-series models)
    if let Some(reasoning) = delta.get("reasoning").and_then(|r| r.as_str())
        && !reasoning.is_empty()
    {
        chunks.push(StreamChunk::Reasoning(reasoning.to_string()));
    }

    // Handle signature
    if let Some(signature) = delta.get("signature").and_then(|s| s.as_str())
        && !signature.is_empty()
    {
        chunks.push(StreamChunk::Signature(signature.to_string()));
    }

    // Handle tool calls (only if tools were provided)
    if has_tools && let Some(tool_calls) = delta.get("tool_calls").and_then(|tc| tc.as_array()) {
        for tool_call in tool_calls {
            let index = tool_call["index"].as_u64().unwrap_or(0) as usize;

            // Extract delta fields
            let id = tool_call.get("id").and_then(|i| i.as_str());
            let name = tool_call
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str());
            let args = tool_call
                .get("function")
                .and_then(|f| f.get("arguments"))
                .and_then(|a| a.as_str())
                .unwrap_or("");

            // Check if this is a new tool (index changed from previous)
            let was_ready = accumulator.is_tool_ready(index);

            // Update accumulator
            accumulator.update_tool(index, id, name, args);

            // If we had a complete tool at this index before, emit completion
            // This happens when index changes (parallel tool support)
            if was_ready && (id.is_some() || name.is_some()) {
                // New tool starting at same index = previous tool complete
                if let Some(completed) = accumulator.complete_tool(index) {
                    chunks.push(StreamChunk::ToolCallComplete(completed));
                }
                // Re-add the new tool
                accumulator.update_tool(index, id, name, args);
            }

            // Emit partial tool state for UI updates
            if let Some(partial) = accumulator.tool_partial(index) {
                chunks.push(StreamChunk::ToolCallPartial(partial));
            }
        }
    }

    // Check for finish_reason to complete any remaining tools
    if has_tools
        && let Some(finish_reason) = chunk_json
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("finish_reason"))
            .and_then(|fr| fr.as_str())
        && (finish_reason == "tool_calls" || finish_reason == "stop")
    {
        // Complete all remaining tools
        let indices: Vec<usize> = accumulator.current_tool_calls.keys().copied().collect();
        for index in indices {
            if let Some(completed) = accumulator.complete_tool(index) {
                chunks.push(StreamChunk::ToolCallComplete(completed));
            }
        }
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulator_new() {
        let acc = OpenAIToolAccumulator::new();
        assert_eq!(acc.current_tool_calls.len(), 0);
    }

    #[test]
    fn test_accumulator_update_tool() {
        let mut acc = OpenAIToolAccumulator::new();
        acc.update_tool(0, Some("call_123"), Some("get_weather"), "{\"location\":");
        acc.update_tool(0, None, None, "\"SF\"}");

        let tool = acc.current_tool_calls.get(&0).unwrap();
        assert_eq!(tool.id, "call_123");
        assert_eq!(tool.name, "get_weather");
        assert_eq!(tool.arguments, "{\"location\":\"SF\"}");
    }

    #[test]
    fn test_accumulator_complete_tool() {
        let mut acc = OpenAIToolAccumulator::new();
        acc.update_tool(
            0,
            Some("call_123"),
            Some("get_weather"),
            "{\"location\":\"SF\"}",
        );

        let completed = acc.complete_tool(0).unwrap();
        assert_eq!(completed["id"], "call_123");
        assert_eq!(completed["function"]["name"], "get_weather");
        assert_eq!(completed["function"]["arguments"]["location"], "SF");

        // Should be removed after completion
        assert!(!acc.current_tool_calls.contains_key(&0));
    }

    #[test]
    fn test_accumulator_is_tool_ready() {
        let mut acc = OpenAIToolAccumulator::new();
        assert!(!acc.is_tool_ready(0));

        acc.update_tool(0, Some("call_123"), None, "");
        assert!(!acc.is_tool_ready(0)); // Need both id and name

        acc.update_tool(0, None, Some("get_weather"), "");
        assert!(acc.is_tool_ready(0)); // Now ready
    }

    #[test]
    fn test_parse_content_chunk() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "content": "Hello"
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Content(s) => assert_eq!(s, "Hello"),
            _ => panic!("Expected Content chunk"),
        }
    }

    #[test]
    fn test_parse_reasoning_chunk() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "reasoning": "Let me think..."
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Reasoning(s) => assert_eq!(s, "Let me think..."),
            _ => panic!("Expected Reasoning chunk"),
        }
    }

    #[test]
    fn test_parse_signature_chunk() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "signature": "sig_123"
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Signature(s) => assert_eq!(s, "sig_123"),
            _ => panic!("Expected Signature chunk"),
        }
    }

    #[test]
    fn test_parse_tool_call_start() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_123",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"loc"
                        }
                    }]
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, true);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::ToolCallPartial(tool) => {
                assert_eq!(tool["id"], "call_123");
                assert_eq!(tool["function"]["name"], "get_weather");
            }
            _ => panic!("Expected ToolCallPartial chunk"),
        }
        assert!(acc.is_tool_ready(0));
    }

    #[test]
    fn test_parse_tool_call_accumulation() {
        let mut acc = OpenAIToolAccumulator::new();

        // First chunk - start tool
        let chunk1 = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_123",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"location\":"
                        }
                    }]
                }
            }]
        });
        let chunks1 = parse_openai_chunk(&chunk1, &mut acc, true);
        assert!(matches!(
            chunks1.first(),
            Some(StreamChunk::ToolCallPartial(_))
        ));

        // Second chunk - continue arguments
        let chunk2 = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "\"SF\"}"
                        }
                    }]
                }
            }]
        });
        let chunks2 = parse_openai_chunk(&chunk2, &mut acc, true);
        assert!(matches!(
            chunks2.first(),
            Some(StreamChunk::ToolCallPartial(_))
        ));

        // Should have accumulated full JSON
        let tool = acc.current_tool_calls.get(&0).unwrap();
        assert_eq!(tool.arguments, "{\"location\":\"SF\"}");
    }

    #[test]
    fn test_parse_tool_call_completion_on_finish() {
        let mut acc = OpenAIToolAccumulator::new();

        // Start tool
        let chunk1 = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_123",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"location\":\"SF\"}"
                        }
                    }]
                }
            }]
        });
        parse_openai_chunk(&chunk1, &mut acc, true);

        // Finish chunk
        let chunk2 = serde_json::json!({
            "choices": [{
                "delta": {},
                "finish_reason": "tool_calls"
            }]
        });
        let chunks = parse_openai_chunk(&chunk2, &mut acc, true);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::ToolCallComplete(tc) => {
                assert_eq!(tc["id"], "call_123");
                assert_eq!(tc["function"]["name"], "get_weather");
            }
            _ => panic!("Expected ToolCallComplete chunk"),
        }
    }

    #[test]
    fn test_parse_parallel_tools() {
        let mut acc = OpenAIToolAccumulator::new();

        // Start two tools
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_123",
                            "function": {
                                "name": "get_weather",
                                "arguments": "{\"location\":\"SF\"}"
                            }
                        },
                        {
                            "index": 1,
                            "id": "call_456",
                            "function": {
                                "name": "get_time",
                                "arguments": "{\"timezone\":\"UTC\"}"
                            }
                        }
                    ]
                }
            }]
        });
        parse_openai_chunk(&chunk, &mut acc, true);

        // Finish both
        let finish = serde_json::json!({
            "choices": [{
                "delta": {},
                "finish_reason": "tool_calls"
            }]
        });
        let chunks = parse_openai_chunk(&finish, &mut acc, true);

        assert_eq!(chunks.len(), 2);
        for chunk in &chunks {
            match chunk {
                StreamChunk::ToolCallComplete(_) => {} // Expected
                _ => panic!("Expected ToolCallComplete chunk"),
            }
        }
    }

    #[test]
    fn test_parse_usage_chunk() {
        let chunk = serde_json::json!({
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "prompt_tokens_details": {
                    "cached_tokens": 25
                },
                "completion_tokens_details": { "reasoning_tokens": 12 },
                "cost": 0.00125
            }
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Usage {
                input_tokens,
                output_tokens,
                cached_tokens,
                reasoning_tokens,
                provider_cost_dollars,
                ..
            } => {
                assert_eq!(*input_tokens, 100);
                assert_eq!(*output_tokens, 50);
                assert_eq!(*cached_tokens, 25);
                assert_eq!(*reasoning_tokens, Some(12));
                assert_eq!(*provider_cost_dollars, Some(0.00125));
            }
            _ => panic!("Expected Usage chunk"),
        }
    }

    #[test]
    fn test_parse_usage_on_final_choice_chunk() {
        let chunk = serde_json::json!({
            "choices": [{ "delta": {}, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 100, "completion_tokens": 50, "cost": 0.00125 }
        });
        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert!(matches!(
            chunks.as_slice(),
            [StreamChunk::Usage {
                input_tokens: 100,
                output_tokens: 50,
                ..
            }]
        ));
    }

    #[test]
    fn test_parse_empty_content_ignored() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "content": ""
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_parse_mixed_content_and_reasoning() {
        let chunk = serde_json::json!({
            "choices": [{
                "delta": {
                    "content": "Answer: ",
                    "reasoning": "First, I'll analyze..."
                }
            }]
        });

        let mut acc = OpenAIToolAccumulator::new();
        let chunks = parse_openai_chunk(&chunk, &mut acc, false);

        assert_eq!(chunks.len(), 2);
        match &chunks[0] {
            StreamChunk::Content(s) => assert_eq!(s, "Answer: "),
            _ => panic!("Expected Content chunk"),
        }
        match &chunks[1] {
            StreamChunk::Reasoning(s) => assert_eq!(s, "First, I'll analyze..."),
            _ => panic!("Expected Reasoning chunk"),
        }
    }
}
