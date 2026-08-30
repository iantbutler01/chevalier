//! Anthropic streaming implementation
//!
//! Handles SSE parsing and progressive tool call accumulation for Anthropic API.

use serde_json::{Value, json};

use crate::utils::parse_json_value_strict_str;
use std::collections::HashMap;

use crate::providers::StreamChunk;

/// Accumulates tool calls during streaming (similar to OpenAI)
#[derive(Debug, Default)]
pub struct ToolCallAccumulator {
    /// Track tool blocks by index
    current_tool_blocks: HashMap<usize, PartialToolCall>,
    usage: StreamUsageAccumulator,
}

#[derive(Debug, Clone)]
struct PartialToolCall {
    id: String,
    name: String,
    input: String, // Accumulated JSON
}

#[derive(Debug, Default)]
struct StreamUsageAccumulator {
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    cache_write_input_tokens: u64,
}

impl ToolCallAccumulator {
    pub fn new() -> Self {
        Self {
            current_tool_blocks: HashMap::new(),
            usage: StreamUsageAccumulator::default(),
        }
    }

    /// Start tracking a new tool call
    pub fn start_tool(&mut self, index: usize, id: String, name: String) {
        self.current_tool_blocks.insert(
            index,
            PartialToolCall {
                id,
                name,
                input: String::new(),
            },
        );
    }

    /// Accumulate partial JSON input
    pub fn accumulate_input(&mut self, index: usize, partial_json: &str) -> Option<Value> {
        if let Some(tool) = self.current_tool_blocks.get_mut(&index) {
            tool.input.push_str(partial_json);

            // Return OpenAI-format partial tool call
            Some(json!({
                "id": tool.id,
                "function": {
                    "name": tool.name,
                    "arguments": tool.input
                }
            }))
        } else {
            None
        }
    }

    /// Complete and remove a tool call
    pub fn complete_tool(&mut self, index: usize) -> Option<Value> {
        if let Some(tool) = self.current_tool_blocks.remove(&index) {
            let raw_arguments = tool.input.clone();

            // Parse JSON into Value (consistent with OpenAI accumulator)
            let arguments = if raw_arguments.is_empty() {
                json!({})
            } else {
                match parse_or_repair_tool_arguments(&raw_arguments) {
                    Ok(ParseRepairOutcome::Parsed(value)) => value,
                    Ok(ParseRepairOutcome::Repaired { value, repaired }) => {
                        tracing::warn!(
                            tool_id = %tool.id,
                            tool_name = %tool.name,
                            repaired_arguments = %repaired,
                            "anthropic streamed tool arguments were truncated or malformed; repaired structurally"
                        );
                        value
                    }
                    Err(error) => {
                        tracing::warn!(
                            tool_id = %tool.id,
                            tool_name = %tool.name,
                            error = %error,
                            raw_arguments = %raw_arguments,
                            "anthropic streamed tool arguments were malformed and unrecoverable; falling back to empty object"
                        );
                        json!({})
                    }
                }
            };

            // Return OpenAI-format complete tool call
            Some(json!({
                "id": tool.id,
                "type": "function",
                "raw_arguments": raw_arguments,
                "function": {
                    "name": tool.name,
                    "arguments": arguments
                }
            }))
        } else {
            None
        }
    }

    /// Complete and remove all tracked tool calls in deterministic index order.
    pub fn drain_all_tools(&mut self) -> Vec<Value> {
        let mut indices: Vec<usize> = self.current_tool_blocks.keys().copied().collect();
        indices.sort_unstable();
        indices
            .into_iter()
            .filter_map(|index| self.complete_tool(index))
            .collect()
    }

    pub fn pending_tool_count(&self) -> usize {
        self.current_tool_blocks.len()
    }

    fn update_usage(
        &mut self,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cached_tokens: Option<u64>,
        cache_write_input_tokens: Option<u64>,
    ) -> StreamChunk {
        if let Some(value) = input_tokens {
            self.usage.input_tokens = value;
        }
        if let Some(value) = output_tokens {
            self.usage.output_tokens = value;
        }
        if let Some(value) = cached_tokens {
            self.usage.cached_tokens = value;
        }
        if let Some(value) = cache_write_input_tokens {
            self.usage.cache_write_input_tokens = value;
        }

        StreamChunk::Usage {
            input_tokens: self.usage.input_tokens,
            output_tokens: self.usage.output_tokens,
            cached_tokens: self.usage.cached_tokens,
            cache_write_input_tokens: self.usage.cache_write_input_tokens,
            reasoning_tokens: None,
            provider_cost_dollars: None,
        }
    }
}

#[derive(Debug)]
enum ParseRepairOutcome {
    Parsed(Value),
    Repaired { value: Value, repaired: String },
}

fn parse_or_repair_tool_arguments(raw: &str) -> Result<ParseRepairOutcome, serde_json::Error> {
    match parse_json_value_strict_str(raw) {
        Ok(value) => Ok(ParseRepairOutcome::Parsed(value)),
        Err(initial_error) => {
            if let Some((value, repaired)) = repair_partial_json(raw) {
                Ok(ParseRepairOutcome::Repaired { value, repaired })
            } else {
                Err(initial_error)
            }
        }
    }
}

fn repair_partial_json(raw: &str) -> Option<(Value, String)> {
    let mut boundaries: Vec<usize> = raw.char_indices().map(|(idx, _)| idx).collect();
    boundaries.push(raw.len());

    for end in boundaries.into_iter().rev() {
        let prefix = trim_incomplete_json_tail(&raw[..end]);
        if prefix.is_empty() {
            continue;
        }

        let repaired = close_json_fragment(prefix);
        if repaired.trim().is_empty() {
            continue;
        }

        if let Ok(value) = parse_json_value_strict_str(&repaired) {
            return Some((value, repaired));
        }
    }

    None
}

fn trim_incomplete_json_tail(input: &str) -> &str {
    input.trim_end_matches(|c: char| c.is_whitespace() || c == ',' || c == ':')
}

fn close_json_fragment(input: &str) -> String {
    let mut stack: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escaping = false;

    for ch in input.chars() {
        if in_string {
            if escaping {
                escaping = false;
                continue;
            }

            match ch {
                '\\' => escaping = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' if stack.last().copied() == Some(ch) => {
                stack.pop();
            }
            _ => {}
        }
    }

    let mut repaired = input.to_string();
    if in_string {
        repaired.push('"');
    }
    while let Some(close) = stack.pop() {
        repaired.push(close);
    }
    repaired
}

fn flush_pending_tools(
    results: &mut Vec<StreamChunk>,
    accumulator: &mut ToolCallAccumulator,
    reason: &str,
) {
    let pending = accumulator.pending_tool_count();
    if pending == 0 {
        return;
    }

    tracing::warn!(
        pending_tool_calls = pending,
        reason,
        "anthropic stream ended a message with unfinished tool calls; flushing pending tools"
    );

    for complete_tool in accumulator.drain_all_tools() {
        results.push(StreamChunk::ToolCallComplete(complete_tool));
    }
}

/// Parse Anthropic streaming chunk into StreamChunk
pub fn parse_anthropic_chunk(
    chunk_json: &Value,
    accumulator: &mut ToolCallAccumulator,
    has_tools: bool,
) -> Vec<StreamChunk> {
    let mut results = Vec::new();

    let chunk_type = match chunk_json["type"].as_str() {
        Some(t) => t,
        None => return results,
    };

    match chunk_type {
        "content_block_delta" => {
            let delta = &chunk_json["delta"];
            let content_type = delta["type"].as_str().unwrap_or("");

            match content_type {
                "text_delta" => {
                    if let Some(text) = delta["text"].as_str() {
                        results.push(StreamChunk::Content(text.to_string()));
                    }
                }
                "input_json_delta" if has_tools => {
                    if let Some(partial_json) = delta["partial_json"].as_str() {
                        let index = chunk_json["index"].as_u64().unwrap_or(0) as usize;
                        if let Some(partial_tool) =
                            accumulator.accumulate_input(index, partial_json)
                        {
                            results.push(StreamChunk::ToolCallPartial(partial_tool));
                        }
                    }
                }
                "thinking_delta" => {
                    if let Some(thinking) = delta["thinking"].as_str() {
                        results.push(StreamChunk::Reasoning(thinking.to_string()));
                    }
                }
                "signature_delta" => {
                    if let Some(signature) = delta["signature"].as_str() {
                        results.push(StreamChunk::Signature(signature.to_string()));
                    }
                }
                _ => {}
            }
        }

        "content_block_start" => {
            let content_block = &chunk_json["content_block"];
            if content_block["type"] == "tool_use" {
                let index = chunk_json["index"].as_u64().unwrap_or(0) as usize;
                let id = content_block["id"].as_str().unwrap_or("").to_string();
                let name = content_block["name"].as_str().unwrap_or("").to_string();
                accumulator.start_tool(index, id, name);
            }
        }

        "content_block_stop" if has_tools => {
            let index = chunk_json["index"].as_u64().unwrap_or(0) as usize;
            if let Some(complete_tool) = accumulator.complete_tool(index) {
                results.push(StreamChunk::ToolCallComplete(complete_tool));
            }
        }

        "message_delta" => {
            let usage = &chunk_json["usage"];
            if usage.is_object() {
                let input_tokens = usage["input_tokens"].as_u64().unwrap_or(0);
                let output_tokens = usage["output_tokens"].as_u64().unwrap_or(0);
                let cache_write_input_tokens = usage
                    .get("cache_creation_input_tokens")
                    .and_then(|v| v.as_u64());
                // message_delta carries output_tokens; input comes from message_start
                if output_tokens > 0 || input_tokens > 0 || cache_write_input_tokens.is_some() {
                    results.push(accumulator.update_usage(
                        None,
                        Some(output_tokens),
                        None,
                        cache_write_input_tokens,
                    ));
                }
            }

            if has_tools
                && chunk_json["delta"]["stop_reason"]
                    .as_str()
                    .map(|reason| !reason.is_empty())
                    .unwrap_or(false)
            {
                flush_pending_tools(&mut results, accumulator, "message_delta_stop_reason");
            }
        }

        "message_start" => {
            if let Some(usage) = chunk_json["message"]["usage"].as_object() {
                let input_tokens = usage
                    .get("input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let cached_tokens = usage
                    .get("cache_read_input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let cache_write_input_tokens = usage
                    .get("cache_creation_input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if input_tokens > 0 {
                    results.push(accumulator.update_usage(
                        Some(input_tokens),
                        Some(0),
                        Some(cached_tokens),
                        Some(cache_write_input_tokens),
                    ));
                }
            }
        }

        "message_stop" if has_tools => {
            flush_pending_tools(&mut results, accumulator, "message_stop");
        }

        _ => {}
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_accumulator_start() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "get_weather".to_string());

        assert_eq!(acc.current_tool_blocks.len(), 1);
        assert_eq!(acc.current_tool_blocks[&0].id, "toolu_123");
        assert_eq!(acc.current_tool_blocks[&0].name, "get_weather");
    }

    #[test]
    fn test_tool_accumulator_accumulate() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "get_weather".to_string());

        let partial1 = acc.accumulate_input(0, "{\"city\":").unwrap();
        assert_eq!(partial1["function"]["arguments"], "{\"city\":");

        let partial2 = acc.accumulate_input(0, "\"SF\"}").unwrap();
        assert_eq!(partial2["function"]["arguments"], "{\"city\":\"SF\"}");
    }

    #[test]
    fn test_tool_accumulator_complete() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "get_weather".to_string());
        acc.accumulate_input(0, "{\"city\":\"SF\"}");

        let complete = acc.complete_tool(0).unwrap();
        assert_eq!(complete["id"], "toolu_123");
        assert_eq!(complete["function"]["name"], "get_weather");
        assert_eq!(complete["function"]["arguments"]["city"], "SF");
        assert_eq!(acc.current_tool_blocks.len(), 0); // Removed
    }

    #[test]
    fn test_parse_text_delta() {
        let mut acc = ToolCallAccumulator::new();
        let chunk = json!({
            "type": "content_block_delta",
            "delta": {
                "type": "text_delta",
                "text": "Hello"
            }
        });

        let chunks = parse_anthropic_chunk(&chunk, &mut acc, false);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Content(text) => assert_eq!(text, "Hello"),
            _ => panic!("Expected Content chunk"),
        }
    }

    #[test]
    fn test_parse_tool_use_flow() {
        let mut acc = ToolCallAccumulator::new();

        // 1. content_block_start
        let start_chunk = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": "toolu_123",
                "name": "get_weather"
            }
        });
        let chunks = parse_anthropic_chunk(&start_chunk, &mut acc, true);
        assert_eq!(chunks.len(), 0); // Just initializes

        // 2. input_json_delta
        let delta_chunk = json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": "{\"city\":\"SF\"}"
            }
        });
        let chunks = parse_anthropic_chunk(&delta_chunk, &mut acc, true);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::ToolCallPartial(tool) => {
                assert_eq!(tool["id"], "toolu_123");
                assert_eq!(tool["function"]["arguments"], "{\"city\":\"SF\"}");
            }
            _ => panic!("Expected ToolCallPartial"),
        }

        // 3. content_block_stop
        let stop_chunk = json!({
            "type": "content_block_stop",
            "index": 0
        });
        let chunks = parse_anthropic_chunk(&stop_chunk, &mut acc, true);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::ToolCallComplete(tool) => {
                assert_eq!(tool["id"], "toolu_123");
                assert_eq!(tool["function"]["name"], "get_weather");
            }
            _ => panic!("Expected ToolCallComplete"),
        }
    }

    #[test]
    fn test_message_stop_flushes_pending_tool() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "write_file".to_string());
        acc.accumulate_input(0, "{\"path\":\"notes.md\",\"content\":\"hi\"}");

        let stop_chunk = json!({
            "type": "message_stop"
        });

        let chunks = parse_anthropic_chunk(&stop_chunk, &mut acc, true);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::ToolCallComplete(tool) => {
                assert_eq!(tool["id"], "toolu_123");
                assert_eq!(tool["function"]["name"], "write_file");
                assert_eq!(tool["function"]["arguments"]["path"], "notes.md");
            }
            _ => panic!("Expected ToolCallComplete"),
        }
    }

    #[test]
    fn test_message_delta_stop_reason_flushes_pending_tool() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "write_file".to_string());
        acc.accumulate_input(0, "{\"path\":\"notes.md\",\"content\":\"hi\"}");

        let delta_chunk = json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": "max_tokens"
            },
            "usage": {
                "output_tokens": 42
            }
        });

        let chunks = parse_anthropic_chunk(&delta_chunk, &mut acc, true);
        assert_eq!(chunks.len(), 2);
        assert!(matches!(&chunks[0], StreamChunk::Usage { .. }));
        match &chunks[1] {
            StreamChunk::ToolCallComplete(tool) => {
                assert_eq!(tool["id"], "toolu_123");
                assert_eq!(tool["function"]["name"], "write_file");
                assert_eq!(tool["function"]["arguments"]["path"], "notes.md");
            }
            _ => panic!("Expected ToolCallComplete"),
        }
    }

    #[test]
    fn test_usage_preserves_cached_tokens_across_message_start_and_delta() {
        let mut acc = ToolCallAccumulator::new();

        let message_start = json!({
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 1200,
                    "cache_read_input_tokens": 900,
                    "cache_creation_input_tokens": 300
                }
            }
        });

        let start_chunks = parse_anthropic_chunk(&message_start, &mut acc, false);
        assert_eq!(start_chunks.len(), 1);
        match &start_chunks[0] {
            StreamChunk::Usage {
                input_tokens,
                output_tokens,
                cached_tokens,
                cache_write_input_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, 1200);
                assert_eq!(*output_tokens, 0);
                assert_eq!(*cached_tokens, 900);
                assert_eq!(*cache_write_input_tokens, 300);
            }
            _ => panic!("Expected Usage chunk"),
        }

        let message_delta = json!({
            "type": "message_delta",
            "delta": {},
            "usage": {
                "output_tokens": 42
            }
        });

        let delta_chunks = parse_anthropic_chunk(&message_delta, &mut acc, false);
        assert_eq!(delta_chunks.len(), 1);
        match &delta_chunks[0] {
            StreamChunk::Usage {
                input_tokens,
                output_tokens,
                cached_tokens,
                cache_write_input_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, 1200);
                assert_eq!(*output_tokens, 42);
                assert_eq!(*cached_tokens, 900);
                assert_eq!(*cache_write_input_tokens, 300);
            }
            _ => panic!("Expected Usage chunk"),
        }
    }

    #[test]
    fn test_repair_partial_json_salvages_truncated_string() {
        let (value, repaired) =
            repair_partial_json("{\"path\":\"report.md\",\"content\":\"hel").unwrap();
        assert_eq!(value["path"], "report.md");
        assert_eq!(value["content"], "hel");
        assert_eq!(repaired, "{\"path\":\"report.md\",\"content\":\"hel\"}");
    }

    #[test]
    fn test_repair_partial_json_salvages_longest_valid_prefix() {
        let (value, repaired) =
            repair_partial_json("{\"path\":\"report.md\",\"content\":").unwrap();
        assert_eq!(value["path"], "report.md");
        assert_eq!(repaired, "{\"path\":\"report.md\"}");
    }

    #[test]
    fn test_complete_tool_preserves_raw_arguments() {
        let mut acc = ToolCallAccumulator::new();
        acc.start_tool(0, "toolu_123".to_string(), "write_file".to_string());
        acc.accumulate_input(0, "{\"path\":\"notes.md\",\"content\":\"hel");

        let complete = acc.complete_tool(0).unwrap();
        assert_eq!(
            complete["raw_arguments"],
            "{\"path\":\"notes.md\",\"content\":\"hel"
        );
        assert_eq!(complete["function"]["arguments"]["path"], "notes.md");
        assert_eq!(complete["function"]["arguments"]["content"], "hel");
    }

    #[test]
    fn test_parse_thinking_delta() {
        let mut acc = ToolCallAccumulator::new();
        let chunk = json!({
            "type": "content_block_delta",
            "delta": {
                "type": "thinking_delta",
                "thinking": "Let me think about this..."
            }
        });

        let chunks = parse_anthropic_chunk(&chunk, &mut acc, false);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Reasoning(text) => assert_eq!(text, "Let me think about this..."),
            _ => panic!("Expected Reasoning chunk"),
        }
    }

    #[test]
    fn test_parse_signature_delta() {
        let mut acc = ToolCallAccumulator::new();
        let chunk = json!({
            "type": "content_block_delta",
            "delta": {
                "type": "signature_delta",
                "signature": "sig_abc123"
            }
        });

        let chunks = parse_anthropic_chunk(&chunk, &mut acc, false);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            StreamChunk::Signature(sig) => assert_eq!(sig, "sig_abc123"),
            _ => panic!("Expected Signature chunk"),
        }
    }
}
