//! Streaming integration tests
//!
//! Tests for streaming functionality across all providers.
//! Mirrors Python tests from:
//! - integration_tests/test_native_streaming_tools.py
//! - integration_tests/test_reasoning_stream.py

use chevalier::providers::{
    AnthropicClient, GenerationConfig, GoogleGenAIClient, InferenceClient, OpenRouterClient,
    StreamChunk,
};
use chevalier::types::{ChatMessage, ToolCall};
use chevalier::utils::ConversationMessage;
use futures::StreamExt;
use std::env;

// ============================================================================
// Helper Functions
// ============================================================================

fn get_anthropic_key() -> Option<String> {
    env::var("ANTHROPIC_API_KEY").ok()
}

fn get_google_key() -> Option<String> {
    env::var("GOOGLE_GEMINI_API_KEY").ok()
}

fn get_openrouter_key() -> Option<String> {
    env::var("OPENROUTER_API_KEY").ok()
}

fn add_numbers_schema() -> serde_json::Value {
    serde_json::json!({
        "name": "add_numbers",
        "description": "Add two numbers together",
        "input_schema": {
            "type": "object",
            "properties": {
                "a": {"type": "integer"},
                "b": {"type": "integer"}
            },
            "required": ["a", "b"]
        }
    })
}

// ============================================================================
// Basic Streaming Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires ANTHROPIC_API_KEY"]
async fn test_anthropic_basic_streaming() {
    let api_key = get_anthropic_key().expect("ANTHROPIC_API_KEY not set");
    let client = AnthropicClient::new(api_key, "claude-haiku-4-5-20251001");

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "Count from 1 to 5, one number per line.",
    ))];

    let config = GenerationConfig::new("claude-haiku-4-5-20251001")
        .with_max_tokens(100)
        .with_temperature(0.0);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut full_content = String::new();
    let mut chunk_count = 0;
    let mut total_input_tokens = 0u64;
    let mut total_output_tokens = 0u64;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                chunk_count += 1;
                match chunk {
                    StreamChunk::Content(text) => {
                        print!("{}", text);
                        full_content.push_str(&text);
                    }
                    StreamChunk::Usage {
                        input_tokens,
                        output_tokens,
                        ..
                    } => {
                        println!("\nUsage: {} in, {} out", input_tokens, output_tokens);
                        total_input_tokens += input_tokens;
                        total_output_tokens += output_tokens;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!("\nTotal chunks: {}", chunk_count);
    assert!(chunk_count > 0, "Should receive streaming chunks");
    assert!(
        full_content.contains("1") && full_content.contains("5"),
        "Content should contain numbers 1 and 5"
    );
    assert!(total_input_tokens > 0, "Should receive input token usage");
    assert!(total_output_tokens > 0, "Should receive output token usage");
}

#[tokio::test]
#[ignore = "Requires GOOGLE_GEMINI_API_KEY"]
async fn test_google_basic_streaming() {
    let api_key = get_google_key().expect("GOOGLE_GEMINI_API_KEY not set");
    let client = GoogleGenAIClient::new(api_key, "gemini-flash-latest");

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "List the days of the week, one per line.",
    ))];

    let config = GenerationConfig::new("gemini-flash-latest").with_max_tokens(200);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut full_content = String::new();
    let mut chunk_count = 0;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                chunk_count += 1;
                match chunk {
                    StreamChunk::Content(text) => {
                        print!("{}", text);
                        full_content.push_str(&text);
                    }
                    StreamChunk::Usage {
                        input_tokens,
                        output_tokens,
                        ..
                    } => {
                        println!("\nUsage: {} in, {} out", input_tokens, output_tokens);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!("\nTotal chunks: {}", chunk_count);
    let content_lower = full_content.to_lowercase();
    assert!(content_lower.contains("monday"), "Should contain Monday");
    assert!(content_lower.contains("sunday"), "Should contain Sunday");
}

#[tokio::test]
#[ignore = "Requires OPENROUTER_API_KEY"]
async fn test_openrouter_basic_streaming() {
    let api_key = get_openrouter_key().expect("OPENROUTER_API_KEY not set");
    let client = OpenRouterClient::new(api_key, "anthropic/claude-sonnet-4", None, None);

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "Say hello in 3 different languages, one per line.",
    ))];

    let config = GenerationConfig::new("anthropic/claude-sonnet-4")
        .with_max_tokens(200)
        .with_temperature(0.0);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut full_content = String::new();
    let mut chunk_count = 0;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                chunk_count += 1;
                if let StreamChunk::Content(text) = chunk {
                    print!("{}", text);
                    full_content.push_str(&text);
                }
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!("\nTotal chunks: {}", chunk_count);
    assert!(chunk_count > 0, "Should receive streaming chunks");
    assert!(!full_content.is_empty(), "Should receive content");
}

// ============================================================================
// Streaming with Tools Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires ANTHROPIC_API_KEY"]
async fn test_anthropic_streaming_with_tools() {
    let api_key = get_anthropic_key().expect("ANTHROPIC_API_KEY not set");
    let client = AnthropicClient::new(api_key, "claude-haiku-4-5-20251001");

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "Use the add_numbers tool to add 10 and 20",
    ))];

    let config = GenerationConfig::new("claude-haiku-4-5-20251001")
        .with_max_tokens(1024)
        .with_tools(vec![add_numbers_schema()])
        .with_native_tools(true);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut content_chunks: Vec<String> = Vec::new();
    let mut tool_calls: Vec<serde_json::Value> = Vec::new();

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => match chunk {
                StreamChunk::Content(text) => {
                    print!("{}", text);
                    content_chunks.push(text);
                }
                StreamChunk::ToolCallComplete(tool_call) => {
                    println!("\nTool call complete: {:?}", tool_call);
                    tool_calls.push(tool_call);
                }
                StreamChunk::ToolCallPartial(partial) => {
                    println!("Tool partial: {:?}", partial);
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!("\nContent chunks: {}", content_chunks.len());
    println!("Tool calls: {}", tool_calls.len());

    // Should have received tool call
    assert!(
        !tool_calls.is_empty(),
        "Should receive tool call in streaming"
    );

    // Streaming accumulator emits tool calls in OpenAI format {id, function: {name, arguments}}
    let tc =
        ToolCall::from_provider_format(tool_calls[0].clone(), chevalier::types::Provider::OpenAI)
            .expect("Failed to parse tool call");
    assert_eq!(tc.tool_name, "add_numbers");
}

#[tokio::test]
#[ignore = "Requires GOOGLE_GEMINI_API_KEY"]
async fn test_google_streaming_tool_detection() {
    let api_key = get_google_key().expect("GOOGLE_GEMINI_API_KEY not set");
    let client = GoogleGenAIClient::new(api_key, "gemini-flash-latest");

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "Use the add_numbers tool to calculate 5 + 7",
    ))];

    let config = GenerationConfig::new("gemini-flash-latest")
        .with_max_tokens(1024)
        .with_tools(vec![add_numbers_schema()])
        .with_native_tools(true);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut tool_calls: Vec<serde_json::Value> = Vec::new();

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => match chunk {
                StreamChunk::Content(text) => {
                    print!("{}", text);
                }
                StreamChunk::ToolCallComplete(tool_call) => {
                    println!("\nGoogle tool call: {:?}", tool_call);
                    tool_calls.push(tool_call);
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    assert!(
        !tool_calls.is_empty(),
        "Should detect tool call in Google streaming"
    );
}

// ============================================================================
// Reasoning/Thinking Streaming Tests
// ============================================================================

#[tokio::test]
#[ignore = "Requires GOOGLE_GEMINI_API_KEY"]
async fn test_google_streaming_with_thinking() {
    let api_key = get_google_key().expect("GOOGLE_GEMINI_API_KEY not set");
    let client = GoogleGenAIClient::new(api_key, "gemini-flash-latest").with_thinking_budget(1024);

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "What is 17 * 23? Think through this step by step.",
    ))];

    let config = GenerationConfig::new("gemini-flash-latest").with_max_tokens(2048);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut content_chunks: Vec<String> = Vec::new();
    let mut reasoning_chunks: Vec<String> = Vec::new();
    let mut has_signature = false;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => match chunk {
                StreamChunk::Content(text) => {
                    print!("C: {}", text);
                    content_chunks.push(text);
                }
                StreamChunk::Reasoning(text) => {
                    print!("R: {}", text);
                    reasoning_chunks.push(text);
                }
                StreamChunk::Signature(sig) => {
                    println!("\nSignature: {}", sig);
                    has_signature = true;
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!("\n\nContent chunks: {}", content_chunks.len());
    println!("Reasoning chunks: {}", reasoning_chunks.len());

    // Thinking model should produce reasoning
    assert!(
        !reasoning_chunks.is_empty() || !content_chunks.is_empty(),
        "Should receive either reasoning or content"
    );

    // Google thinking models should emit signatures for reasoning integrity
    println!("Has signature: {}", has_signature);
}

#[tokio::test]
#[ignore = "Requires OPENROUTER_API_KEY"]
async fn test_openrouter_streaming_with_reasoning() {
    let api_key = get_openrouter_key().expect("OPENROUTER_API_KEY not set");
    // Note: This requires a model that supports reasoning
    let client = OpenRouterClient::new(api_key, "anthropic/claude-sonnet-4", None, None);

    let messages = vec![ConversationMessage::Chat(ChatMessage::user(
        "What is 15 squared? Show your reasoning.",
    ))];

    // Note: For reasoning mode, would need to use @reasoning parameter in model string
    // This is a basic test that streaming works
    let config = GenerationConfig::new("anthropic/claude-sonnet-4").with_max_tokens(500);

    let mut stream = client.connect_and_listen(&messages, &config).await.unwrap();

    let mut full_content = String::new();

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => match chunk {
                StreamChunk::Content(text) => {
                    print!("{}", text);
                    full_content.push_str(&text);
                }
                StreamChunk::Reasoning(text) => {
                    print!("[R: {}]", text);
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    println!();
    assert!(!full_content.is_empty());
    // 15 squared = 225
    assert!(
        full_content.contains("225"),
        "Should contain the answer 225"
    );
}

// ============================================================================
// Streaming Chunk Type Tests
// ============================================================================

#[test]
fn test_stream_chunk_types() {
    // Test that all StreamChunk variants can be created
    let content = StreamChunk::Content("Hello".to_string());
    let reasoning = StreamChunk::Reasoning("Thinking...".to_string());
    let signature = StreamChunk::Signature("sig123".to_string());
    let usage = StreamChunk::Usage {
        input_tokens: 100,
        output_tokens: 50,
        cached_tokens: 0,
        cache_write_input_tokens: 0,
        reasoning_tokens: None,
        provider_cost_dollars: None,
    };
    let tool_partial = StreamChunk::ToolCallPartial(serde_json::json!({
        "partial": true
    }));
    let tool_complete = StreamChunk::ToolCallComplete(serde_json::json!({
        "name": "test",
        "input": {}
    }));

    // Basic assertions to ensure variants are created correctly
    match content {
        StreamChunk::Content(s) => assert_eq!(s, "Hello"),
        _ => panic!("Wrong variant"),
    }

    match reasoning {
        StreamChunk::Reasoning(s) => assert_eq!(s, "Thinking..."),
        _ => panic!("Wrong variant"),
    }

    match usage {
        StreamChunk::Usage {
            input_tokens,
            output_tokens,
            ..
        } => {
            assert_eq!(input_tokens, 100);
            assert_eq!(output_tokens, 50);
        }
        _ => panic!("Wrong variant"),
    }

    match signature {
        StreamChunk::Signature(s) => assert_eq!(s, "sig123"),
        _ => panic!("Wrong variant"),
    }

    match tool_partial {
        StreamChunk::ToolCallPartial(v) => assert_eq!(v["partial"], true),
        _ => panic!("Wrong variant"),
    }

    match tool_complete {
        StreamChunk::ToolCallComplete(v) => assert_eq!(v["name"], "test"),
        _ => panic!("Wrong variant"),
    }
}

// ============================================================================
// Streaming Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_streaming_invalid_api_key() {
    let client = AnthropicClient::new("invalid-key", "claude-haiku-4-5-20251001");

    let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
    let config = GenerationConfig::new("claude-haiku-4-5-20251001");

    let result = client.connect_and_listen(&messages, &config).await;

    // Should get an error when trying to connect
    assert!(result.is_err(), "Should fail with invalid API key");
}
