//! Tool Call Hydration Example - Real LLM Calls
//!
//! Demonstrates how tool handlers receive typed structs instead of raw JSON
//! when using chevalier's native tool calling with actual LLM API calls.
//!
//! Run with:
//! ```bash
//! OPENROUTER_API_KEY=xxx cargo run --example tool_hydration
//! ```

use chevalier::agentic;
use chevalier::error::Result;
use chevalier::parsers::{Deserializable, FieldDescription};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[allow(clippy::too_many_arguments)]
fn run_params(
    prompt: Option<&str>,
    system: Option<&str>,
    history: Option<Vec<chevalier::utils::ConversationMessage>>,
    output_type: Option<&str>,
    output_schema: Option<serde_json::Value>,
    temperature: Option<f32>,
    top_p: Option<f32>,
    max_tokens: Option<u32>,
    model: Option<&str>,
    api_key: Option<&str>,
) -> chevalier::runtime::RunParams {
    chevalier::runtime::RunParams {
        prompt: prompt.map(|s| s.to_string()),
        system: system.map(|s| s.to_string()),
        history,
        output_type: output_type.map(|s| s.to_string()),
        output_schema,
        temperature,
        top_p,
        max_tokens,
        reasoning_effort: None,
        model: model.map(|s| s.to_string()),
        api_key: api_key.map(|s| s.to_string()),
        timeout: None,
        retry_config: None,
        previous_response_id: None,
    }
}

// =============================================================================
// Tool Input Types - implement Deserializable for hydration
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WeatherQuery {
    location: String,
    #[serde(default)]
    units: Option<String>,
}

impl Deserializable for WeatherQuery {
    fn from_partial(value: serde_json::Value) -> Result<Self> {
        serde_json::from_value(value)
            .map_err(|e| chevalier::error::Error::NonRetryable(format!("Failed to parse: {}", e)))
    }

    fn validate_complete(&self) -> Result<()> {
        Ok(())
    }

    fn field_descriptions() -> Vec<FieldDescription> {
        vec![
            FieldDescription {
                name: "location".to_string(),
                field_type: "string".to_string(),
                description: "The city to get weather for".to_string(),
                required: true,
            },
            FieldDescription {
                name: "units".to_string(),
                field_type: "string".to_string(),
                description: "celsius or fahrenheit".to_string(),
                required: false,
            },
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MathOperation {
    a: i64,
    b: i64,
}

impl Deserializable for MathOperation {
    fn from_partial(value: serde_json::Value) -> Result<Self> {
        serde_json::from_value(value)
            .map_err(|e| chevalier::error::Error::NonRetryable(format!("Failed to parse: {}", e)))
    }

    fn validate_complete(&self) -> Result<()> {
        Ok(())
    }

    fn field_descriptions() -> Vec<FieldDescription> {
        vec![
            FieldDescription {
                name: "a".to_string(),
                field_type: "integer".to_string(),
                description: "First number".to_string(),
                required: true,
            },
            FieldDescription {
                name: "b".to_string(),
                field_type: "integer".to_string(),
                description: "Second number".to_string(),
                required: true,
            },
        ]
    }
}

// =============================================================================
// Tool Handlers - receive TYPED structs, not serde_json::Value!
// =============================================================================

fn get_weather(query: WeatherQuery) -> BoxFuture<'static, Result<String>> {
    Box::pin(async move {
        println!(
            "  📍 get_weather called: location={}, units={:?}",
            query.location, query.units
        );

        let temp = match query.location.to_lowercase().as_str() {
            "tokyo" => 25,
            "london" => 15,
            "new york" => 22,
            "paris" => 18,
            _ => 20,
        };

        let units = query.units.unwrap_or_else(|| "celsius".to_string());
        let temp_display = if units == "fahrenheit" {
            format!("{}°F", temp * 9 / 5 + 32)
        } else {
            format!("{}°C", temp)
        };

        Ok(format!(
            "Weather in {}: {}, partly cloudy",
            query.location, temp_display
        ))
    })
}

fn add_numbers(op: MathOperation) -> BoxFuture<'static, Result<String>> {
    Box::pin(async move {
        println!("  🔢 add_numbers called: a={}, b={}", op.a, op.b);
        Ok(format!("{} + {} = {}", op.a, op.b, op.a + op.b))
    })
}

fn multiply_numbers(op: MathOperation) -> BoxFuture<'static, Result<String>> {
    Box::pin(async move {
        println!("  ✖️  multiply_numbers called: a={}, b={}", op.a, op.b);
        Ok(format!("{} × {} = {}", op.a, op.b, op.a * op.b))
    })
}

// =============================================================================
// Agent with typed tools - uses real LLM
// =============================================================================

/// Agent function that uses typed tools
#[agentic(model = "openrouter:anthropic/claude-sonnet-4")]
async fn assistant(request: String, runtime: Runtime) -> Result<String> {
    let _instructions = r#"
    A helpful assistant with weather and math tools.

    Tools:
    - get_weather(location, units): Get weather for a city
    - add_numbers(a, b): Add two numbers
    - multiply_numbers(a, b): Multiply two numbers

    Help the user with their request.
    "#;

    // Register tools with types - handlers receive typed structs
    runtime
        .tool::<WeatherQuery, _>(get_weather, Some("get_weather"))
        .await?;
    runtime
        .tool::<MathOperation, _>(add_numbers, Some("add_numbers"))
        .await?;
    runtime
        .tool::<MathOperation, _>(multiply_numbers, Some("multiply_numbers"))
        .await?;

    // Initial LLM call
    let mut result = runtime
        .run(run_params(
            Some(&request),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
        .await?;

    // Tool loop
    while runtime.is_tool_call(&result) {
        let tool_name = runtime.get_tool_name(&result).unwrap_or_default();
        println!("🔧 LLM requested tool: {}", tool_name);

        // execute_tool hydrates JSON into typed struct automatically
        let tool_output = runtime.execute_tool(&result).await?;
        println!("✅ Tool result: {}", tool_output);

        // Continue with tool result
        let prompt = format!("Tool returned: {}. Respond to the user.", tool_output);
        result = runtime
            .run(run_params(
                Some(&prompt),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ))
            .await?;
    }

    Ok(result
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| result.to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("============================================================");
    println!("Tool Hydration Example - Real LLM Calls");
    println!("============================================================");

    let requests = vec![
        "What's the weather like in Tokyo?",
        "What is 42 plus 17?",
        "Multiply 15 by 8",
    ];

    for request in requests {
        println!("\n👤 User: {}", request);
        println!("----------------------------------------");

        match assistant(request.to_string()).await {
            Ok(response) => println!("\n🤖 Assistant: {}\n", response),
            Err(e) => println!("❌ Error: {:?}\n", e),
        }
    }

    Ok(())
}
