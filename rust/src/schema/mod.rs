//! Schema generation for tools and structured outputs
//!
//! Converts Rust types to provider-specific tool schemas and output schemas.

use crate::error::{Error, Result};
use serde_json::Value;

mod normalized;

pub use normalized::{AdditionalProperties, SchemaType, ToolParametersSchema};

/// Fix output schema for provider-specific requirements
///
/// Different providers have different JSON Schema requirements:
/// - OpenAI: Requires `additionalProperties: false` and all properties in `required`
/// - Google: Does NOT support `additionalProperties`, uses standard JSON Schema
/// - Anthropic: Similar to OpenAI but with different structured output format
///
/// This function modifies the schema in-place to match provider requirements.
pub fn fix_output_schema_for_provider(schema: &mut Value, provider: &str) {
    match provider {
        "openai"
        | "openrouter"
        | "openai-responses"
        | "openrouter-responses"
        | "openai-codex-responses" => fix_schema_for_openai(schema),
        "anthropic" | "kimi-coding" | "bedrock" | "google-anthropic" | "vertexai" => {
            fix_schema_for_anthropic(schema)
        }
        "google" | "google_gemini" | "vertex_gemini" | "gemini" | "google-genai"
        | "google-gemini" => fix_schema_for_google(schema),
        _ => {
            // Default: apply OpenAI-style fixes (most restrictive)
            fix_schema_for_openai(schema)
        }
    }
}

/// Fix a provider-specific tool schema in-place.
///
/// This normalizes manually supplied tool schemas before they are sent to a
/// provider client. Runtime-generated schemas already pass through
/// `fix_output_schema_for_provider`.
pub fn fix_tool_schema_for_provider(tool: &mut Value, provider: &str) {
    if let Ok(generator) = get_schema_generator(provider) {
        let extracted = tool
            .get("function")
            .map(|function| {
                (
                    function["name"].as_str().unwrap_or_default().to_string(),
                    function["description"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    function["parameters"].clone(),
                    function.get("strict").and_then(|value| value.as_bool()),
                )
            })
            .or_else(|| {
                (tool.get("type").and_then(|v| v.as_str()) == Some("function")).then(|| {
                    (
                        tool["name"].as_str().unwrap_or_default().to_string(),
                        tool["description"].as_str().unwrap_or_default().to_string(),
                        tool["parameters"].clone(),
                        tool.get("strict").and_then(|value| value.as_bool()),
                    )
                })
            })
            .or_else(|| {
                tool.get("name").and_then(|name| name.as_str()).map(|name| {
                    (
                        name.to_string(),
                        tool["description"].as_str().unwrap_or_default().to_string(),
                        tool.get("input_schema")
                            .cloned()
                            .or_else(|| tool.get("parameters").cloned())
                            .unwrap_or(Value::Null),
                        tool.get("strict").and_then(|value| value.as_bool()),
                    )
                })
            });

        if let Some((name, description, mut parameters, strict)) = extracted
            && !name.is_empty()
            && !parameters.is_null()
        {
            fix_output_schema_for_provider(&mut parameters, provider);
            *tool = generator.generate_schema(&name, &description, parameters);
            apply_tool_strict_for_provider(tool, provider, strict);
            return;
        }
    }

    match provider {
        "anthropic" | "kimi-coding" | "bedrock" | "google-anthropic" | "vertexai" => {
            if let Some(input_schema) = tool.get_mut("input_schema") {
                fix_output_schema_for_provider(input_schema, provider);
            }
        }
        "openai" | "openrouter" => {
            if let Some(parameters) = tool
                .get_mut("function")
                .and_then(|v| v.get_mut("parameters"))
            {
                fix_output_schema_for_provider(parameters, provider);
            }
        }
        "openai-responses"
        | "openrouter-responses"
        | "openai-codex-responses"
        | "google"
        | "google_gemini"
        | "vertex_gemini"
        | "gemini"
        | "google-genai"
        | "google-gemini" => {
            if let Some(parameters) = tool.get_mut("parameters") {
                fix_output_schema_for_provider(parameters, provider);
            }
        }
        _ => {}
    }
}

/// Fix schema for OpenAI's strict structured outputs
/// - Add `additionalProperties: false` to all objects
/// - Convert optional fields to required+nullable
fn fix_schema_for_openai(schema: &mut Value) {
    if let Value::Object(map) = schema {
        // If this is an object type with properties
        if schema_type_includes(map, "object") {
            // Add additionalProperties: false
            map.insert("additionalProperties".to_string(), Value::Bool(false));
            require_all_properties_with_nullable_optionals(map);
        }

        // Recurse into nested schemas
        recurse_schema_fix(schema, fix_schema_for_openai);
    }
}

/// Fix schema for Anthropic's structured outputs
/// - Add `additionalProperties: false` to all objects
/// - Convert optional fields to required+nullable to avoid strict grammar blowups
fn fix_schema_for_anthropic(schema: &mut Value) {
    if let Value::Object(map) = schema {
        // If this is an object type with properties
        if schema_type_includes(map, "object") {
            // Add additionalProperties: false
            map.insert("additionalProperties".to_string(), Value::Bool(false));
            require_all_properties_with_nullable_optionals(map);
        }

        // Recurse into nested schemas
        recurse_schema_fix(schema, fix_schema_for_anthropic);
    }
}

fn schema_type_includes(map: &serde_json::Map<String, Value>, expected: &str) -> bool {
    match map.get("type") {
        Some(Value::String(kind)) => kind == expected,
        Some(Value::Array(kinds)) => kinds.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    }
}

fn require_all_properties_with_nullable_optionals(map: &mut serde_json::Map<String, Value>) {
    let mut required: Vec<String> = map
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();

    if let Some(Value::Object(props)) = map.get_mut("properties") {
        for (name, prop_schema) in props.iter_mut() {
            if !required.iter().any(|entry| entry == name) {
                if is_degenerate_empty_object_schema(prop_schema) {
                    continue;
                }
                make_schema_nullable(prop_schema);
                required.push(name.clone());
            }
        }
    }

    map.insert(
        "required".to_string(),
        Value::Array(required.into_iter().map(Value::String).collect()),
    );
}

fn is_degenerate_empty_object_schema(schema: &Value) -> bool {
    let Value::Object(map) = schema else {
        return false;
    };

    if !schema_type_includes(map, "object") {
        return false;
    }

    let has_properties = map
        .get("properties")
        .and_then(|value| value.as_object())
        .is_some_and(|properties| !properties.is_empty());
    let has_items = map.get("items").is_some();
    let has_combinators = ["anyOf", "oneOf", "allOf"]
        .iter()
        .any(|key| map.get(*key).is_some());
    // @dive: `additionalProperties: false` alone (without any properties/patternProperties)
    //        is NOT a shape constraint that disqualifies a schema from being degenerate-empty.
    //        Treating it as one broke idempotence: after one pass of the fixer adds
    //        `additionalProperties: false`, a second pass would see the schema as non-degenerate
    //        and nullable-promote it, producing unions that bloat the strict-mode grammar.
    //        Only treat `additionalProperties: <schema>` (not bool) as a real shape constraint.
    let has_shape_constraints = map.get("enum").is_some()
        || map.get("const").is_some()
        || map.get("patternProperties").is_some()
        || map
            .get("additionalProperties")
            .is_some_and(|value| value.is_object());

    !has_properties && !has_items && !has_combinators && !has_shape_constraints
}

fn make_schema_nullable(schema: &mut Value) {
    if let Value::Object(map) = schema {
        match map.get_mut("type") {
            Some(Value::String(kind)) => {
                if kind != "null" {
                    let original = kind.clone();
                    map.insert(
                        "type".to_string(),
                        Value::Array(vec![Value::String(original), Value::String("null".into())]),
                    );
                }
            }
            Some(Value::Array(kinds)) => {
                let has_null = kinds.iter().any(|value| value.as_str() == Some("null"));
                if !has_null {
                    kinds.push(Value::String("null".into()));
                }
            }
            _ => {
                let original = Value::Object(map.clone());
                *schema = serde_json::json!({
                    "anyOf": [original, { "type": "null" }]
                });
            }
        }
    }
}

/// Fix schema for Google's structured outputs
/// - Remove `additionalProperties` (Google doesn't support it)
/// - Remove `$defs` and inline references (Google doesn't support $ref)
/// - Keep original required fields
fn fix_schema_for_google(schema: &mut Value) {
    // First, inline any $defs references
    inline_schema_refs(schema);

    if let Value::Object(map) = schema {
        // Remove additionalProperties (Google doesn't support it)
        map.remove("additionalProperties");

        // Remove $defs (already inlined)
        map.remove("$defs");
        map.remove("definitions");

        // Remove title (Google may not need it)
        // map.remove("title"); // Keep title, it's informative

        // Recurse into nested schemas
        recurse_schema_fix(schema, fix_schema_for_google);
    }
}

/// Fix tool parameter schemas for provider-specific requirements.
///
/// This is intentionally less aggressive than structured-output normalization.
/// Tool schemas should preserve optionality and nested structure.
pub fn fix_tool_parameters_schema_for_provider(schema: &mut Value, provider: &str) {
    match provider {
        "google" | "google_gemini" | "vertex_gemini" | "gemini" | "google-genai"
        | "google-gemini" => fix_schema_for_google(schema),
        _ => {}
    }
}

/// Inline $ref references by copying from $defs
fn inline_schema_refs(schema: &mut Value) {
    // Extract $defs if present
    let defs = if let Value::Object(map) = schema {
        map.get("$defs")
            .cloned()
            .or_else(|| map.get("definitions").cloned())
    } else {
        None
    };

    if let Some(defs) = defs {
        inline_refs_recursive(schema, &defs);
    }
}

/// Recursively replace $ref with actual definitions
fn inline_refs_recursive(schema: &mut Value, defs: &Value) {
    match schema {
        Value::Object(map) => {
            // Check if this object has a $ref
            if let Some(Value::String(ref_path)) = map.get("$ref").cloned() {
                // Extract definition name from "#/$defs/Name" or "#/definitions/Name"
                let def_name = ref_path
                    .strip_prefix("#/$defs/")
                    .or_else(|| ref_path.strip_prefix("#/definitions/"));

                if let Some(name) = def_name
                    && let Some(def) = defs.get(name)
                {
                    // Replace the entire object with the definition
                    *schema = def.clone();
                    // Recurse into the inlined definition
                    inline_refs_recursive(schema, defs);
                    return;
                }
            }

            // Recurse into all values
            for (_, v) in map.iter_mut() {
                inline_refs_recursive(v, defs);
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                inline_refs_recursive(item, defs);
            }
        }
        _ => {}
    }
}

/// Helper to recurse into nested schema structures
fn recurse_schema_fix(schema: &mut Value, fix_fn: fn(&mut Value)) {
    if let Value::Object(map) = schema {
        // Recurse into properties
        if let Some(Value::Object(props)) = map.get_mut("properties") {
            for (_, prop_schema) in props.iter_mut() {
                fix_fn(prop_schema);
            }
        }

        // Recurse into items (for arrays)
        if let Some(items) = map.get_mut("items") {
            fix_fn(items);
        }

        // Recurse into $defs
        if let Some(Value::Object(defs)) = map.get_mut("$defs") {
            for (_, def_schema) in defs.iter_mut() {
                fix_fn(def_schema);
            }
        }

        // Recurse into definitions
        if let Some(Value::Object(defs)) = map.get_mut("definitions") {
            for (_, def_schema) in defs.iter_mut() {
                fix_fn(def_schema);
            }
        }

        // Recurse into anyOf/oneOf/allOf
        for key in &["anyOf", "oneOf", "allOf"] {
            if let Some(Value::Array(variants)) = map.get_mut(*key) {
                for variant in variants.iter_mut() {
                    fix_fn(variant);
                }
            }
        }
    }
}

/// Trait for generating provider-specific schemas
pub trait SchemaGenerator: Send + Sync {
    /// Generate a schema for a tool
    ///
    /// # Arguments
    /// * `name` - Tool name
    /// * `description` - Tool description
    /// * `parameters` - JSON schema for parameters
    ///
    /// # Returns
    /// Provider-specific schema JSON
    fn generate_schema(&self, name: &str, description: &str, parameters: Value) -> Value;
}

/// Anthropic schema generator
pub struct AnthropicSchemaGenerator;

impl SchemaGenerator for AnthropicSchemaGenerator {
    fn generate_schema(&self, name: &str, description: &str, parameters: Value) -> Value {
        serde_json::json!({
            "name": name,
            "description": description,
            "input_schema": parameters,
        })
    }
}

/// OpenAI schema generator
pub struct OpenAISchemaGenerator;

impl SchemaGenerator for OpenAISchemaGenerator {
    fn generate_schema(&self, name: &str, description: &str, parameters: Value) -> Value {
        serde_json::json!({
            "type": "function",
            "function": {
                "name": name,
                "description": description,
                "parameters": parameters,
            }
        })
    }
}

/// OpenAI Responses schema generator
pub struct OpenAIResponsesSchemaGenerator;

impl SchemaGenerator for OpenAIResponsesSchemaGenerator {
    fn generate_schema(&self, name: &str, description: &str, parameters: Value) -> Value {
        serde_json::json!({
            "type": "function",
            "name": name,
            "description": description,
            "parameters": parameters,
        })
    }
}

/// Google GenAI schema generator
pub struct GoogleSchemaGenerator;

impl SchemaGenerator for GoogleSchemaGenerator {
    fn generate_schema(&self, name: &str, description: &str, parameters: Value) -> Value {
        serde_json::json!({
            "name": name,
            "description": description,
            "parameters": parameters,
        })
    }
}

pub(crate) fn apply_tool_strict_for_provider(
    tool: &mut Value,
    provider: &str,
    strict: Option<bool>,
) {
    let Some(strict) = strict else { return };
    match provider {
        "anthropic" | "kimi-coding" | "bedrock" | "google-anthropic" | "vertexai" => {
            if let Some(object) = tool.as_object_mut() {
                object.insert("strict".to_string(), Value::Bool(strict));
            }
        }
        "openai" | "openrouter" | "custom-openai" => {
            if let Some(function) = tool
                .get_mut("function")
                .and_then(|value| value.as_object_mut())
            {
                function.insert("strict".to_string(), Value::Bool(strict));
            }
        }
        "openai-responses" | "openrouter-responses" | "openai-codex-responses" => {
            if let Some(object) = tool.as_object_mut() {
                object.insert("strict".to_string(), Value::Bool(strict));
            }
        }
        _ => {}
    }
}

/// Get appropriate schema generator for provider
pub fn get_schema_generator(provider: &str) -> Result<Box<dyn SchemaGenerator>> {
    match provider {
        "anthropic" | "kimi-coding" => Ok(Box::new(AnthropicSchemaGenerator)),
        "openai" => Ok(Box::new(OpenAISchemaGenerator)),
        "openrouter" => Ok(Box::new(OpenAISchemaGenerator)), // OpenRouter uses OpenAI format
        "custom-openai" => Ok(Box::new(OpenAISchemaGenerator)), // custom-openai uses OpenAI format
        "openai-responses" => Ok(Box::new(OpenAIResponsesSchemaGenerator)),
        "openrouter-responses" => Ok(Box::new(OpenAIResponsesSchemaGenerator)),
        "openai-codex-responses" => Ok(Box::new(OpenAIResponsesSchemaGenerator)),
        "google" | "google_gemini" | "vertex_gemini" | "gemini" | "google-genai"
        | "google-gemini" => Ok(Box::new(GoogleSchemaGenerator)),
        "bedrock" => Ok(Box::new(AnthropicSchemaGenerator)), // Bedrock uses Anthropic format
        "google-anthropic" | "vertexai" => Ok(Box::new(AnthropicSchemaGenerator)), // Vertex AI with Claude
        _ => Err(Error::NonRetryable(format!(
            "Native tools not supported for provider: {}",
            provider
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anthropic_schema() {
        let generator = AnthropicSchemaGenerator;
        let params = serde_json::json!({
            "type": "object",
            "properties": {
                "location": {"type": "string"}
            }
        });

        let schema = generator.generate_schema("get_weather", "Get weather for location", params);

        assert_eq!(schema["name"], "get_weather");
        assert_eq!(schema["description"], "Get weather for location");
        assert!(schema["input_schema"].is_object());
    }

    #[test]
    fn test_openai_schema() {
        let generator = OpenAISchemaGenerator;
        let params = serde_json::json!({
            "type": "object",
            "properties": {
                "location": {"type": "string"}
            }
        });

        let schema = generator.generate_schema("get_weather", "Get weather for location", params);

        assert_eq!(schema["type"], "function");
        assert_eq!(schema["function"]["name"], "get_weather");
        assert_eq!(
            schema["function"]["description"],
            "Get weather for location"
        );
        assert!(schema["function"]["parameters"].is_object());
    }

    #[test]
    fn test_google_schema() {
        let generator = GoogleSchemaGenerator;
        let params = serde_json::json!({
            "type": "object",
            "properties": {
                "location": {"type": "string"}
            }
        });

        let schema = generator.generate_schema("get_weather", "Get weather for location", params);

        assert_eq!(schema["name"], "get_weather");
        assert_eq!(schema["description"], "Get weather for location");
        assert!(schema["parameters"].is_object());
    }

    #[test]
    fn test_anthropic_schema_preserves_strict_when_requested() {
        let generator = AnthropicSchemaGenerator;
        let mut schema = generator.generate_schema(
            "get_weather",
            "Get weather for location",
            serde_json::json!({"type": "object", "properties": {}}),
        );
        apply_tool_strict_for_provider(&mut schema, "anthropic", Some(true));

        assert_eq!(schema["strict"], true);
    }

    #[test]
    fn test_openai_schema_preserves_strict_when_requested() {
        let generator = OpenAISchemaGenerator;
        let mut schema = generator.generate_schema(
            "get_weather",
            "Get weather for location",
            serde_json::json!({"type": "object", "properties": {}}),
        );
        apply_tool_strict_for_provider(&mut schema, "openai", Some(true));

        assert_eq!(schema["function"]["strict"], true);
    }

    #[test]
    fn test_fix_tool_schema_for_provider_preserves_openai_function_strict() {
        let mut tool = serde_json::json!({
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get weather for location",
                "strict": true,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string"}
                    },
                    "required": ["location"]
                }
            }
        });

        fix_tool_schema_for_provider(&mut tool, "openai");

        assert_eq!(tool["function"]["strict"], true);
    }

    #[test]
    fn test_fix_tool_schema_for_provider_preserves_anthropic_tool_strict() {
        let mut tool = serde_json::json!({
            "name": "get_weather",
            "description": "Get weather for location",
            "strict": false,
            "input_schema": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"}
                },
                "required": ["location"]
            }
        });

        fix_tool_schema_for_provider(&mut tool, "anthropic");

        assert_eq!(tool["strict"], false);
    }

    #[test]
    fn test_get_schema_generator_anthropic() {
        let result = get_schema_generator("anthropic");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_openai() {
        let result = get_schema_generator("openai");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_openrouter() {
        let result = get_schema_generator("openrouter");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_openai_responses() {
        let result = get_schema_generator("openai-responses");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_openrouter_responses() {
        let result = get_schema_generator("openrouter-responses");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_google() {
        let result = get_schema_generator("google_gemini");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_bedrock() {
        let result = get_schema_generator("bedrock");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_schema_generator_unsupported() {
        let result = get_schema_generator("unsupported");
        assert!(result.is_err());
    }

    #[test]
    fn test_strict_schema_fixers_make_30_optionals_nullable_and_required() {
        let mut properties = serde_json::Map::new();
        for idx in 0..30 {
            properties.insert(
                format!("optional_{idx:02}"),
                serde_json::json!({"type": "string"}),
            );
        }

        for provider in ["anthropic", "openai", "openrouter", "bedrock", "vertexai"] {
            let mut schema = serde_json::json!({
                "type": "object",
                "properties": properties.clone(),
                "required": []
            });

            fix_output_schema_for_provider(&mut schema, provider);

            assert_eq!(schema["additionalProperties"], false, "provider={provider}");
            let required = schema["required"].as_array().unwrap();
            assert_eq!(required.len(), 30, "provider={provider}");
            for idx in 0..30 {
                let field = format!("optional_{idx:02}");
                assert!(
                    required.iter().any(|value| value.as_str() == Some(&field)),
                    "provider={provider} missing required field {field}"
                );
                assert_eq!(
                    schema["properties"][&field]["type"],
                    serde_json::json!(["string", "null"]),
                    "provider={provider} field={field}"
                );
            }
        }
    }

    #[test]
    fn test_required_fields_stay_non_nullable_under_strict_fixers() {
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {
                "required_field": {"type": "string"},
                "optional_field": {"type": "integer"}
            },
            "required": ["required_field"]
        });

        fix_output_schema_for_provider(&mut schema, "openai");

        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["required"],
            serde_json::json!(["required_field", "optional_field"])
        );
        assert_eq!(schema["properties"]["required_field"]["type"], "string");
        assert_eq!(
            schema["properties"]["optional_field"]["type"],
            serde_json::json!(["integer", "null"])
        );
    }

    #[test]
    fn test_strict_schema_fixers_leave_optional_empty_objects_optional() {
        for provider in ["anthropic", "openai"] {
            let mut schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" },
                    "context": { "type": "object" }
                },
                "required": ["message"]
            });

            fix_output_schema_for_provider(&mut schema, provider);

            assert_eq!(schema["additionalProperties"], false, "provider={provider}");
            assert_eq!(
                schema["required"],
                serde_json::json!(["message"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["context"]["type"], "object",
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["context"]["additionalProperties"], false,
                "provider={provider}"
            );
        }
    }

    #[test]
    fn test_strict_schema_fixers_rewrite_nested_optional_objects_recursively() {
        for provider in ["anthropic", "openai"] {
            let mut schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {
                            "label": { "type": "string" },
                            "limits": {
                                "type": "object",
                                "properties": {
                                    "soft": { "type": "integer" },
                                    "hard": { "type": "integer" }
                                },
                                "required": ["hard"]
                            }
                        },
                        "required": ["label"]
                    }
                },
                "required": []
            });

            fix_output_schema_for_provider(&mut schema, provider);

            assert_eq!(
                schema["properties"]["config"]["type"],
                serde_json::json!(["object", "null"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["additionalProperties"], false,
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["required"],
                serde_json::json!(["label", "limits"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["properties"]["limits"]["type"],
                serde_json::json!(["object", "null"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["properties"]["limits"]["required"],
                serde_json::json!(["hard", "soft"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["properties"]["limits"]["properties"]["soft"]["type"],
                serde_json::json!(["integer", "null"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["config"]["properties"]["limits"]["properties"]["hard"]["type"],
                "integer",
                "provider={provider}"
            );
        }
    }

    #[test]
    fn test_strict_schema_fixers_rewrite_arrays_and_array_items_recursively() {
        for provider in ["anthropic", "openai"] {
            let mut schema = serde_json::json!({
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": { "type": "string" },
                                "tags": {
                                    "type": "array",
                                    "items": { "type": "string" }
                                }
                            },
                            "required": ["title"]
                        }
                    }
                },
                "required": []
            });

            fix_output_schema_for_provider(&mut schema, provider);

            assert_eq!(
                schema["properties"]["steps"]["type"],
                serde_json::json!(["array", "null"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["steps"]["items"]["additionalProperties"], false,
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["steps"]["items"]["required"],
                serde_json::json!(["title", "tags"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["steps"]["items"]["properties"]["tags"]["type"],
                serde_json::json!(["array", "null"]),
                "provider={provider}"
            );
            assert_eq!(
                schema["properties"]["steps"]["items"]["properties"]["tags"]["items"]["type"],
                "string",
                "provider={provider}"
            );
        }
    }
}
