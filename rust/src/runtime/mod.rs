//! Runtime - Core execution environment for agentic functions
//!
//! The Runtime is the main interface for Chevalier. It manages:
//! - LLM client lifecycle and API calls
//! - Tool registration and execution
//! - Message history and accumulators
//! - Structured output parsing

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::parsers::{Deserializable, ParsedTool, ToolConstructor};
use crate::providers::ProviderConfig;
use crate::schema::ToolParametersSchema;
use crate::types::{AssistantResponse, ReasoningSegment, ResponseStreamEvent, ToolCall};
use crate::utils::ConversationMessage;
use futures::future::BoxFuture;

pub mod decorators;
pub mod inference;

/// Accumulated state during runtime execution
#[derive(Debug, Default, Clone)]
pub struct Accumulators {
    raw_response: Vec<String>,
    reasoning: Vec<String>,
    reasoning_segments: Vec<ReasoningSegment>,
    current_reasoning_segment: Option<ReasoningSegment>,
}

/// Stored schema information for a tool
#[derive(Debug, Clone)]
pub struct ToolSchemaInfo {
    pub asynchronous: bool,
    pub name: String,
    pub description: String,
    pub fields: Vec<crate::parsers::FieldDescription>,
    pub strict: Option<bool>,
    pub parameters: ToolParametersSchema,
}

/// Runtime - Main execution environment for agentic functions
pub struct Runtime {
    // Public configuration
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub used: bool,

    // Private state (using interior mutability)
    tools: Arc<RwLock<HashMap<String, ToolFunction>>>,
    /// Registration order of tool names. Providers serialize tool definitions
    /// in this order so that appending a tool mid-session extends the prompt
    /// prefix instead of resorting it — re-sorting (e.g. alphabetically) moves
    /// existing tool bytes and invalidates provider prompt caches from the
    /// insertion point onward.
    tool_order: Arc<RwLock<Vec<String>>>,
    model_tool_names: Arc<RwLock<Option<Vec<String>>>>,
    tool_types: Arc<RwLock<HashMap<String, String>>>, // tool_name -> type_name mapping
    tool_schemas: Arc<RwLock<HashMap<String, ToolSchemaInfo>>>, // tool_name -> schema info
    tool_constructors: Arc<RwLock<HashMap<String, Arc<ToolConstructor>>>>, // For NativeToolParser
    default_prompt: Arc<RwLock<String>>,
    system_messages: Arc<RwLock<Vec<crate::types::ChatMessage>>>,
    provider_config: Arc<RwLock<Option<ProviderConfig>>>,
    return_type: Arc<RwLock<Option<String>>>, // Store type name as string
    accumulators: Arc<RwLock<Accumulators>>,
    #[allow(dead_code)]
    messages: Arc<RwLock<Vec<ConversationMessage>>>,
    current_call_args: Arc<RwLock<Option<HashMap<String, serde_json::Value>>>>,
}

/// Wrapper for tool functions (sync or async)
///
/// Tool functions receive raw JSON args and are responsible for deserializing
/// into their expected type. The `tool<T, F>()` method handles this automatically
/// by wrapping typed handlers.
pub enum ToolFunction {
    Sync(Box<dyn Fn(serde_json::Value) -> Result<String> + Send + Sync>),
    Async(
        Box<
            dyn Fn(serde_json::Value) -> futures::future::BoxFuture<'static, Result<String>>
                + Send
                + Sync,
        >,
    ),
}

#[derive(Clone)]
pub struct ToolExecutor {
    tools: Arc<RwLock<HashMap<String, ToolFunction>>>,
}

impl ToolExecutor {
    pub async fn execute(&self, tool_call: &ToolCall) -> Result<String> {
        let tools = self.tools.read().await;
        let tool = tools.get(&tool_call.tool_name).ok_or_else(|| {
            Error::NonRetryable(format!("Tool '{}' not found", tool_call.tool_name))
        })?;
        let args = tool_call.args.clone();
        match tool {
            ToolFunction::Sync(handler) => handler(args),
            ToolFunction::Async(handler) => handler(args).await,
        }
    }
}

/// Parameters for `Runtime::run()` and `Runtime::run_stream()`
#[derive(Debug, Default, Clone)]
pub struct RunParams {
    pub prompt: Option<String>,
    pub system: Option<String>,
    pub history: Option<Vec<ConversationMessage>>,
    pub output_type: Option<String>,
    pub output_schema: Option<serde_json::Value>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
    pub reasoning_effort: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub timeout: Option<std::time::Duration>,
    /// Retry policy for the provider request. When `None`, providers fall back
    /// to `RetryConfig::default()`. Inject a longer-horizon policy from the
    /// caller for LLM workloads that may exceed the 60s default max_time.
    pub retry_config: Option<crate::retry::RetryConfig>,
    pub previous_response_id: Option<String>,
    pub responses: Option<crate::providers::responses_control::ResponsesOptions>,
}

/// Metadata about a tool call for execution context
#[derive(Debug, Clone)]
pub struct ToolCallContext {
    pub tool_name: String,
    pub tool_use_id: String,
}

impl Runtime {
    fn strip_nulls_for_non_required_fields(
        mut json_value: serde_json::Value,
        fields: &[crate::parsers::FieldDescription],
    ) -> serde_json::Value {
        let Some(object) = json_value.as_object_mut() else {
            return json_value;
        };

        for field in fields.iter().filter(|field| !field.required) {
            if object.get(&field.name).is_some_and(|value| value.is_null()) {
                object.remove(&field.name);
            }
        }

        json_value
    }

    /// Create a new Runtime with default memory storage
    pub fn new() -> Self {
        Self {
            model: None,
            api_key: None,
            used: false,
            tools: Arc::new(RwLock::new(HashMap::new())),
            tool_order: Arc::new(RwLock::new(Vec::new())),
            model_tool_names: Arc::new(RwLock::new(None)),
            tool_types: Arc::new(RwLock::new(HashMap::new())),
            tool_schemas: Arc::new(RwLock::new(HashMap::new())),
            tool_constructors: Arc::new(RwLock::new(HashMap::new())),
            default_prompt: Arc::new(RwLock::new(String::new())),
            system_messages: Arc::new(RwLock::new(Vec::new())),
            provider_config: Arc::new(RwLock::new(None)),
            return_type: Arc::new(RwLock::new(None)),
            accumulators: Arc::new(RwLock::new(Accumulators::default())),
            messages: Arc::new(RwLock::new(Vec::new())),
            current_call_args: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a Runtime with specific configuration
    pub fn with_config(model: Option<String>, api_key: Option<String>) -> Self {
        Self {
            model,
            api_key,
            used: false,
            tools: Arc::new(RwLock::new(HashMap::new())),
            tool_order: Arc::new(RwLock::new(Vec::new())),
            model_tool_names: Arc::new(RwLock::new(None)),
            tool_types: Arc::new(RwLock::new(HashMap::new())),
            tool_schemas: Arc::new(RwLock::new(HashMap::new())),
            tool_constructors: Arc::new(RwLock::new(HashMap::new())),
            default_prompt: Arc::new(RwLock::new(String::new())),
            system_messages: Arc::new(RwLock::new(Vec::new())),
            provider_config: Arc::new(RwLock::new(None)),
            return_type: Arc::new(RwLock::new(None)),
            accumulators: Arc::new(RwLock::new(Accumulators::default())),
            messages: Arc::new(RwLock::new(Vec::new())),
            current_call_args: Arc::new(RwLock::new(None)),
        }
    }

    /// Register a tool function
    ///
    /// # Arguments
    /// * `name` - Tool name for LLM
    /// * `tool_fn` - Sync or async function
    /// * `tool_type` - Optional type name for marshalling
    pub async fn register_tool(
        &self,
        name: impl Into<String>,
        tool_fn: ToolFunction,
        tool_type: Option<String>,
    ) -> Result<()> {
        let name = name.into();

        // Store tool function
        let mut tools = self.tools.write().await;
        if tools.contains_key(&name) {
            return Err(Error::NonRetryable(format!(
                "Tool '{}' is already registered",
                name
            )));
        }
        tools.insert(name.clone(), tool_fn);
        drop(tools);

        self.tool_order.write().await.push(name.clone());

        // Store tool type if provided
        if let Some(type_name) = tool_type {
            let mut tool_types = self.tool_types.write().await;
            tool_types.insert(name, type_name);
        }

        Ok(())
    }

    /// Register a tool function with schema information
    ///
    /// This method is useful when you have a tool struct that provides its schema
    /// via `#[derive(Tool)]`, and you want to register it with a handler function.
    ///
    /// # Arguments
    /// * `name` - Tool name for LLM
    /// * `description` - Tool description
    /// * `schema` - JSON schema for the tool parameters (from Tool::schema())
    /// * `tool_fn` - Sync or async function to handle the tool call
    ///
    /// # Example
    /// ```ignore
    /// runtime.register_tool_with_schema(
    ///     MyTool::tool_name(),
    ///     MyTool::description(),
    ///     MyTool::schema(),
    ///     ToolFunction::Sync(Box::new(|args| {
    ///         // Handle the tool call
    ///         Ok("result".to_string())
    ///     })),
    /// ).await?;
    /// ```
    pub async fn register_tool_with_schema(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        schema: serde_json::Value,
        tool_fn: ToolFunction,
    ) -> Result<()> {
        let name = name.into();
        let description = description.into();

        // Store tool function
        let mut tools = self.tools.write().await;
        if tools.contains_key(&name) {
            return Err(Error::NonRetryable(format!(
                "Tool '{}' is already registered",
                name
            )));
        }
        tools.insert(name.clone(), tool_fn);
        drop(tools);

        self.tool_order.write().await.push(name.clone());

        // Parse and store full schema information while preserving
        // flat field metadata for introspection and existing callers.
        let parameters = ToolParametersSchema::from_json_schema(&schema)?;
        let fields = parameters.top_level_field_descriptions();
        let schema_info = ToolSchemaInfo {
            asynchronous: false,
            name: name.clone(),
            description,
            fields,
            strict: schema.get("strict").and_then(|value| value.as_bool()),
            parameters,
        };
        let mut schemas = self.tool_schemas.write().await;
        schemas.insert(name, schema_info);

        Ok(())
    }

    /// Unregister a tool and remove all associated metadata.
    ///
    /// Returns `true` if the tool existed and was removed.
    pub async fn unregister_tool(&self, name: &str) -> bool {
        let removed = {
            let mut tools = self.tools.write().await;
            tools.remove(name).is_some()
        };

        if removed {
            self.tool_order.write().await.retain(|n| n != name);
        }

        let mut tool_types = self.tool_types.write().await;
        tool_types.remove(name);
        drop(tool_types);

        let mut tool_schemas = self.tool_schemas.write().await;
        tool_schemas.remove(name);
        drop(tool_schemas);

        let mut tool_constructors = self.tool_constructors.write().await;
        tool_constructors.remove(name);

        removed
    }

    /// Register a tool with type and handler (Python: runtime.tool(fn, name=..., tool_type=...))
    ///
    /// This is the primary API for registering tools for native tool calling.
    /// The type parameter T must implement Deserializable. The handler receives
    /// the typed struct directly - deserialization is handled automatically.
    ///
    /// # Arguments
    /// * `handler` - Async function that receives the typed struct T
    /// * `name` - Optional tool name (defaults to type name if None)
    ///
    /// # Example
    /// ```ignore
    /// #[derive(Deserialize, Serialize)]
    /// struct WeatherQuery {
    ///     city: String,
    ///     units: Option<String>,
    /// }
    /// impl Deserializable for WeatherQuery { ... }
    ///
    /// // Handler receives WeatherQuery directly, not raw JSON
    /// runtime.tool::<WeatherQuery, _>(|query| Box::pin(async move {
    ///     Ok(format!("Weather in {}: Sunny", query.city))
    /// }), Some("get_weather")).await?;
    /// ```
    pub async fn tool<T, F>(&self, handler: F, name: Option<&str>) -> Result<()>
    where
        T: Deserializable + serde::Serialize + 'static,
        F: Fn(T) -> BoxFuture<'static, Result<String>> + Send + Sync + 'static,
    {
        // Get tool name - either from parameter or type name
        let type_name = std::any::type_name::<T>();
        let tool_name_str =
            name.unwrap_or_else(|| type_name.split("::").last().unwrap_or(type_name));
        let tool_name = tool_name_str.to_string();

        // Check for duplicate registration
        let mut tools = self.tools.write().await;
        if tools.contains_key(&tool_name) {
            return Err(Error::NonRetryable(format!(
                "Tool '{}' is already registered",
                tool_name
            )));
        }

        // Wrap the typed handler to deserialize JSON -> T before calling
        let handler = Arc::new(handler);
        let nullable_field_descriptions = T::field_descriptions();
        let wrapped_handler = Box::new(move |json_value: serde_json::Value| {
            let handler = handler.clone();
            let json_value =
                Self::strip_nulls_for_non_required_fields(json_value, &nullable_field_descriptions);
            Box::pin(async move {
                // Deserialize JSON into the typed struct T
                let typed_args: T = T::from_partial(json_value)?;
                // Call the handler with the typed struct
                handler(typed_args).await
            }) as BoxFuture<'static, Result<String>>
        });

        let was_new = tools
            .insert(tool_name.clone(), ToolFunction::Async(wrapped_handler))
            .is_none();
        drop(tools);

        if was_new {
            self.tool_order.write().await.push(tool_name.clone());
        }

        // Store type name for introspection
        let mut tool_types = self.tool_types.write().await;
        tool_types.insert(tool_name.clone(), type_name.to_string());
        drop(tool_types);

        // Extract and store schema information from Deserializable type
        let field_descriptions = T::field_descriptions();
        let parameters = ToolParametersSchema::from_field_descriptions(&field_descriptions);
        let schema_info = ToolSchemaInfo {
            asynchronous: false,
            name: tool_name.clone(),
            description: format!("Tool: {}", tool_name), // Default description
            fields: field_descriptions,
            strict: None,
            parameters,
        };
        let mut schemas = self.tool_schemas.write().await;
        schemas.insert(tool_name.clone(), schema_info);
        drop(schemas);

        // Store constructor for NativeToolParser (streaming use case)
        let tool_name_clone = tool_name.clone();
        let parser_field_descriptions = T::field_descriptions();
        let constructor: ToolConstructor = Box::new(move |json: serde_json::Value| {
            let json = Self::strip_nulls_for_non_required_fields(json, &parser_field_descriptions);
            T::from_partial(json.clone()).map(|tool| {
                ParsedTool {
                    tool_name: tool_name_clone.clone(),
                    tool_use_id: String::new(), // Will be set by parser
                    value: serde_json::to_value(&tool).unwrap(),
                }
            })
        });

        let mut constructors = self.tool_constructors.write().await;
        constructors.insert(tool_name.clone(), Arc::new(constructor));

        Ok(())
    }

    /// Get a NativeToolParser instance with all registered tool constructors
    ///
    /// This parser can be used during streaming to dynamically construct ParsedTool
    /// instances from tool call deltas.
    pub async fn get_parser(&self) -> crate::parsers::NativeToolParser {
        let constructors = self.tool_constructors.read().await;
        crate::parsers::NativeToolParser::new(constructors.clone())
    }

    /// Get tool schema information for all registered tools
    pub async fn get_tool_schemas(&self) -> HashMap<String, ToolSchemaInfo> {
        let schemas = self.tool_schemas.read().await;
        schemas.clone()
    }

    pub async fn set_model_tool_names(&self, names: Option<Vec<String>>) -> Result<()> {
        let tools = self.tools.read().await;
        if let Some(names) = &names {
            for name in names {
                if !tools.contains_key(name) {
                    return Err(Error::NonRetryable(format!("Unknown tool: {name}")));
                }
            }
        }
        *self.model_tool_names.write().await = names;
        Ok(())
    }

    pub async fn set_tool_async(&self, name: &str, asynchronous: bool) -> Result<()> {
        let mut schemas = self.tool_schemas.write().await;
        let schema = schemas
            .get_mut(name)
            .ok_or_else(|| Error::NonRetryable(format!("Unknown tool: {name}")))?;
        schema.asynchronous = asynchronous;
        Ok(())
    }

    /// Execute a non-streaming LLM call
    pub async fn run(&mut self, params: RunParams) -> Result<AssistantResponse> {
        // Mark as used
        self.used = true;

        // Clear accumulators
        self.clear_raw_response().await;
        self.clear_reasoning().await;

        // Get effective values
        let prompt_text = match params.prompt {
            Some(p) => p,
            None => self.default_prompt.read().await.clone(),
        };

        let effective_model = params
            .model
            .or_else(|| self.model.clone())
            .ok_or_else(|| Error::NonRetryable("No model specified".to_string()))?;

        let effective_api_key = params.api_key.or_else(|| self.api_key.clone());
        let runtime_system_messages = self.system_messages.read().await.clone();
        let runtime_provider_config = self.provider_config.read().await.clone();

        // Call inference utilities
        let result = inference::call_llm(
            Some(&prompt_text),
            &effective_model,
            self.tools.clone(),
            self.tool_order.clone(),
            self.model_tool_names.clone(),
            self.tool_schemas.clone(),
            params.output_type,
            params.output_schema,
            effective_api_key.as_deref(),
            params.system.as_deref(),
            Some(runtime_system_messages),
            params.history,
            params.temperature,
            params.top_p,
            params.max_tokens,
            params.reasoning_effort,
            params.timeout,
            params.retry_config,
            params.previous_response_id,
            runtime_provider_config,
            self.current_call_args.clone(),
            params.responses,
        )
        .await?;

        // Update accumulators
        let text = result.response.text();
        if !text.is_empty() {
            let mut acc = self.accumulators.write().await;
            acc.raw_response.push(text);
        }

        let reasoning = result.response.reasoning();
        if !reasoning.is_empty() {
            let mut acc = self.accumulators.write().await;
            acc.reasoning.push(reasoning);
        }

        Ok(result.response)
    }

    /// Execute a streaming LLM call
    ///
    /// Returns an async stream of typed response-building events.
    pub async fn run_stream(
        &mut self,
        params: RunParams,
    ) -> Result<impl futures::stream::Stream<Item = Result<ResponseStreamEvent>>> {
        // Mark as used
        self.used = true;

        // Clear accumulators
        self.clear_raw_response().await;
        self.clear_reasoning().await;
        self.clear_reasoning_segments().await;

        // Get effective values
        let prompt_text = match params.prompt {
            Some(p) => p,
            None => self.default_prompt.read().await.clone(),
        };

        let effective_model = params
            .model
            .or_else(|| self.model.clone())
            .ok_or_else(|| Error::NonRetryable("No model specified".to_string()))?;

        let effective_api_key = params.api_key.or_else(|| self.api_key.clone());
        let runtime_system_messages = self.system_messages.read().await.clone();
        let runtime_provider_config = self.provider_config.read().await.clone();

        // Call streaming inference
        inference::call_llm_stream(
            Some(&prompt_text),
            &effective_model,
            self.tools.clone(),
            self.tool_order.clone(),
            self.model_tool_names.clone(),
            self.tool_schemas.clone(),
            params.output_type,
            params.output_schema,
            effective_api_key.as_deref(),
            params.system.as_deref(),
            Some(runtime_system_messages),
            params.history,
            params.temperature,
            params.top_p,
            params.max_tokens,
            params.reasoning_effort,
            params.timeout,
            params.retry_config,
            params.previous_response_id,
            runtime_provider_config,
            self.current_call_args.clone(),
            self.accumulators.clone(),
            params.responses,
        )
        .await
    }

    /// Execute a tool call
    pub async fn execute_tool_call(&self, tool_call: &ToolCall) -> Result<String> {
        self.tool_executor().execute(tool_call).await
    }

    pub fn tool_executor(&self) -> ToolExecutor {
        ToolExecutor {
            tools: self.tools.clone(),
        }
    }

    /// Check whether an assistant response contains tool calls.
    pub fn is_tool_call(&self, response: &AssistantResponse) -> bool {
        response.has_tool_calls()
    }

    /// Get the first tool name from an assistant response.
    pub fn get_tool_name(&self, response: &AssistantResponse) -> Option<String> {
        response
            .tool_calls()
            .first()
            .map(|tool_call| tool_call.tool_name.clone())
    }

    /// Execute the first tool call in an assistant response.
    pub async fn execute_tool(&self, response: &AssistantResponse) -> Result<String> {
        let tool_call = response.tool_calls().first().copied().ok_or_else(|| {
            Error::NonRetryable("Assistant response does not contain a tool call".to_string())
        })?;
        self.execute_tool_call(tool_call).await
    }

    /// Get accumulated raw response
    pub async fn raw_response(&self) -> String {
        let acc = self.accumulators.read().await;
        acc.raw_response.join("")
    }

    /// Get accumulated reasoning
    pub async fn reasoning(&self) -> String {
        let acc = self.accumulators.read().await;
        acc.reasoning.join("")
    }

    /// Get reasoning segments
    pub async fn reasoning_segments(&self) -> Vec<ReasoningSegment> {
        let acc = self.accumulators.read().await;
        acc.reasoning_segments.clone()
    }

    /// Clear raw response accumulator
    pub async fn clear_raw_response(&self) {
        let mut acc = self.accumulators.write().await;
        acc.raw_response.clear();
    }

    /// Clear reasoning accumulator
    pub async fn clear_reasoning(&self) {
        let mut acc = self.accumulators.write().await;
        acc.reasoning.clear();
    }

    /// Clear reasoning segments
    pub async fn clear_reasoning_segments(&self) {
        let mut acc = self.accumulators.write().await;
        acc.reasoning_segments.clear();
        acc.current_reasoning_segment = None;
    }

    /// Set default prompt (from function docstring)
    pub async fn set_default_prompt(&self, prompt: impl Into<String>) {
        let mut default_prompt = self.default_prompt.write().await;
        *default_prompt = prompt.into();
    }

    /// Set the structured system-message prefix for subsequent runs.
    pub async fn set_system_messages(&self, messages: Vec<crate::types::ChatMessage>) {
        let mut system_messages = self.system_messages.write().await;
        *system_messages = messages;
    }

    /// Set provider-specific request shaping options for subsequent runs.
    pub async fn set_provider_config(&self, config: Option<ProviderConfig>) {
        let mut provider_config = self.provider_config.write().await;
        *provider_config = config;
    }

    /// Set return type
    pub async fn set_return_type(&self, type_name: Option<String>) {
        let mut return_type = self.return_type.write().await;
        *return_type = type_name;
    }

    /// Set current call arguments (for prompt enhancement)
    pub async fn set_current_call_args(&self, args: Option<HashMap<String, serde_json::Value>>) {
        let mut current_args = self.current_call_args.write().await;
        *current_args = args;
    }

    /// Connect to an MCP server and register all its tools into this runtime.
    ///
    /// Auto-detects transport from the URI:
    /// - `http://` or `https://` → HTTP streaming
    /// - `ws://` or `wss://` → WebSocket
    /// - Anything else → stdio (treated as a command to spawn)
    ///
    /// Discovered tools become available alongside locally registered tools.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use chevalier::runtime::Runtime;
    /// # async fn example() -> chevalier::error::Result<()> {
    /// # let runtime = Runtime::new();
    /// runtime.mcp("http://localhost:8080/mcp").await?;
    /// runtime.mcp("npx @modelcontextprotocol/server-filesystem /tmp").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mcp")]
    pub async fn mcp(&self, uri: impl Into<String>) -> Result<()> {
        let _ = crate::mcp::connect_and_register(self, &uri.into(), None).await?;
        Ok(())
    }

    /// Connect to an MCP server and register its tools with a namespace prefix.
    ///
    /// Tools are registered as `{label}_{tool_name}`. The original name is
    /// still used when calling the remote server. Use this to avoid conflicts
    /// when connecting to multiple MCP servers that expose tools with the
    /// same name.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use chevalier::runtime::Runtime;
    /// # async fn example() -> chevalier::error::Result<()> {
    /// # let runtime = Runtime::new();
    /// runtime.mcp_as("http://server1:8080", "s1").await?; // s1_search, s1_read
    /// runtime.mcp_as("http://server2:8080", "s2").await?; // s2_search, s2_write
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mcp")]
    pub async fn mcp_as(&self, uri: impl Into<String>, label: &str) -> Result<()> {
        let _ = crate::mcp::connect_and_register(self, &uri.into(), Some(label)).await?;
        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_new() {
        let runtime = Runtime::new();
        assert!(!runtime.used);
        assert!(runtime.model.is_none());
    }

    #[tokio::test]
    async fn test_runtime_with_config() {
        let runtime =
            Runtime::with_config(Some("test-model".to_string()), Some("test-key".to_string()));

        assert_eq!(runtime.model, Some("test-model".to_string()));
        assert_eq!(runtime.api_key, Some("test-key".to_string()));
    }

    #[tokio::test]
    async fn test_register_tool() {
        let runtime = Runtime::new();

        let tool_fn = ToolFunction::Sync(Box::new(|_args| Ok("result".to_string())));

        runtime
            .register_tool("test_tool", tool_fn, None)
            .await
            .unwrap();

        let tools = runtime.tools.read().await;
        assert!(tools.contains_key("test_tool"));
    }

    #[tokio::test]
    async fn test_register_duplicate_tool() {
        let runtime = Runtime::new();

        let tool_fn1 = ToolFunction::Sync(Box::new(|_args| Ok("result1".to_string())));
        let tool_fn2 = ToolFunction::Sync(Box::new(|_args| Ok("result2".to_string())));

        runtime
            .register_tool("test_tool", tool_fn1, None)
            .await
            .unwrap();

        let result = runtime.register_tool("test_tool", tool_fn2, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unregister_tool_removes_registration() {
        let runtime = Runtime::new();

        let tool_fn = ToolFunction::Sync(Box::new(|_args| Ok("result".to_string())));
        runtime
            .register_tool("test_tool", tool_fn, Some("TestType".to_string()))
            .await
            .unwrap();

        let removed = runtime.unregister_tool("test_tool").await;
        assert!(removed);

        let tools = runtime.tools.read().await;
        assert!(!tools.contains_key("test_tool"));
        drop(tools);

        let tool_types = runtime.tool_types.read().await;
        assert!(!tool_types.contains_key("test_tool"));
        drop(tool_types);

        let tool_schemas = runtime.tool_schemas.read().await;
        assert!(!tool_schemas.contains_key("test_tool"));
    }

    #[tokio::test]
    async fn test_unregister_typed_tool_allows_reregister() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();

        runtime
            .tool::<WeatherQuery, _>(
                |_query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move { Ok("Sunny".to_string()) })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        let removed = runtime.unregister_tool("get_weather").await;
        assert!(removed);

        let constructors = runtime.tool_constructors.read().await;
        assert!(!constructors.contains_key("get_weather"));
        drop(constructors);

        runtime
            .tool::<WeatherQuery, _>(
                |_query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move { Ok("Rainy".to_string()) })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        let tool_call = crate::types::ToolCall::new(
            "get_weather",
            serde_json::json!({
                "location": "Paris"
            }),
        );
        let result = runtime.execute_tool_call(&tool_call).await.unwrap();
        assert_eq!(result, "Rainy");
    }

    #[tokio::test]
    async fn test_assistant_response_tool_helpers() {
        let response = crate::types::AssistantResponse::new(vec![
            crate::types::ResponsePart::Text {
                text: "hello".to_string(),
            },
            crate::types::ResponsePart::Tool {
                call: crate::types::ToolCall::new(
                    "my_tool",
                    serde_json::json!({ "arg1": "value1" }),
                ),
            },
        ]);

        assert!(response.has_tool_calls());
        assert_eq!(response.tool_calls().len(), 1);
        assert_eq!(response.tool_calls()[0].tool_name, "my_tool");
    }

    #[tokio::test]
    async fn test_execute_tool_sync() {
        let runtime = Runtime::new();

        let tool_fn = ToolFunction::Sync(Box::new(|args| {
            let name = args.get("name").and_then(|v| v.as_str()).unwrap_or("world");
            Ok(format!("Hello, {}!", name))
        }));

        runtime.register_tool("greet", tool_fn, None).await.unwrap();

        let tool_call = crate::types::ToolCall::new(
            "greet",
            serde_json::json!({
                "name": "Alice"
            }),
        );

        let result = runtime.execute_tool_call(&tool_call).await.unwrap();
        assert_eq!(result, "Hello, Alice!");
    }

    #[tokio::test]
    async fn test_accumulators() {
        let runtime = Runtime::new();

        // Initially empty
        assert_eq!(runtime.raw_response().await, "");
        assert_eq!(runtime.reasoning().await, "");

        // Add some data
        {
            let mut acc = runtime.accumulators.write().await;
            acc.raw_response.push("Hello ".to_string());
            acc.raw_response.push("World".to_string());
            acc.reasoning.push("Think: ".to_string());
            acc.reasoning.push("Answer".to_string());
        }

        assert_eq!(runtime.raw_response().await, "Hello World");
        assert_eq!(runtime.reasoning().await, "Think: Answer");

        // Clear
        runtime.clear_raw_response().await;
        runtime.clear_reasoning().await;

        assert_eq!(runtime.raw_response().await, "");
        assert_eq!(runtime.reasoning().await, "");
    }

    #[tokio::test]
    #[ignore] // TODO: Requires Storage trait refactor for interior mutability
    async fn test_context() {
        // TODO: Implement when context() method is added to Runtime
    }

    // Test types for tool registration
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct WeatherQuery {
        #[serde(default)]
        location: String,
        #[serde(default)]
        unit: Option<String>,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct SearchWithDefaultCategory {
        #[serde(default)]
        text: String,
        #[serde(default = "default_test_category")]
        category: String,
    }

    fn default_test_category() -> String {
        "general".to_string()
    }

    impl crate::parsers::Deserializable for WeatherQuery {
        fn from_partial(partial: serde_json::Value) -> crate::error::Result<Self> {
            serde_json::from_value(partial)
                .map_err(|e| crate::error::Error::NonRetryable(format!("Parse error: {}", e)))
        }

        fn validate_complete(&self) -> crate::error::Result<()> {
            if self.location.is_empty() {
                return Err(crate::error::Error::NonRetryable(
                    "location is required".to_string(),
                ));
            }
            Ok(())
        }

        fn field_descriptions() -> Vec<crate::parsers::FieldDescription> {
            vec![
                crate::parsers::FieldDescription {
                    name: "location".to_string(),
                    field_type: "string".to_string(),
                    description: "The city to get weather for".to_string(),
                    required: true,
                },
                crate::parsers::FieldDescription {
                    name: "unit".to_string(),
                    field_type: "string".to_string(),
                    description: "Temperature unit (celsius or fahrenheit)".to_string(),
                    required: false,
                },
            ]
        }
    }

    impl crate::parsers::Deserializable for SearchWithDefaultCategory {
        fn from_partial(partial: serde_json::Value) -> crate::error::Result<Self> {
            serde_json::from_value(partial)
                .map_err(|e| crate::error::Error::NonRetryable(format!("Parse error: {}", e)))
        }

        fn validate_complete(&self) -> crate::error::Result<()> {
            Ok(())
        }

        fn field_descriptions() -> Vec<crate::parsers::FieldDescription> {
            vec![
                crate::parsers::FieldDescription {
                    name: "text".to_string(),
                    field_type: "string".to_string(),
                    description: "Search text".to_string(),
                    required: true,
                },
                crate::parsers::FieldDescription {
                    name: "category".to_string(),
                    field_type: "string".to_string(),
                    description: "Category".to_string(),
                    required: false,
                },
            ]
        }
    }

    #[tokio::test]
    async fn test_tool_registration_with_schema() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();

        // Register tool with type parameter - handler receives WeatherQuery directly
        runtime
            .tool::<WeatherQuery, _>(
                |query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move { Ok(format!("Weather in {}: Sunny", query.location)) })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        // Check that tool is registered
        let tools = runtime.tools.read().await;
        assert!(tools.contains_key("get_weather"));
        drop(tools);

        // Check that schema info is captured
        let schemas = runtime.tool_schemas.read().await;
        let schema_info = schemas.get("get_weather").unwrap();
        assert_eq!(schema_info.name, "get_weather");
        assert_eq!(schema_info.fields.len(), 2);
        assert_eq!(schema_info.fields[0].name, "location");
        assert!(schema_info.fields[0].required);
        assert_eq!(schema_info.fields[1].name, "unit");
        assert!(!schema_info.fields[1].required);
    }

    #[tokio::test]
    async fn test_tool_schema_generation() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();

        // Register tool - handler receives WeatherQuery directly
        runtime
            .tool::<WeatherQuery, _>(
                |_query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move { Ok("Sunny".to_string()) })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        // Get tool schemas and verify structure
        let schemas = runtime.get_tool_schemas().await;
        assert_eq!(schemas.len(), 1);

        let weather_schema = schemas.get("get_weather").unwrap();
        assert!(
            weather_schema
                .fields
                .iter()
                .any(|f| f.name == "location" && f.required)
        );
        assert!(
            weather_schema
                .fields
                .iter()
                .any(|f| f.name == "unit" && !f.required)
        );
        assert_eq!(
            weather_schema.parameters.to_json_schema()["properties"]["location"]["type"],
            "string"
        );
    }

    #[tokio::test]
    async fn test_register_tool_with_schema_preserves_nested_structure() {
        let runtime = Runtime::new();

        runtime
            .register_tool_with_schema(
                "write_thread",
                "Write a thread",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "$ref": "#/$defs/ThreadItem"
                            }
                        }
                    },
                    "required": ["items"],
                    "$defs": {
                        "ThreadItem": {
                            "type": "object",
                            "properties": {
                                "text": { "type": "string" },
                                "image_ids": {
                                    "type": "array",
                                    "items": { "type": "integer" }
                                }
                            },
                            "required": ["text"]
                        }
                    }
                }),
                ToolFunction::Sync(Box::new(|args| Ok(args.to_string()))),
            )
            .await
            .unwrap();

        let schemas = runtime.get_tool_schemas().await;
        let write_thread = schemas.get("write_thread").unwrap();
        let json = write_thread.parameters.to_json_schema();
        assert_eq!(
            json["properties"]["items"]["items"]["properties"]["text"]["type"],
            "string"
        );
        assert_eq!(
            json["properties"]["items"]["items"]["properties"]["image_ids"]["items"]["type"],
            "integer"
        );
    }

    #[tokio::test]
    async fn test_typed_tool_treats_null_optional_fields_as_missing() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();
        runtime
            .tool::<SearchWithDefaultCategory, _>(
                |query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move { Ok(query.category) })
                },
                Some("search"),
            )
            .await
            .unwrap();

        let result = runtime
            .execute_tool_call(&crate::types::ToolCall::new(
                "search",
                serde_json::json!({
                    "text": "python tutorials",
                    "category": null
                }),
            ))
            .await
            .unwrap();

        assert_eq!(result, "general");
    }

    #[tokio::test]
    async fn test_tool_hydration_and_execution() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();

        // Register tool - handler receives WeatherQuery, not raw JSON
        runtime
            .tool::<WeatherQuery, _>(
                |query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move {
                        // Handler receives the deserialized WeatherQuery struct
                        let unit = query.unit.unwrap_or_else(|| "celsius".to_string());
                        Ok(format!(
                            "Weather in {} ({}): Sunny, 22°",
                            query.location, unit
                        ))
                    })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        // Simulate a tool call from LLM with JSON args
        let tool_call = crate::types::ToolCall::new(
            "get_weather",
            serde_json::json!({
                "location": "Paris",
                "unit": "fahrenheit"
            }),
        );

        // Execute the tool - it should deserialize JSON -> WeatherQuery -> call handler
        let result = runtime.execute_tool_call(&tool_call).await.unwrap();
        assert_eq!(result, "Weather in Paris (fahrenheit): Sunny, 22°");
    }

    #[tokio::test]
    async fn test_tool_hydration_with_defaults() {
        use futures::future::BoxFuture;

        let runtime = Runtime::new();

        // Register tool
        runtime
            .tool::<WeatherQuery, _>(
                |query| -> BoxFuture<'static, crate::error::Result<String>> {
                    Box::pin(async move {
                        let unit = query.unit.unwrap_or_else(|| "celsius".to_string());
                        Ok(format!("{} in {}", unit, query.location))
                    })
                },
                Some("get_weather"),
            )
            .await
            .unwrap();

        // Tool call with optional field missing
        let tool_call = crate::types::ToolCall::new(
            "get_weather",
            serde_json::json!({
                "location": "Tokyo"
                // unit is not provided, should use default
            }),
        );

        let result = runtime.execute_tool_call(&tool_call).await.unwrap();
        assert_eq!(result, "celsius in Tokyo");
    }
}
