//! Google Generative AI (Gemini) client implementation
//!
//! Implements the InferenceClient trait for Google's Gemini models.
//! Uses the REST API directly since there's no official Rust SDK.
//!
//! Supports:
//! - Native tool calling via function_declarations
//! - Thinking/reasoning mode
//! - Streaming via Server-Sent Events
//! - API key authentication
//! - Application Default Credentials (ADC) via `google-adc` feature
//! - File API for uploading large media files (>20MB)

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use reqwest::StatusCode;
use std::collections::HashMap;
use std::pin::Pin;
#[cfg(feature = "google-adc")]
use std::sync::Arc;
#[cfg(feature = "google-adc")]
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::providers::{
    GenerationConfig, GenerationResponse, InferenceClient, StreamChunk, TraceCallback,
};
use crate::retry::{RetryConfig, retry_with_backoff};
use crate::types::ChatRole;
use crate::types::{AssistantResponse, Provider, ResponsePart, TokenUsage, ToolCall};
use crate::utils::{
    ConversationMessage, JsonStreamAccumulator, media_part_to_google_format,
    validate_image_input_supported,
};

#[cfg(feature = "google-adc")]
use crate::utils::parse_json_value_strict_str;

fn google_debug_enabled() -> bool {
    std::env::var("CHEVALIER_DEBUG_GOOGLE")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

fn google_stream_debug_enabled() -> bool {
    std::env::var("CHEVALIER_DEBUG_GOOGLE_STREAM")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

fn sanitize_google_endpoint(endpoint: &str) -> String {
    endpoint
        .split_once("?key=")
        .map(|(base, _)| format!("{}?key=***", base))
        .unwrap_or_else(|| endpoint.to_string())
}

/// Authentication method for Google GenAI
#[derive(Clone)]
pub enum GoogleAuth {
    /// API key authentication (passed in URL)
    ApiKey(String),
    /// Application Default Credentials (ADC) - uses GOOGLE_APPLICATION_CREDENTIALS
    #[cfg(feature = "google-adc")]
    Adc(Arc<RwLock<Option<Arc<dyn gcp_auth::TokenProvider>>>>),
}

/// Status of an uploaded file
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FileState {
    /// File is being processed
    Processing,
    /// File is ready to use
    Active,
    /// Processing failed
    Failed,
}

/// Response from uploading a file to Google's File API
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadedFile {
    /// The file resource name (e.g., "files/abc123")
    pub name: String,
    /// Display name of the file
    #[serde(default)]
    pub display_name: String,
    /// MIME type of the file
    pub mime_type: String,
    /// Size of the file in bytes
    #[serde(default)]
    pub size_bytes: String,
    /// The URI to use in generateContent requests
    pub uri: String,
    /// Current state of the file
    pub state: FileState,
    /// Error details if state is FAILED
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct PartialGoogleToolCall {
    id: String,
    name: String,
    args: serde_json::Value,
}

#[derive(Debug, Default)]
struct GoogleToolCallAccumulator {
    current_tool_calls: HashMap<(usize, usize), PartialGoogleToolCall>,
}

impl GoogleToolCallAccumulator {
    fn update_partial(
        &mut self,
        candidate_index: usize,
        part_index: usize,
        function_call: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let key = (candidate_index, part_index);
        let name = function_call
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let id = function_call
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let entry = self.current_tool_calls.entry(key).or_insert_with(|| {
            let effective_name = if name.is_empty() {
                "tool".to_string()
            } else {
                name.clone()
            };
            PartialGoogleToolCall {
                id: id.unwrap_or_else(|| {
                    format!(
                        "google_{}_stream_{}_{}",
                        effective_name, candidate_index, part_index
                    )
                }),
                name: effective_name,
                args: serde_json::json!({}),
            }
        });

        if let Some(id) = function_call.get("id").and_then(|v| v.as_str()) {
            entry.id = id.to_string();
        }
        if !name.is_empty() {
            entry.name = name;
        }

        if let Some(partial_args) = function_call.get("partialArgs").and_then(|v| v.as_array()) {
            for fragment in partial_args {
                apply_google_partial_arg(&mut entry.args, fragment);
            }
        }

        Some(openai_style_partial_tool_call(
            &entry.id,
            &entry.name,
            &entry.args,
        ))
    }

    fn complete(
        &mut self,
        candidate_index: usize,
        part_index: usize,
        function_call: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let key = (candidate_index, part_index);
        let name = function_call
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let id = function_call
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let args = function_call
            .get("args")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let mut entry = self.current_tool_calls.remove(&key).unwrap_or_else(|| {
            let effective_name = if name.is_empty() {
                "tool".to_string()
            } else {
                name.clone()
            };
            PartialGoogleToolCall {
                id: id.unwrap_or_else(|| {
                    format!(
                        "google_{}_stream_{}_{}",
                        effective_name, candidate_index, part_index
                    )
                }),
                name: effective_name,
                args: serde_json::json!({}),
            }
        });

        if let Some(id) = function_call.get("id").and_then(|v| v.as_str()) {
            entry.id = id.to_string();
        }
        if !name.is_empty() {
            entry.name = name;
        }
        entry.args = args;

        Some(openai_style_complete_tool_call(
            &entry.id,
            &entry.name,
            &entry.args,
        ))
    }
}

fn openai_style_partial_tool_call(
    id: &str,
    name: &str,
    args: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "function": {
            "name": name,
            "arguments": serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string())
        }
    })
}

fn openai_style_complete_tool_call(
    id: &str,
    name: &str,
    args: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "_tool_name": name,
        "_tool_use_id": id,
        "name": name,
        "input": args,
        "type": "function",
        "raw_arguments": serde_json::to_string(args).unwrap_or_else(|_| "{}".to_string()),
        "function": {
            "name": name,
            "arguments": args
        }
    })
}

fn google_partial_fragment_value(fragment: &serde_json::Value) -> Option<serde_json::Value> {
    for key in [
        "stringValue",
        "numberValue",
        "intValue",
        "boolValue",
        "objectValue",
        "listValue",
    ] {
        if let Some(value) = fragment.get(key) {
            return Some(value.clone());
        }
    }

    if fragment
        .get("nullValue")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Some(serde_json::Value::Null);
    }

    None
}

#[derive(Debug)]
enum JsonPathSegment {
    Key(String),
    Index(usize),
}

fn parse_google_json_path(path: &str) -> Option<Vec<JsonPathSegment>> {
    if !path.starts_with('$') {
        return None;
    }

    let mut chars = path[1..].chars().peekable();
    let mut segments = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        match ch {
            '.' => {
                chars.next();
                let mut key = String::new();
                while let Some(next) = chars.peek().copied() {
                    if matches!(next, '.' | '[') {
                        break;
                    }
                    key.push(next);
                    chars.next();
                }
                if key.is_empty() {
                    return None;
                }
                segments.push(JsonPathSegment::Key(key));
            }
            '[' => {
                chars.next();
                let mut index = String::new();
                while let Some(next) = chars.peek().copied() {
                    if next == ']' {
                        break;
                    }
                    index.push(next);
                    chars.next();
                }
                if chars.next() != Some(']') {
                    return None;
                }
                segments.push(JsonPathSegment::Index(index.parse().ok()?));
            }
            _ => return None,
        }
    }

    Some(segments)
}

fn set_value_at_path(
    root: &mut serde_json::Value,
    path: &[JsonPathSegment],
    value: serde_json::Value,
) {
    if path.is_empty() {
        *root = value;
        return;
    }

    match &path[0] {
        JsonPathSegment::Key(key) => {
            if !root.is_object() {
                *root = serde_json::json!({});
            }
            let map = root.as_object_mut().expect("object just created");
            let entry = map
                .entry(key.clone())
                .or_insert_with(|| serde_json::Value::Null);
            set_value_at_path(entry, &path[1..], value);
        }
        JsonPathSegment::Index(index) => {
            if !root.is_array() {
                *root = serde_json::json!([]);
            }
            let array = root.as_array_mut().expect("array just created");
            while array.len() <= *index {
                array.push(serde_json::Value::Null);
            }
            set_value_at_path(&mut array[*index], &path[1..], value);
        }
    }
}

fn apply_google_partial_arg(target: &mut serde_json::Value, fragment: &serde_json::Value) {
    let Some(path) = fragment.get("jsonPath").and_then(|v| v.as_str()) else {
        return;
    };
    let Some(value) = google_partial_fragment_value(fragment) else {
        return;
    };
    let Some(segments) = parse_google_json_path(path) else {
        return;
    };
    if target.is_null() {
        *target = serde_json::json!({});
    }
    set_value_at_path(target, &segments, value);
}

fn parse_google_stream_value(
    json: &serde_json::Value,
    accumulator: &mut GoogleToolCallAccumulator,
) -> Vec<Result<StreamChunk>> {
    let mut chunks = Vec::new();

    if let Some(candidates) = json.get("candidates").and_then(|c| c.as_array()) {
        for (candidate_index, candidate) in candidates.iter().enumerate() {
            if let Some(parts) = candidate["content"]["parts"].as_array() {
                for (part_index, part) in parts.iter().enumerate() {
                    if let Some(func_call) = part.get("functionCall") {
                        if func_call.get("args").is_some() {
                            if let Some(tool_call) =
                                accumulator.complete(candidate_index, part_index, func_call)
                            {
                                chunks.push(Ok(StreamChunk::ToolCallComplete(tool_call)));
                            }
                        } else if (func_call.get("name").is_some()
                            || func_call.get("partialArgs").is_some()
                            || func_call
                                .get("willContinue")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false))
                            && let Some(partial) =
                                accumulator.update_partial(candidate_index, part_index, func_call)
                        {
                            chunks.push(Ok(StreamChunk::ToolCallPartial(partial)));
                        }
                    } else if part
                        .get("thought")
                        .and_then(|t| t.as_bool())
                        .unwrap_or(false)
                    {
                        if let Some(text) = part["text"].as_str() {
                            chunks.push(Ok(StreamChunk::Reasoning(text.to_string())));
                        }
                    } else if let Some(sig) = part.get("thoughtSignature").and_then(|s| s.as_str())
                    {
                        chunks.push(Ok(StreamChunk::Signature(sig.to_string())));
                    } else if let Some(text) = part["text"].as_str() {
                        chunks.push(Ok(StreamChunk::Content(text.to_string())));
                    }
                }
            }
        }
    }

    if let Some(usage) = json.get("usageMetadata") {
        let input = usage["promptTokenCount"].as_u64().unwrap_or(0);
        let output = usage["candidatesTokenCount"].as_u64().unwrap_or(0);
        let cached = usage["cachedContentTokenCount"].as_u64().unwrap_or(0);

        chunks.push(Ok(StreamChunk::Usage {
            input_tokens: input,
            output_tokens: output,
            cached_tokens: cached,
            cache_write_input_tokens: 0,
            reasoning_tokens: usage.get("thoughtsTokenCount").and_then(|v| v.as_u64()),
            provider_cost_dollars: None,
        }));
    }

    chunks
}

/// Google Generative AI (Gemini) client
#[derive(Clone)]
pub struct GoogleGenAIClient {
    model: String,
    auth: GoogleAuth,
    api_url: String,
    thinking_budget: Option<u32>,
    trace_callback: Option<TraceCallback>,
}

impl GoogleGenAIClient {
    /// Create a new Google GenAI client with API key authentication
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            auth: GoogleAuth::ApiKey(api_key.into()),
            api_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
            thinking_budget: None,
            trace_callback: None,
        }
    }

    /// Create a new Google GenAI client with Application Default Credentials (ADC)
    /// using the Vertex AI endpoint.
    ///
    /// This uses the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or
    /// credentials from `gcloud auth application-default login`.
    ///
    /// The project ID is automatically extracted from the service account JSON file.
    /// Location defaults to "us-central1" if not specified via `GOOGLE_CLOUD_LOCATION`.
    ///
    /// Note: ADC/service accounts require using the Vertex AI endpoint, not the
    /// standard Generative Language API (which uses API keys).
    ///
    /// Requires the `google-adc` feature to be enabled.
    ///
    /// # Arguments
    /// * `model` - The model name (e.g., "gemini-2.0-flash-thinking-exp")
    ///
    /// # Panics
    /// Panics if `GOOGLE_APPLICATION_CREDENTIALS` is not set or if the credentials
    /// file cannot be read or doesn't contain a `project_id`.
    #[cfg(feature = "google-adc")]
    pub fn from_adc(model: impl Into<String>) -> Self {
        // Read project_id from the service account JSON file
        let creds_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .expect("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set");

        let creds_content = std::fs::read_to_string(&creds_path)
            .unwrap_or_else(|_| panic!("Failed to read credentials file: {}", creds_path));

        let creds_json: serde_json::Value = parse_json_value_strict_str(&creds_content)
            .expect("Failed to parse credentials file as JSON");

        let project = creds_json["project_id"]
            .as_str()
            .expect("Credentials file must contain project_id")
            .to_string();

        // Get location from env var or default to us-central1
        let loc =
            std::env::var("GOOGLE_CLOUD_LOCATION").unwrap_or_else(|_| "us-central1".to_string());

        let model_str = model.into();
        Self {
            model: model_str.clone(),
            auth: GoogleAuth::Adc(Arc::new(RwLock::new(None))),
            // Vertex AI endpoint format
            api_url: format!(
                "https://{}-aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/google/models/{}",
                loc, project, loc, model_str
            ),
            thinking_budget: None,
            trace_callback: None,
        }
    }

    /// Create a new Google GenAI client with ADC and explicit project/location.
    ///
    /// Use this if you want to override the project_id from the credentials file
    /// or specify a different location.
    #[cfg(feature = "google-adc")]
    pub fn from_adc_with_config(
        model: impl Into<String>,
        project_id: impl Into<String>,
        location: impl Into<String>,
    ) -> Self {
        let project = project_id.into();
        let loc = location.into();
        let model_str = model.into();
        Self {
            model: model_str.clone(),
            auth: GoogleAuth::Adc(Arc::new(RwLock::new(None))),
            // Vertex AI endpoint format
            api_url: format!(
                "https://{}-aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/google/models/{}",
                loc, project, loc, model_str
            ),
            thinking_budget: None,
            trace_callback: None,
        }
    }

    /// Set the thinking budget for reasoning mode
    pub fn with_thinking_budget(mut self, budget: u32) -> Self {
        self.thinking_budget = Some(budget);
        self
    }

    /// Set a custom API URL
    pub fn with_api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = url.into();
        self
    }

    // ==================== File API Methods ====================

    /// Upload a file to Google's File API for use in generateContent requests.
    ///
    /// Use this for files larger than 20MB or when you want to reuse the same
    /// file across multiple requests.
    ///
    /// # Arguments
    /// * `data` - The file bytes to upload
    /// * `mime_type` - The MIME type of the file (e.g., "video/mp4", "image/png")
    /// * `display_name` - Optional display name for the file
    ///
    /// # Returns
    /// An `UploadedFile` containing the `uri` to use in requests. Note that video
    /// files may need processing time - check the `state` field and use
    /// `wait_for_file_processing` if needed.
    ///
    /// # Example
    /// ```ignore
    /// let video_bytes = std::fs::read("video.mp4")?;
    /// let uploaded = client.upload_file(&video_bytes, "video/mp4", Some("my-video")).await?;
    /// // For videos, wait for processing
    /// let ready = client.wait_for_file_processing(&uploaded.name, None).await?;
    /// // Use ready.uri in your generateContent request
    /// ```
    pub async fn upload_file(
        &self,
        data: &[u8],
        mime_type: &str,
        display_name: Option<&str>,
    ) -> Result<UploadedFile> {
        let client = reqwest::Client::new();
        let file_api_base = "https://generativelanguage.googleapis.com";

        // Step 1: Start resumable upload - get upload URL from response headers
        let start_url = match &self.auth {
            GoogleAuth::ApiKey(key) => {
                format!("{}/upload/v1beta/files?key={}", file_api_base, key)
            }
            #[cfg(feature = "google-adc")]
            GoogleAuth::Adc(_) => {
                format!("{}/upload/v1beta/files", file_api_base)
            }
        };

        let metadata = serde_json::json!({
            "file": {
                "display_name": display_name.unwrap_or("uploaded_file")
            }
        });

        #[allow(unused_mut)] // mut needed only with google-adc feature
        let mut start_request = client
            .post(&start_url)
            .header("X-Goog-Upload-Protocol", "resumable")
            .header("X-Goog-Upload-Command", "start")
            .header(
                "X-Goog-Upload-Header-Content-Length",
                data.len().to_string(),
            )
            .header("X-Goog-Upload-Header-Content-Type", mime_type)
            .header("Content-Type", "application/json");

        // Add authorization header for ADC
        #[cfg(feature = "google-adc")]
        if let GoogleAuth::Adc(_) = &self.auth {
            let token = self.get_adc_token().await?;
            start_request = start_request.header("Authorization", format!("Bearer {}", token));
        }

        let start_response = start_request
            .json(&metadata)
            .send()
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to start file upload: {}", e)))?;

        if !start_response.status().is_success() {
            let error_body = start_response.text().await.unwrap_or_default();
            return Err(Error::NonRetryable(format!(
                "Failed to start file upload: {}",
                error_body
            )));
        }

        // Extract upload URL from response headers
        let upload_url = start_response
            .headers()
            .get("x-goog-upload-url")
            .ok_or_else(|| {
                Error::NonRetryable("Missing x-goog-upload-url header in response".to_string())
            })?
            .to_str()
            .map_err(|e| Error::NonRetryable(format!("Invalid upload URL header: {}", e)))?
            .to_string();

        // Step 2: Upload file bytes to the upload URL
        let upload_response = client
            .post(&upload_url)
            .header("Content-Length", data.len().to_string())
            .header("X-Goog-Upload-Offset", "0")
            .header("X-Goog-Upload-Command", "upload, finalize")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to upload file data: {}", e)))?;

        if !upload_response.status().is_success() {
            let error_body = upload_response.text().await.unwrap_or_default();
            return Err(Error::NonRetryable(format!(
                "Failed to upload file data: {}",
                error_body
            )));
        }

        // Parse the response to get the file info
        let response_json: serde_json::Value = upload_response
            .json()
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to parse upload response: {}", e)))?;

        // Extract the file object from the response
        let file_json = response_json
            .get("file")
            .ok_or_else(|| {
                Error::NonRetryable(format!(
                    "Missing 'file' in upload response: {:?}",
                    response_json
                ))
            })?
            .clone();

        let uploaded_file: UploadedFile = serde_json::from_value(file_json)
            .map_err(|e| Error::NonRetryable(format!("Failed to parse UploadedFile: {}", e)))?;

        Ok(uploaded_file)
    }

    /// Get the current status of an uploaded file.
    ///
    /// # Arguments
    /// * `file_name` - The file resource name (e.g., "files/abc123")
    pub async fn get_file(&self, file_name: &str) -> Result<UploadedFile> {
        let client = reqwest::Client::new();
        let file_api_base = "https://generativelanguage.googleapis.com";

        let url = match &self.auth {
            GoogleAuth::ApiKey(key) => {
                format!("{}/v1beta/{}?key={}", file_api_base, file_name, key)
            }
            #[cfg(feature = "google-adc")]
            GoogleAuth::Adc(_) => {
                format!("{}/v1beta/{}", file_api_base, file_name)
            }
        };

        #[allow(unused_mut)] // mut needed only with google-adc feature
        let mut request = client.get(&url);

        #[cfg(feature = "google-adc")]
        if let GoogleAuth::Adc(_) = &self.auth {
            let token = self.get_adc_token().await?;
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to get file status: {}", e)))?;

        if !response.status().is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(Error::NonRetryable(format!(
                "Failed to get file status: {}",
                error_body
            )));
        }

        let file: UploadedFile = response.json().await.map_err(|e| {
            Error::NonRetryable(format!("Failed to parse file status response: {}", e))
        })?;

        Ok(file)
    }

    /// Wait for a file to finish processing and become ACTIVE.
    ///
    /// Video files require server-side processing before they can be used.
    /// This method polls the file status until it becomes ACTIVE or FAILED.
    ///
    /// # Arguments
    /// * `file_name` - The file resource name (e.g., "files/abc123")
    /// * `timeout_secs` - Maximum time to wait (defaults to 300 seconds / 5 minutes)
    ///
    /// # Returns
    /// The file when it reaches ACTIVE state, or an error if it fails or times out.
    pub async fn wait_for_file_processing(
        &self,
        file_name: &str,
        timeout_secs: Option<u64>,
    ) -> Result<UploadedFile> {
        let timeout = timeout_secs.unwrap_or(300);
        let start = std::time::Instant::now();
        let poll_interval = std::time::Duration::from_secs(2);

        loop {
            let file = self.get_file(file_name).await?;

            match file.state {
                FileState::Active => return Ok(file),
                FileState::Failed => {
                    let error_msg = file
                        .error
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "Unknown error".to_string());
                    return Err(Error::NonRetryable(format!(
                        "File processing failed: {}",
                        error_msg
                    )));
                }
                FileState::Processing => {
                    if start.elapsed().as_secs() > timeout {
                        return Err(Error::NonRetryable(format!(
                            "Timeout waiting for file {} to process",
                            file_name
                        )));
                    }
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }
    }

    /// Delete an uploaded file.
    ///
    /// # Arguments
    /// * `file_name` - The file resource name (e.g., "files/abc123")
    pub async fn delete_file(&self, file_name: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let file_api_base = "https://generativelanguage.googleapis.com";

        let url = match &self.auth {
            GoogleAuth::ApiKey(key) => {
                format!("{}/v1beta/{}?key={}", file_api_base, file_name, key)
            }
            #[cfg(feature = "google-adc")]
            GoogleAuth::Adc(_) => {
                format!("{}/v1beta/{}", file_api_base, file_name)
            }
        };

        #[allow(unused_mut)] // mut needed only with google-adc feature
        let mut request = client.delete(&url);

        #[cfg(feature = "google-adc")]
        if let GoogleAuth::Adc(_) = &self.auth {
            let token = self.get_adc_token().await?;
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::NonRetryable(format!("Failed to delete file: {}", e)))?;

        if !response.status().is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(Error::NonRetryable(format!(
                "Failed to delete file: {}",
                error_body
            )));
        }

        Ok(())
    }

    // ==================== End File API Methods ====================

    /// Get a valid access token for ADC authentication
    #[cfg(feature = "google-adc")]
    async fn get_adc_token(&self) -> Result<String> {
        if let GoogleAuth::Adc(token_provider) = &self.auth {
            let mut provider = token_provider.write().await;

            // Initialize if needed
            if provider.is_none() {
                let tp = gcp_auth::provider()
                    .await
                    .map_err(|e| Error::NonRetryable(format!("Failed to initialize ADC: {}", e)))?;
                *provider = Some(tp);
            }

            // Get token with cloud-platform scope (required for Vertex AI/Generative AI)
            let tp = provider.as_ref().unwrap();
            let scopes = &["https://www.googleapis.com/auth/cloud-platform"];
            let token = tp
                .token(scopes)
                .await
                .map_err(|e| Error::NonRetryable(format!("Failed to get ADC token: {}", e)))?;

            Ok(token.as_str().to_string())
        } else {
            Err(Error::NonRetryable(
                "Not using ADC authentication".to_string(),
            ))
        }
    }

    /// Build the request body for Google GenAI API
    fn build_request_body(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
        stream: bool,
    ) -> Result<serde_json::Value> {
        let model = config.effective_model(&self.model);
        validate_image_input_supported(messages, Provider::GoogleGenAI, model)?;

        // Extract system instruction if present
        let (system_instruction, messages) = self.extract_system_message(messages)?;

        // Convert messages to Google format
        let contents = self.convert_messages_to_contents(messages)?;

        let mut request = serde_json::json!({
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": config.max_tokens.unwrap_or(4096),
            }
        });

        // Add temperature and topP if not in thinking mode
        if self.thinking_budget.is_none() {
            if let Some(temp) = config.temperature {
                request["generationConfig"]["temperature"] = serde_json::json!(temp);
            }
            if let Some(top_p) = config.top_p {
                request["generationConfig"]["topP"] = serde_json::json!(top_p);
            }
        }

        // Add system instruction if present
        if let Some(system) = system_instruction {
            request["systemInstruction"] = serde_json::json!({
                "parts": [{"text": system}]
            });
        }

        // Add tools if provided (Google format)
        if let Some(ref tools) = config.tools
            && !tools.is_empty()
        {
            // Check if tools are already in Google format or need conversion
            let google_tools = self.convert_tools_to_google_format(tools)?;
            request["tools"] = google_tools;
            if stream {
                request["toolConfig"]["functionCallingConfig"]["streamFunctionCallArguments"] =
                    serde_json::json!(true);
            }
        }

        // Add thinking config if enabled
        if let Some(budget) = self.thinking_budget {
            request["generationConfig"]["thinkingConfig"] = serde_json::json!({
                "includeThoughts": true,
                "thinkingBudget": budget
            });
        }

        // Add structured output schema if provided
        if let Some(ref schema) = config.output_schema {
            request["generationConfig"]["responseMimeType"] = serde_json::json!("application/json");
            request["generationConfig"]["responseSchema"] = schema.clone();
        }

        Ok(request)
    }

    /// Extract system message and return (system, remaining_messages)
    fn extract_system_message<'a>(
        &self,
        messages: &'a [ConversationMessage],
    ) -> Result<(Option<String>, &'a [ConversationMessage])> {
        if let Some(ConversationMessage::Chat(first)) = messages.first()
            && first.role == ChatRole::System
        {
            return Ok((Some(first.content.clone()), &messages[1..]));
        }
        Ok((None, messages))
    }

    /// Convert messages to Google's contents format
    fn convert_messages_to_contents(
        &self,
        messages: &[ConversationMessage],
    ) -> Result<Vec<serde_json::Value>> {
        let mut contents = Vec::new();

        for msg in messages {
            match msg {
                ConversationMessage::Chat(chat_msg) => {
                    let role = match chat_msg.role {
                        ChatRole::User => "user",
                        ChatRole::Assistant => "model",
                        ChatRole::System => continue, // Skip system messages (handled separately)
                        ChatRole::Tool => "user",     // Tool results come from user role
                    };

                    contents.push(serde_json::json!({
                        "role": role,
                        "parts": [{"text": chat_msg.content}]
                    }));
                }
                ConversationMessage::AssistantResponse(response) => {
                    let mut parts = Vec::new();
                    let mut idx = 0;
                    while idx < response.output.len() {
                        match &response.output[idx] {
                            ResponsePart::Text { text } => parts.push(serde_json::json!({
                                "text": text
                            })),
                            ResponsePart::Reasoning { text } => {
                                let mut part = serde_json::json!({
                                    "thought": true,
                                    "text": text
                                });
                                if let Some(ResponsePart::Signature { value }) =
                                    response.output.get(idx + 1)
                                {
                                    part["thoughtSignature"] = serde_json::json!(value);
                                    idx += 1;
                                }
                                parts.push(part);
                            }
                            ResponsePart::Tool { call } => {
                                let mut part = serde_json::json!({
                                    "functionCall": {
                                        "name": call.tool_name,
                                        "args": call.args
                                    }
                                });
                                if let Some(ResponsePart::Signature { value }) =
                                    response.output.get(idx + 1)
                                {
                                    part["thoughtSignature"] = serde_json::json!(value);
                                    idx += 1;
                                }
                                parts.push(part);
                            }
                            ResponsePart::Signature { .. } => {}
                        }
                        idx += 1;
                    }

                    contents.push(serde_json::json!({
                        "role": "model",
                        "parts": parts
                    }));
                }
                ConversationMessage::ToolCall(tool_call) => {
                    // Google uses functionCall for assistant tool calls
                    let mut part = serde_json::json!({
                        "functionCall": {
                            "name": tool_call.tool_name,
                            "args": tool_call.args
                        }
                    });
                    // Include thoughtSignature if available (required by Google for multi-turn)
                    if let Some(ref signature) = tool_call.signature {
                        part["thoughtSignature"] = serde_json::json!(signature);
                    } else if let Some(ref obj) = tool_call.tool_obj
                        && let Some(sig) = obj.get("thoughtSignature")
                    {
                        part["thoughtSignature"] = sig.clone();
                    }
                    contents.push(serde_json::json!({
                        "role": "model",
                        "parts": [part]
                    }));
                }
                ConversationMessage::ToolResult(tool_result) => {
                    // Google uses functionResponse for tool results
                    // Get tool_name from: 1) tool_name field, 2) tool_obj["_tool_name"], 3) empty string
                    let tool_name = tool_result.tool_name.clone().unwrap_or_else(|| {
                        tool_result
                            .tool_obj
                            .as_ref()
                            .and_then(|obj| obj.get("_tool_name"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    });

                    contents.push(serde_json::json!({
                        "role": "user",
                        "parts": [{
                            "functionResponse": {
                                "name": tool_name,
                                "response": {
                                    "result": tool_result.content
                                }
                            }
                        }]
                    }));
                }
                ConversationMessage::Reasoning(segment) => {
                    // Google uses thought: true for reasoning
                    contents.push(serde_json::json!({
                        "role": "model",
                        "parts": [{
                            "thought": true,
                            "text": segment.content
                        }]
                    }));
                }
                ConversationMessage::Multimodal(multimodal_msg) => {
                    // Convert multimodal message with media parts (images, video, audio)
                    let role = match multimodal_msg.role {
                        ChatRole::User => "user",
                        ChatRole::Assistant => "model",
                        ChatRole::System => continue, // Skip system messages
                        ChatRole::Tool => "user",
                    };

                    let parts: Vec<serde_json::Value> = multimodal_msg
                        .parts
                        .iter()
                        .map(|part| media_part_to_google_format(part, None))
                        .collect();

                    contents.push(serde_json::json!({
                        "role": role,
                        "parts": parts
                    }));
                }
            }
        }

        Ok(contents)
    }

    /// Convert tools from Anthropic/OpenAI format to Google format
    fn convert_tools_to_google_format(
        &self,
        tools: &[serde_json::Value],
    ) -> Result<serde_json::Value> {
        // Check if already in Google format
        if tools
            .iter()
            .any(|t| t.get("function_declarations").is_some())
        {
            return Ok(serde_json::json!(tools));
        }

        // Convert from Anthropic format
        let function_declarations: Vec<serde_json::Value> = tools
            .iter()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.get("name").cloned().unwrap_or(serde_json::json!("")),
                    "description": tool.get("description").cloned().unwrap_or(serde_json::json!("")),
                    "parameters": tool.get("input_schema").cloned().unwrap_or_else(|| {
                        tool.get("parameters").cloned().unwrap_or(serde_json::json!({}))
                    })
                })
            })
            .collect();

        Ok(serde_json::json!([{
            "function_declarations": function_declarations
        }]))
    }

    /// Extract canonical assistant response from Gemini candidates.
    fn extract_response(&self, candidates: &serde_json::Value) -> Result<AssistantResponse> {
        let mut response = AssistantResponse::default();
        if let Some(first) = candidates.as_array().and_then(|a| a.first())
            && let Some(parts) = first["content"]["parts"].as_array()
        {
            for part in parts {
                if let Some(func_call) = part.get("functionCall") {
                    let mut call = ToolCall::from_provider_format(
                        serde_json::json!({ "functionCall": func_call }),
                        Provider::GoogleGenAI,
                    )?;
                    if let Some(signature) = part.get("thoughtSignature").and_then(|v| v.as_str()) {
                        call.signature = Some(signature.to_string());
                    }
                    response.push_output(ResponsePart::Tool { call });
                    if let Some(signature) = part.get("thoughtSignature").and_then(|v| v.as_str()) {
                        response.push_output(ResponsePart::Signature {
                            value: signature.to_string(),
                        });
                    }
                } else if part
                    .get("thought")
                    .and_then(|t| t.as_bool())
                    .unwrap_or(false)
                {
                    if let Some(text) = part["text"].as_str()
                        && !text.is_empty()
                    {
                        response.push_output(ResponsePart::Reasoning {
                            text: text.to_string(),
                        });
                    }
                    if let Some(signature) = part.get("thoughtSignature").and_then(|v| v.as_str()) {
                        response.push_output(ResponsePart::Signature {
                            value: signature.to_string(),
                        });
                    }
                } else if let Some(text) = part["text"].as_str()
                    && !text.is_empty()
                {
                    response.push_output(ResponsePart::Text {
                        text: text.to_string(),
                    });
                }
            }
        }
        Ok(response)
    }

    /// Parse token usage from response
    fn parse_usage(&self, usage_metadata: &serde_json::Value) -> TokenUsage {
        TokenUsage {
            input_tokens: usage_metadata["promptTokenCount"].as_u64().unwrap_or(0),
            output_tokens: usage_metadata["candidatesTokenCount"].as_u64().unwrap_or(0),
            cached_tokens: usage_metadata["cachedContentTokenCount"]
                .as_u64()
                .unwrap_or(0),
            cache_write_input_tokens: 0,
            reasoning_tokens: usage_metadata["thoughtsTokenCount"].as_u64(),
            provider_cost_dollars: None,
        }
    }

    /// Check if using Vertex AI endpoint
    #[allow(dead_code)]
    fn is_vertex_ai(&self) -> bool {
        self.api_url.contains("aiplatform.googleapis.com")
    }

    /// Get the API endpoint for the model
    fn get_endpoint(&self, stream: bool) -> String {
        let action = if stream {
            "streamGenerateContent"
        } else {
            "generateContent"
        };

        match &self.auth {
            GoogleAuth::ApiKey(key) => {
                // Standard Gemini API: {base}/models/{model}:{action}?key={key}
                format!(
                    "{}/models/{}:{}?key={}",
                    self.api_url, self.model, action, key
                )
            }
            #[cfg(feature = "google-adc")]
            GoogleAuth::Adc(_) => {
                // Vertex AI: {base}:{action} (model is already in the URL)
                format!("{}:{}", self.api_url, action)
            }
        }
    }

    /// Make HTTP request to Google API
    async fn make_request(
        &self,
        body: serde_json::Value,
        stream: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<reqwest::Response> {
        let client = reqwest::Client::new();
        if google_debug_enabled() {
            let endpoint = sanitize_google_endpoint(&self.get_endpoint(stream));
            eprintln!(
                "google request: model={} stream={} endpoint={} body={}",
                self.model, stream, endpoint, body
            );
        }
        #[allow(unused_mut)] // mut needed only with google-adc feature
        let mut request = client
            .post(self.get_endpoint(stream))
            .timeout(timeout.unwrap_or(std::time::Duration::from_secs(300)))
            .header("Content-Type", "application/json");

        // Add authorization header for ADC
        #[cfg(feature = "google-adc")]
        if let GoogleAuth::Adc(_) = &self.auth {
            let token = self.get_adc_token().await?;
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.json(&body).send().await?;

        Ok(response)
    }

    /// Handle error responses - categorize as retryable or non-retryable
    fn handle_error_response(&self, status: StatusCode, body: String) -> Error {
        match status {
            // Client errors (4xx) are generally not retryable
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::NonRetryable(format!("{}: {}", status, body))
            }
            // Rate limit - retryable
            StatusCode::TOO_MANY_REQUESTS => Error::Inference(format!("Rate limited: {}", body)),
            // Server errors (5xx) are retryable
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Error::Inference(format!("{}: {}", status, body)),
            // Default: assume retryable for unknown errors
            _ => Error::Inference(format!("{}: {}", status, body)),
        }
    }

    /// Make request with retry and exponential backoff
    async fn make_request_with_retry(
        &self,
        body: serde_json::Value,
        timeout: Option<std::time::Duration>,
        retry_config: Option<RetryConfig>,
    ) -> Result<serde_json::Value> {
        let config = retry_config.unwrap_or_default();

        retry_with_backoff(config, || async {
            let response = self.make_request(body.clone(), false, timeout).await?;
            let status = response.status();

            if !status.is_success() {
                let error_body = response.text().await.unwrap_or_default();
                if google_debug_enabled() {
                    eprintln!(
                        "google response error: model={} status={} body={}",
                        self.model, status, error_body
                    );
                }
                return Err(self.handle_error_response(status, error_body));
            }

            response.json().await.map_err(Error::from)
        })
        .await
    }
}

#[async_trait]
impl InferenceClient for GoogleGenAIClient {
    async fn get_generation(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<GenerationResponse> {
        let request_body = self.build_request_body(messages, config, false)?;
        let body = self
            .make_request_with_retry(request_body, config.timeout, config.retry_config.clone())
            .await?;

        // Parse response
        let candidates = &body["candidates"];
        let response = self.extract_response(candidates)?;

        // Parse usage
        let usage = self.parse_usage(&body["usageMetadata"]);

        // If tools were provided, return full response
        let has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();
        let has_tool_calls = response.has_tool_calls();

        Ok(GenerationResponse::from_assistant_response(
            response,
            usage,
            None,
            if has_tools || has_tool_calls {
                Some(body)
            } else {
                None
            },
        ))
    }

    async fn connect_and_listen(
        &self,
        messages: &[ConversationMessage],
        config: &GenerationConfig,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk>> + Send>>> {
        let request_body = self.build_request_body(messages, config, true)?;
        let timeout = config.timeout;

        // Retry the connection establishment with backoff
        let retry_config = config.retry_config.clone().unwrap_or_default();
        let response = retry_with_backoff(retry_config, || async {
            let resp = self
                .make_request(request_body.clone(), true, timeout)
                .await?;
            let status = resp.status();

            if !status.is_success() {
                let error_body = resp.text().await.unwrap_or_default();
                if google_debug_enabled() {
                    eprintln!(
                        "google streaming response error: model={} status={} body={}",
                        self.model, status, error_body
                    );
                }
                return Err(self.handle_error_response(status, error_body));
            }

            Ok(resp)
        })
        .await?;

        let _has_tools = config.tools.is_some() && !config.tools.as_ref().unwrap().is_empty();

        // Google streams a JSON array; use serde_json's streaming deserializer.
        let stream = response.bytes_stream();

        Ok(Box::pin(
            stream
                .scan(
                    (
                        JsonStreamAccumulator::new(),
                        GoogleToolCallAccumulator::default(),
                    ),
                    move |state, chunk_result| {
                        let mut chunks = Vec::new();
                        let (parser, tool_accumulator) = state;

                        match chunk_result {
                            Ok(bytes) => {
                                if google_stream_debug_enabled() {
                                    let text = String::from_utf8_lossy(&bytes);
                                    eprintln!("google stream chunk: {}", text);
                                }
                                match parser.push_bytes(&bytes) {
                                    Ok(values) => {
                                        for json in values {
                                            chunks.extend(parse_google_stream_value(
                                                &json,
                                                tool_accumulator,
                                            ));
                                        }
                                    }
                                    Err(err) => {
                                        if google_stream_debug_enabled() {
                                            eprintln!("google stream parse error: {}", err);
                                        }
                                        chunks.push(Err(err));
                                    }
                                }
                            }
                            Err(e) => {
                                chunks.push(Err(Error::Inference(format!(
                                    "Google stream error: {}",
                                    e
                                ))));
                            }
                        }

                        futures::future::ready(Some(chunks))
                    },
                )
                .flat_map(futures::stream::iter),
        ))
    }

    fn provider(&self) -> Provider {
        Provider::GoogleGenAI
    }

    fn set_trace_callback(&mut self, callback: TraceCallback) {
        self.trace_callback = Some(callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChatMessage;

    #[test]
    fn test_client_creation() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        assert_eq!(client.model, "gemini-1.5-pro");
        assert!(matches!(client.auth, GoogleAuth::ApiKey(ref k) if k == "test-key"));
        assert!(client.api_url.contains("generativelanguage.googleapis.com"));
    }

    #[test]
    fn test_with_thinking_budget() {
        let client =
            GoogleGenAIClient::new("test-key", "gemini-1.5-pro").with_thinking_budget(1024);
        assert_eq!(client.thinking_budget, Some(1024));
    }

    #[test]
    fn test_extract_system_message() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        assert_eq!(system, Some("You are helpful".to_string()));
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_extract_system_message_none() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];

        let (system, remaining) = client.extract_system_message(&messages).unwrap();
        assert_eq!(system, None);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_convert_messages_to_contents() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::user("Hello")),
            ConversationMessage::Chat(ChatMessage::assistant("Hi there!")),
        ];

        let contents = client.convert_messages_to_contents(&messages).unwrap();

        assert_eq!(contents.len(), 2);
        assert_eq!(contents[0]["role"], "user");
        assert_eq!(contents[0]["parts"][0]["text"], "Hello");
        assert_eq!(contents[1]["role"], "model");
        assert_eq!(contents[1]["parts"][0]["text"], "Hi there!");
    }

    #[test]
    fn test_convert_tools_to_google_format() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");

        // Anthropic format tools
        let tools = vec![serde_json::json!({
            "name": "get_weather",
            "description": "Get weather for a location",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"}
                }
            }
        })];

        let google_tools = client.convert_tools_to_google_format(&tools).unwrap();

        assert!(google_tools.is_array());
        let declarations = &google_tools[0]["function_declarations"];
        assert!(declarations.is_array());
        assert_eq!(declarations[0]["name"], "get_weather");
    }

    #[test]
    fn test_build_request_body_basic() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("gemini-1.5-pro")
            .with_max_tokens(1024)
            .with_temperature(0.7);

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["contents"].is_array());
        assert_eq!(body["generationConfig"]["maxOutputTokens"], 1024);
        assert!((body["generationConfig"]["temperature"].as_f64().unwrap() - 0.7).abs() < 0.01);
    }

    #[test]
    fn test_build_request_with_system() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let messages = vec![
            ConversationMessage::Chat(ChatMessage::system("You are helpful")),
            ConversationMessage::Chat(ChatMessage::user("Hello")),
        ];
        let config = GenerationConfig::new("gemini-1.5-pro");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["systemInstruction"].is_object());
        assert_eq!(
            body["systemInstruction"]["parts"][0]["text"],
            "You are helpful"
        );
        // Only user message should be in contents
        assert_eq!(body["contents"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_build_request_with_thinking() {
        let client =
            GoogleGenAIClient::new("test-key", "gemini-1.5-pro").with_thinking_budget(1024);
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Think"))];
        let config = GenerationConfig::new("gemini-1.5-pro");

        let body = client
            .build_request_body(&messages, &config, false)
            .unwrap();

        assert!(body["generationConfig"]["thinkingConfig"].is_object());
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["includeThoughts"],
            true
        );
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingBudget"],
            1024
        );
    }

    #[test]
    fn test_extract_text_content() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let candidates = serde_json::json!([{
            "content": {
                "parts": [
                    {"text": "Hello, world!"}
                ]
            }
        }]);

        let response = client.extract_response(&candidates).unwrap();
        assert_eq!(response.text(), "Hello, world!");
    }

    #[test]
    fn test_extract_text_skips_thoughts() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let candidates = serde_json::json!([{
            "content": {
                "parts": [
                    {"thought": true, "text": "Let me think..."},
                    {"text": "The answer is 42"}
                ]
            }
        }]);

        let response = client.extract_response(&candidates).unwrap();
        assert_eq!(response.text(), "The answer is 42");
    }

    #[test]
    fn test_extract_reasoning() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let candidates = serde_json::json!([{
            "content": {
                "parts": [
                    {"thought": true, "text": "Let me think..."},
                    {"text": "The answer is 42"}
                ]
            }
        }]);

        let response = client.extract_response(&candidates).unwrap();
        assert_eq!(response.reasoning(), "Let me think...");
    }

    #[test]
    fn test_extract_tool_calls() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let candidates = serde_json::json!([{
            "content": {
                "parts": [{
                    "functionCall": {
                        "name": "get_weather",
                        "args": {"location": "San Francisco"}
                    }
                }]
            }
        }]);

        let response = client.extract_response(&candidates).unwrap();
        assert_eq!(response.tool_calls().len(), 1);
        assert_eq!(response.tool_calls()[0].tool_name, "get_weather");
    }

    #[test]
    fn test_parse_usage() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        let usage = serde_json::json!({
            "promptTokenCount": 100,
            "candidatesTokenCount": 50,
            "cachedContentTokenCount": 25
        });

        let parsed = client.parse_usage(&usage);
        assert_eq!(parsed.input_tokens, 100);
        assert_eq!(parsed.output_tokens, 50);
        assert_eq!(parsed.cached_tokens, 25);
    }

    #[test]
    fn test_get_endpoint() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");

        let endpoint = client.get_endpoint(false);
        assert!(endpoint.contains("generateContent"));
        assert!(endpoint.contains("gemini-1.5-pro"));
        assert!(endpoint.contains("key=test-key"));

        let stream_endpoint = client.get_endpoint(true);
        assert!(stream_endpoint.contains("streamGenerateContent"));
    }

    #[test]
    fn test_provider() {
        let client = GoogleGenAIClient::new("test-key", "gemini-1.5-pro");
        assert_eq!(client.provider(), Provider::GoogleGenAI);
    }

    #[test]
    fn test_build_request_body_enables_streaming_function_call_arguments() {
        let client = GoogleGenAIClient::new("test-key", "gemini-2.5-pro");
        let messages = vec![ConversationMessage::Chat(ChatMessage::user("Hello"))];
        let config = GenerationConfig::new("gemini-2.5-pro").with_tools(vec![serde_json::json!({
            "name": "get_weather",
            "description": "Get weather",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"}
                }
            }
        })]);

        let body = client.build_request_body(&messages, &config, true).unwrap();
        assert_eq!(
            body["toolConfig"]["functionCallingConfig"]["streamFunctionCallArguments"],
            true
        );
    }

    #[test]
    fn test_parse_google_stream_value_emits_partial_and_complete_tool_calls() {
        let mut accumulator = GoogleToolCallAccumulator::default();

        let name_chunk = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {
                            "id": "fc_123",
                            "name": "get_weather",
                            "willContinue": true
                        }
                    }]
                }
            }]
        });
        let chunks = parse_google_stream_value(&name_chunk, &mut accumulator);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            Ok(StreamChunk::ToolCallPartial(tool)) => {
                assert_eq!(tool["id"], "fc_123");
                assert_eq!(tool["function"]["name"], "get_weather");
                assert_eq!(tool["function"]["arguments"], "{}");
            }
            other => panic!("expected ToolCallPartial, got {:?}", other),
        }

        let partial_chunk = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {
                            "partialArgs": [{
                                "jsonPath": "$.location",
                                "stringValue": "San Francisco"
                            }],
                            "willContinue": true
                        }
                    }]
                }
            }]
        });
        let chunks = parse_google_stream_value(&partial_chunk, &mut accumulator);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            Ok(StreamChunk::ToolCallPartial(tool)) => {
                assert_eq!(tool["id"], "fc_123");
                assert_eq!(
                    tool["function"]["arguments"],
                    "{\"location\":\"San Francisco\"}"
                );
            }
            other => panic!("expected ToolCallPartial, got {:?}", other),
        }

        let complete_chunk = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {
                            "id": "fc_123",
                            "name": "get_weather",
                            "args": {
                                "location": "San Francisco"
                            }
                        }
                    }]
                }
            }]
        });
        let chunks = parse_google_stream_value(&complete_chunk, &mut accumulator);
        assert_eq!(chunks.len(), 1);
        match &chunks[0] {
            Ok(StreamChunk::ToolCallComplete(tool)) => {
                assert_eq!(tool["id"], "fc_123");
                assert_eq!(tool["_tool_name"], "get_weather");
                assert_eq!(tool["name"], "get_weather");
                assert_eq!(tool["input"]["location"], "San Francisco");
                assert_eq!(tool["function"]["name"], "get_weather");
                assert_eq!(tool["function"]["arguments"]["location"], "San Francisco");
            }
            other => panic!("expected ToolCallComplete, got {:?}", other),
        }
    }

    #[test]
    fn test_apply_google_partial_arg_handles_nested_objects_and_arrays() {
        let mut args = serde_json::json!({});
        apply_google_partial_arg(
            &mut args,
            &serde_json::json!({
                "jsonPath": "$.doc.title",
                "stringValue": "Hello"
            }),
        );
        apply_google_partial_arg(
            &mut args,
            &serde_json::json!({
                "jsonPath": "$.items[0].name",
                "stringValue": "first"
            }),
        );

        assert_eq!(args["doc"]["title"], "Hello");
        assert_eq!(args["items"][0]["name"], "first");
    }
}
