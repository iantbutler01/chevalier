use chevalier_core::runtime::ToolSchemaInfo;
use chevalier_core::types::{
    AssistantResponse as EngineAssistantResponse, ProviderRateLimit as EngineProviderRateLimit,
    ResponsePart as EngineResponsePart, ResponseStreamEvent as EngineResponseStreamEvent,
    TokenUsage as EngineTokenUsage, ToolCall as EngineToolCall,
};
use pyo3::prelude::*;
use serde::Serialize;

use crate::json::to_python;

#[pyclass(frozen, module = "chevalier.chevalier")]
#[derive(Clone)]
pub struct ToolCall {
    inner: EngineToolCall,
}

impl From<EngineToolCall> for ToolCall {
    fn from(inner: EngineToolCall) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl ToolCall {
    #[getter]
    fn tool_use_id(&self) -> String {
        self.inner.tool_use_id.clone()
    }

    #[getter]
    fn tool_name(&self) -> String {
        self.inner.tool_name.clone()
    }

    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<PyObject> {
        to_python(py, &self.inner.args)
    }

    #[getter]
    fn raw_arguments(&self) -> Option<String> {
        self.inner.raw_arguments.clone()
    }

    #[getter]
    fn signature(&self) -> Option<String> {
        self.inner.signature.clone()
    }

    #[getter]
    fn tool_obj(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.inner
            .tool_obj
            .as_ref()
            .map(|value| to_python(py, value))
            .unwrap_or_else(|| Ok(py.None()))
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct TextResponsePart {
    text: String,
}

#[pymethods]
impl TextResponsePart {
    #[getter]
    fn text(&self) -> String {
        self.text.clone()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct ReasoningResponsePart {
    text: String,
}

#[pymethods]
impl ReasoningResponsePart {
    #[getter]
    fn text(&self) -> String {
        self.text.clone()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct ToolResponsePart {
    call: EngineToolCall,
}

#[pymethods]
impl ToolResponsePart {
    #[getter]
    fn call(&self) -> ToolCall {
        self.call.clone().into()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct SignatureResponsePart {
    value: String,
}

#[pymethods]
impl SignatureResponsePart {
    #[getter]
    fn value(&self) -> String {
        self.value.clone()
    }
}

fn response_part_to_python(py: Python<'_>, part: EngineResponsePart) -> PyResult<PyObject> {
    match part {
        EngineResponsePart::Text { text } => Ok(Py::new(py, TextResponsePart { text })?.into_any()),
        EngineResponsePart::Reasoning { text } => {
            Ok(Py::new(py, ReasoningResponsePart { text })?.into_any())
        }
        EngineResponsePart::Tool { call } => Ok(Py::new(py, ToolResponsePart { call })?.into_any()),
        EngineResponsePart::Signature { value } => {
            Ok(Py::new(py, SignatureResponsePart { value })?.into_any())
        }
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
#[derive(Clone)]
pub struct AssistantResponse {
    inner: EngineAssistantResponse,
}

impl From<EngineAssistantResponse> for AssistantResponse {
    fn from(inner: EngineAssistantResponse) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl AssistantResponse {
    #[getter]
    fn output(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.inner
            .output
            .iter()
            .cloned()
            .map(|part| response_part_to_python(py, part))
            .collect()
    }

    fn text(&self) -> String {
        self.inner.text()
    }

    fn as_str(&self) -> Option<String> {
        self.inner.as_str().map(ToString::to_string)
    }

    fn reasoning(&self) -> String {
        self.inner.reasoning()
    }

    fn tool_calls(&self) -> Vec<ToolCall> {
        self.inner
            .tool_calls()
            .into_iter()
            .cloned()
            .map(ToolCall::from)
            .collect()
    }

    fn signatures(&self) -> Vec<String> {
        self.inner
            .signatures()
            .into_iter()
            .map(ToString::to_string)
            .collect()
    }

    fn has_tool_calls(&self) -> bool {
        self.inner.has_tool_calls()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct TokenUsage {
    inner: EngineTokenUsage,
}

#[pymethods]
impl TokenUsage {
    #[getter]
    fn input_tokens(&self) -> u64 {
        self.inner.input_tokens
    }

    #[getter]
    fn output_tokens(&self) -> u64 {
        self.inner.output_tokens
    }

    #[getter]
    fn cached_tokens(&self) -> u64 {
        self.inner.cached_tokens
    }

    #[getter]
    fn cache_write_input_tokens(&self) -> u64 {
        self.inner.cache_write_input_tokens
    }

    fn total_tokens(&self) -> u64 {
        self.inner.total_tokens()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct ProviderRateLimit {
    inner: EngineProviderRateLimit,
}

#[pymethods]
impl ProviderRateLimit {
    #[getter]
    fn scope(&self) -> &'static str {
        self.inner.scope.as_str()
    }

    #[getter]
    fn used_percent(&self) -> u32 {
        self.inner.used_percent
    }

    #[getter]
    fn window_minutes(&self) -> u64 {
        self.inner.window_minutes
    }

    #[getter]
    fn resets_at_epoch_sec(&self) -> u64 {
        self.inner.resets_at_epoch_sec
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct OutputStreamEvent {
    output: EngineResponsePart,
}

#[pymethods]
impl OutputStreamEvent {
    #[getter(r#type)]
    fn kind(&self) -> &'static str {
        "output"
    }

    #[getter]
    fn output(&self, py: Python<'_>) -> PyResult<PyObject> {
        response_part_to_python(py, self.output.clone())
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct ToolPartialStreamEvent {
    data: serde_json::Value,
}

#[pymethods]
impl ToolPartialStreamEvent {
    #[getter(r#type)]
    fn kind(&self) -> &'static str {
        "toolPartial"
    }

    #[getter]
    fn data(&self, py: Python<'_>) -> PyResult<PyObject> {
        to_python(py, &self.data)
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct UsageStreamEvent {
    usage: EngineTokenUsage,
}

#[pymethods]
impl UsageStreamEvent {
    #[getter(r#type)]
    fn kind(&self) -> &'static str {
        "usage"
    }

    #[getter]
    fn usage(&self) -> TokenUsage {
        TokenUsage {
            inner: self.usage.clone(),
        }
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct RateLimitsStreamEvent {
    rate_limits: Vec<EngineProviderRateLimit>,
}

#[pymethods]
impl RateLimitsStreamEvent {
    #[getter(r#type)]
    fn kind(&self) -> &'static str {
        "rateLimits"
    }

    #[getter]
    fn rate_limits(&self) -> Vec<ProviderRateLimit> {
        self.rate_limits
            .iter()
            .cloned()
            .map(|inner| ProviderRateLimit { inner })
            .collect()
    }
}

#[pyclass(frozen, module = "chevalier.chevalier")]
pub struct CompleteStreamEvent {
    response: EngineAssistantResponse,
}

#[pymethods]
impl CompleteStreamEvent {
    #[getter(r#type)]
    fn kind(&self) -> &'static str {
        "complete"
    }

    #[getter]
    fn response(&self) -> AssistantResponse {
        self.response.clone().into()
    }
}

pub fn stream_event_to_python(
    py: Python<'_>,
    event: EngineResponseStreamEvent,
) -> PyResult<PyObject> {
    match event {
        EngineResponseStreamEvent::Output(output) => {
            Ok(Py::new(py, OutputStreamEvent { output })?.into_any())
        }
        EngineResponseStreamEvent::ToolPartial(data) => {
            Ok(Py::new(py, ToolPartialStreamEvent { data })?.into_any())
        }
        EngineResponseStreamEvent::Usage(usage) => {
            Ok(Py::new(py, UsageStreamEvent { usage })?.into_any())
        }
        EngineResponseStreamEvent::RateLimits(rate_limits) => {
            Ok(Py::new(py, RateLimitsStreamEvent { rate_limits })?.into_any())
        }
        EngineResponseStreamEvent::ResponseId(_) => Ok(py.None()),
        EngineResponseStreamEvent::Complete(response) => {
            Ok(Py::new(py, CompleteStreamEvent { response })?.into_any())
        }
    }
}

#[derive(Serialize)]
pub struct ToolSchemaOutput {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

impl From<ToolSchemaInfo> for ToolSchemaOutput {
    fn from(schema: ToolSchemaInfo) -> Self {
        Self {
            name: schema.name,
            description: schema.description,
            parameters: schema.parameters.to_json_schema(),
        }
    }
}
