use pyo3::prelude::*;

mod errors;
mod json;
mod mcp;
mod messages;
mod programmatic;
mod runtime;
mod stream;
mod types;
mod vfs;

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn chevalier(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "ChevalierError",
        module.py().get_type::<errors::ChevalierError>(),
    )?;
    module.add_class::<types::ToolCall>()?;
    module.add_class::<types::TextResponsePart>()?;
    module.add_class::<types::ReasoningResponsePart>()?;
    module.add_class::<types::ToolResponsePart>()?;
    module.add_class::<types::SignatureResponsePart>()?;
    module.add_class::<types::AssistantResponse>()?;
    module.add_class::<types::TokenUsage>()?;
    module.add_class::<types::ProviderRateLimit>()?;
    module.add_class::<types::OutputStreamEvent>()?;
    module.add_class::<types::ToolPartialStreamEvent>()?;
    module.add_class::<types::UsageStreamEvent>()?;
    module.add_class::<types::RateLimitsStreamEvent>()?;
    module.add_class::<types::CompleteStreamEvent>()?;
    module.add_class::<runtime::Runtime>()?;
    module.add_class::<programmatic::ProgrammaticExecution>()?;
    module.add_function(wrap_pyfunction!(
        programmatic::programmatic_description,
        module
    )?)?;
    module.add_class::<stream::StreamHandle>()?;
    module.add_class::<mcp::McpClient>()?;
    module.add_class::<mcp::McpServer>()?;
    module.add_class::<vfs::VfsStorage>()?;
    module.add_class::<vfs::VfsContentHasher>()?;
    module.add_function(wrap_pyfunction!(vfs::vfs_content_hash, module)?)?;
    module.add_function(wrap_pyfunction!(vfs::vfs_content_hash_algorithm, module)?)?;
    module.add_function(wrap_pyfunction!(version, module)?)?;
    Ok(())
}
