use pyo3::prelude::*;

mod error;
mod handles;
mod json;
mod options;
mod sandbox;
mod session;
mod types;

#[pymodule]
fn chevalier_sandbox(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<sandbox::Sandbox>()?;
    module.add_class::<session::Session>()?;
    module.add_class::<handles::ExecHandle>()?;
    module.add_class::<handles::ShellHandle>()?;
    module.add_class::<handles::ForwardHandle>()?;
    Ok(())
}
