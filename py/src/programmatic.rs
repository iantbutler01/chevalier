use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chevalier_core::error::{Error, Result};
use chevalier_core::programmatic::{
    CancellationToken, ProgrammaticDispatcher, ProgrammaticEvent, ProgrammaticOptions,
    ProgrammaticSandbox, ProgrammaticSession, ProgrammaticTool, execute_programmatic,
    programmatic_tool_description,
};
use pyo3::prelude::*;
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::errors::to_py_err;
use crate::json::{from_python, to_python};

struct Callback {
    callable: PyObject,
    locals: pyo3_async_runtimes::TaskLocals,
}
type PythonFuture = Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>>;

struct Bridge {
    callback: Arc<Callback>,
    next_id: AtomicU32,
    finished: CancellationToken,
    watchers: Mutex<Vec<JoinHandle<Result<()>>>>,
}

impl Bridge {
    async fn invoke(&self, request: Value) -> Result<Value> {
        let future: PythonFuture = Python::with_gil(|py| -> Result<PythonFuture> {
            let request =
                to_python(py, &request).map_err(|error| Error::NonRetryable(error.to_string()))?;
            let coroutine = self
                .callback
                .callable
                .call1(py, (request,))
                .map_err(|error| Error::NonRetryable(error.to_string()))?;
            let locals = self.callback.locals.clone_ref(py);
            let future =
                pyo3_async_runtimes::into_future_with_locals(&locals, coroutine.into_bound(py))
                    .map_err(|error| Error::NonRetryable(error.to_string()))?;
            Ok(Box::pin(future))
        })?;
        let value = future.await.map_err(|error| {
            Error::NonRetryable(format!("programmatic callback failed: {error}"))
        })?;
        Python::with_gil(|py| {
            from_python(value.bind(py)).map_err(|error| Error::NonRetryable(error.to_string()))
        })
    }

    fn watch_cancel(
        self: &Arc<Self>,
        id: u32,
        cancel: CancellationToken,
        completed: CancellationToken,
    ) -> JoinHandle<Result<()>> {
        let bridge = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    bridge.invoke(json!({ "op": "cancel", "id": id })).await?;
                }
                () = completed.cancelled() => {}
                () = bridge.finished.cancelled() => {}
            }
            Ok(())
        })
    }

    async fn finish(&self) -> Result<()> {
        self.finished.cancel();
        let watchers = std::mem::take(&mut *self.watchers.lock().await);
        let teardown = tokio::spawn(async move {
            let mut failure = None;
            for watcher in watchers {
                let result = watcher.await.map_err(|error| {
                    Error::NonRetryable(format!("programmatic cancellation bridge failed: {error}"))
                });
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) | Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        });
        tokio::time::timeout(Duration::from_secs(5), teardown)
            .await
            .map_err(|_| Error::NonRetryable("Sandbox did not confirm callback cleanup within five seconds; pending callbacks remain owned for late cleanup".into()))?
            .map_err(|error| Error::NonRetryable(format!("Programmatic callback cleanup failed: {error}")))?
    }
}

struct SandboxAdapter(Arc<Bridge>);
struct DispatcherAdapter(Arc<Bridge>);
struct SessionAdapter {
    bridge: Arc<Bridge>,
    id: u32,
}

async fn cleanup_late_session(session: Arc<dyn ProgrammaticSession>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        session.signal(9).await?;
        loop {
            if matches!(
                session.next().await?,
                None | Some(ProgrammaticEvent::Exit { .. })
            ) {
                break;
            }
        }
        Ok::<(), Error>(())
    })
    .await
    .map_err(|_| Error::NonRetryable("Late sandbox startup did not confirm termination".into()))?
}

#[async_trait]
impl ProgrammaticSandbox for SandboxAdapter {
    async fn start_exec(
        &self,
        command: &str,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<Arc<dyn ProgrammaticSession>> {
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        let watcher = self
            .0
            .watch_cancel(id, cancel.clone(), CancellationToken::new());
        self.0.watchers.lock().await.push(watcher);
        let bridge = self.0.clone();
        let command = command.to_owned();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let startup = tokio::spawn(async move {
            let result = async {
                bridge
                    .invoke(json!({
                    "op": "start", "id": id, "command": command,
                    "timeoutMs": timeout.as_millis() as u64,
                    }))
                    .await?;
                let session: Arc<dyn ProgrammaticSession> = Arc::new(SessionAdapter { bridge, id });
                if cancel.is_cancelled() {
                    cleanup_late_session(session).await?;
                    return Err(Error::NonRetryable("Programmatic startup cancelled".into()));
                }
                Ok(session)
            }
            .await;
            match sender.send(result) {
                Ok(()) => Ok(()),
                Err(Ok(session)) => cleanup_late_session(session).await,
                Err(Err(error)) => Err(error),
            }
        });
        self.0.watchers.lock().await.push(startup);
        receiver.await.map_err(|error| {
            Error::NonRetryable(format!("Programmatic startup callback failed: {error}"))
        })?
    }
}

#[async_trait]
impl ProgrammaticSession for SessionAdapter {
    async fn write(&self, data: &str) -> Result<()> {
        self.bridge
            .invoke(json!({ "op": "write", "sessionId": self.id, "data": data }))
            .await?;
        Ok(())
    }

    async fn next(&self) -> Result<Option<ProgrammaticEvent>> {
        let result = self
            .bridge
            .invoke(json!({ "op": "next", "sessionId": self.id }))
            .await?;
        serde_json::from_value(result).map_err(Error::from)
    }

    async fn signal(&self, signal: i32) -> Result<()> {
        self.bridge
            .invoke(json!({ "op": "signal", "sessionId": self.id, "signal": signal }))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ProgrammaticDispatcher for DispatcherAdapter {
    async fn call(&self, name: &str, args: Value, cancel: CancellationToken) -> Result<Value> {
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        let completed = CancellationToken::new();
        let watcher = self.0.watch_cancel(id, cancel, completed.clone());
        let result = self
            .0
            .invoke(json!({ "op": "call", "id": id, "name": name, "args": args }))
            .await;
        completed.cancel();
        let forwarded = watcher.await.map_err(|error| {
            Error::NonRetryable(format!("programmatic cancellation bridge failed: {error}"))
        })?;
        forwarded?;
        result
    }
}

struct Execution {
    bridge: Arc<Bridge>,
    cancel: CancellationToken,
    started: AtomicBool,
}

impl Execution {
    fn new(callback: Callback) -> Self {
        Self {
            bridge: Arc::new(Bridge {
                callback: Arc::new(callback),
                next_id: AtomicU32::new(1),
                finished: CancellationToken::new(),
                watchers: Mutex::new(Vec::new()),
            }),
            cancel: CancellationToken::new(),
            started: AtomicBool::new(false),
        }
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn execute(
        &self,
        code: String,
        tools: Value,
        timeout_ms: Option<u32>,
    ) -> Result<Value> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(Error::NonRetryable(
                "ProgrammaticExecution can only execute once".into(),
            ));
        }
        let tools: Vec<ProgrammaticTool> = serde_json::from_value(tools).map_err(Error::from)?;
        let result = execute_programmatic(
            &code,
            &tools,
            Arc::new(SandboxAdapter(self.bridge.clone())),
            Arc::new(DispatcherAdapter(self.bridge.clone())),
            ProgrammaticOptions {
                timeout: Duration::from_millis(u64::from(timeout_ms.unwrap_or(30_000))),
                cancel: self.cancel.clone(),
            },
        )
        .await;
        let cleanup = self.bridge.finish().await;
        let result = match (result, cleanup) {
            (Ok(result), Ok(())) => result,
            (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
            (Err(error), Err(cleanup)) => {
                return Err(Error::NonRetryable(format!("{error}; {cleanup}")));
            }
        };
        Ok(json!({ "output": result.output }))
    }
}

#[pyclass(module = "chevalier.chevalier")]
pub struct ProgrammaticExecution {
    inner: Arc<Execution>,
}

#[pymethods]
impl ProgrammaticExecution {
    #[new]
    fn new(py: Python<'_>, callback: PyObject) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(Execution::new(Callback {
                callable: callback,
                locals: pyo3_async_runtimes::tokio::get_current_locals(py)?,
            })),
        })
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    #[pyo3(signature = (code, tools, timeout_ms=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        code: String,
        tools: &Bound<'_, PyAny>,
        timeout_ms: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tools = from_python(tools)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output = inner
                .execute(code, tools, timeout_ms)
                .await
                .map_err(to_py_err)?;
            Python::with_gil(|py| to_python(py, &output))
        })
    }
}

#[pyfunction]
pub fn programmatic_description(tools: &Bound<'_, PyAny>) -> PyResult<String> {
    let tools: Vec<ProgrammaticTool> = from_python(tools)?;
    Ok(programmatic_tool_description(&tools))
}
