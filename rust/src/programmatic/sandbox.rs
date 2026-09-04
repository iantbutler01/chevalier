use super::{CancellationToken, ProgrammaticEvent, ProgrammaticSandbox, ProgrammaticSession};
use crate::error::{Error, Result};
use crate::sandbox::{EventStream, ExecEvent, ExecHandle, ExecInput, ExecOptions, Session};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

#[derive(Clone)]
pub struct ProgrammaticSandboxConfig {
    pub session: Session,
    pub cwd: Option<String>,
    pub env: HashMap<String, String>,
}

#[async_trait]
impl ProgrammaticSandbox for ProgrammaticSandboxConfig {
    async fn start_exec(
        &self,
        command: &str,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<Arc<dyn ProgrammaticSession>> {
        let command = match &self.cwd {
            Some(cwd) => format!("cd -- '{}' && exec {command}", cwd.replace('\'', "'\\''")),
            None => format!("exec {command}"),
        };
        let timeout_secs = timeout
            .as_secs()
            .saturating_add(u64::from(timeout.subsec_nanos() != 0));
        let options = ExecOptions {
            env: self.env.clone(),
            timeout_secs: Some(timeout_secs.clamp(1, i32::MAX as u64) as i32),
            ..Default::default()
        };
        if cancel.is_cancelled() {
            return Err(Error::NonRetryable(
                "Programmatic execution cancelled".into(),
            ));
        }
        let existing = self.session.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = existing
                .exec(&command, options)
                .await
                .map_err(sandbox_error)
                .map(|handle| {
                    Arc::new(SandboxProgrammaticSession::new(handle))
                        as Arc<dyn ProgrammaticSession>
                });
            if cancel.is_cancelled() {
                if let Ok(session) = result {
                    terminate_late_start(session).await;
                }
                let _ = sender.send(Err(Error::NonRetryable(
                    "Programmatic execution cancelled".into(),
                )));
            } else if let Err(Ok(session)) = sender.send(result) {
                terminate_late_start(session).await;
            }
        });
        receiver.await.map_err(|error| {
            Error::NonRetryable(format!("Programmatic sandbox startup failed: {error}"))
        })?
    }
}

async fn terminate_late_start(session: Arc<dyn ProgrammaticSession>) {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        session.signal(9).await?;
        while let Some(event) = session.next().await? {
            if matches!(event, ProgrammaticEvent::Exit { .. }) {
                break;
            }
        }
        Ok::<_, Error>(())
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        tracing::error!(
            "Cancelled programmatic sandbox startup did not confirm termination: {result:?}"
        );
    }
}

fn sandbox_error(error: crate::sandbox::SandboxError) -> Error {
    Error::NonRetryable(format!("Programmatic sandbox: {error}"))
}

struct SandboxProgrammaticSession {
    input: mpsc::Sender<ExecInput>,
    events: Mutex<Pin<Box<dyn Stream<Item = Result<ProgrammaticEvent>> + Send>>>,
}

impl SandboxProgrammaticSession {
    fn new(handle: ExecHandle) -> Self {
        Self {
            input: handle.input,
            events: Mutex::new(Box::pin(decode_events(handle.events))),
        }
    }
}

#[async_trait]
impl ProgrammaticSession for SandboxProgrammaticSession {
    async fn write(&self, data: &str) -> Result<()> {
        self.input
            .send(ExecInput::Data(data.as_bytes().to_vec()))
            .await
            .map_err(|_| Error::NonRetryable("Programmatic sandbox input closed".into()))
    }

    async fn next(&self) -> Result<Option<ProgrammaticEvent>> {
        self.events.lock().await.next().await.transpose()
    }

    async fn signal(&self, signal: i32) -> Result<()> {
        self.input
            .send(ExecInput::Signal(signal))
            .await
            .map_err(|_| Error::NonRetryable("Programmatic sandbox input closed".into()))
    }
}

fn decode_events(
    mut events: EventStream<ExecEvent>,
) -> impl Stream<Item = Result<ProgrammaticEvent>> + Send {
    async_stream::try_stream! {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        while let Some(event) = events.next().await {
            match event.map_err(sandbox_error)? {
                ExecEvent::Stdout(bytes) => {
                    let text = decode_chunk(&mut stdout, bytes)?;
                    if !text.is_empty() { yield ProgrammaticEvent::Stdout { text }; }
                }
                ExecEvent::Stderr(bytes) => {
                    let text = decode_chunk(&mut stderr, bytes)?;
                    if !text.is_empty() { yield ProgrammaticEvent::Stderr { text }; }
                }
                ExecEvent::Exit(code) => {
                    std::str::from_utf8(&stdout)?;
                    std::str::from_utf8(&stderr)?;
                    yield ProgrammaticEvent::Exit { code };
                }
                ExecEvent::Timeout => { yield ProgrammaticEvent::Timeout; }
            }
        }
        std::str::from_utf8(&stdout)?;
        std::str::from_utf8(&stderr)?;
    }
}

fn decode_chunk(pending: &mut Vec<u8>, bytes: Vec<u8>) -> Result<String> {
    pending.extend(bytes);
    let valid_length = match std::str::from_utf8(pending) {
        Ok(text) => text.len(),
        Err(error) if error.error_len().is_none() => error.valid_up_to(),
        Err(error) => return Err(error.into()),
    };
    let text = std::str::from_utf8(&pending[..valid_length])?.to_owned();
    pending.drain(..valid_length);
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn independent_input_unblocks_pending_output_and_utf8_survives_byte_boundaries() {
        let (input, mut commands) = mpsc::channel(4);
        let events = async_stream::stream! {
            match commands.recv().await.unwrap() {
                ExecInput::Data(bytes) => assert_eq!(bytes, "request\n".as_bytes()),
                other => panic!("Unexpected input {other:?}"),
            }
            for byte in "🦀".as_bytes() {
                yield Ok(ExecEvent::Stdout(vec![*byte]));
                yield Ok(ExecEvent::Stderr(vec![b'x']));
            }
            match commands.recv().await.unwrap() {
                ExecInput::Signal(15) => {},
                other => panic!("Unexpected signal {other:?}"),
            }
            yield Ok(ExecEvent::Exit(0));
        };
        let session = SandboxProgrammaticSession::new(ExecHandle {
            input,
            events: Box::pin(events),
        });
        let reader = async {
            let mut stdout = String::new();
            let mut stderr = String::new();
            while let Some(event) = session.next().await.unwrap() {
                match event {
                    ProgrammaticEvent::Stdout { text } => stdout.push_str(&text),
                    ProgrammaticEvent::Stderr { text } => stderr.push_str(&text),
                    ProgrammaticEvent::Exit { code } => {
                        assert_eq!(code, 0);
                        break;
                    }
                    ProgrammaticEvent::Timeout => panic!("Unexpected timeout"),
                }
            }
            assert_eq!(stdout, "🦀");
            assert_eq!(stderr, "xxxx");
        };
        let writer = async {
            tokio::task::yield_now().await;
            session.write("request\n").await.unwrap();
            session.signal(15).await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(reader, writer);
        })
        .await
        .unwrap();
    }
}
