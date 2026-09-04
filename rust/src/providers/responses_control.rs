use crate::error::{Error, Result};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};

#[derive(Debug)]
pub struct ResponsesCommand {
    pub kind: &'static str,
    pub input: Value,
    pub reply: oneshot::Sender<Result<()>>,
}

#[derive(Debug, Clone)]
pub struct ResponsesControl {
    pub(crate) ready: Arc<AtomicBool>,
    sender: mpsc::UnboundedSender<ResponsesCommand>,
    pub(crate) receiver: Arc<Mutex<mpsc::UnboundedReceiver<ResponsesCommand>>>,
}

impl Default for ResponsesControl {
    fn default() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ResponsesControl {
    pub async fn send(&self, kind: &'static str, input: Value) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            return Err(Error::NonRetryable(
                "Responses WebSocket control is not active".into(),
            ));
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .send(ResponsesCommand { kind, input, reply })
            .map_err(|_| Error::NonRetryable("Responses control is closed".into()))?;
        result.await.map_err(|_| {
            Error::NonRetryable("Responses connection closed before control was sent".into())
        })?
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResponsesOptions {
    pub websocket: bool,
    pub compaction_threshold: Option<u32>,
    pub control: Option<ResponsesControl>,
}

pub fn apply_compaction(
    request: &mut Value,
    options: Option<&ResponsesOptions>,
    continuation: bool,
) {
    if let Some(threshold) = options.and_then(|options| options.compaction_threshold) {
        request["context_management"] =
            serde_json::json!([{ "type": "compaction", "compact_threshold": threshold }]);
        if !continuation
            && let Some(items) = request["input"].as_array_mut()
            && let Some(index) = items.iter().rposition(|item| item["type"] == "compaction")
        {
            items.drain(..index);
        }
    }
}
