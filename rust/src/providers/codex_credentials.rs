//! ChatGPT subscription credentials owned by the host application.
//!
//! A host serving many people cannot put each person's access token in a
//! model string: model strings are logged, persisted with resumable loop
//! state, and copied into derived routes. Instead the string names the
//! account (`openai-codex-responses:gpt-5.5@account=<id>`) and the host
//! installs a source that turns that name into a live credential at call
//! time, refreshing it as needed.

use async_trait::async_trait;
use std::sync::{Arc, OnceLock};

use super::CodexSubscriptionProviderConfig;
use crate::error::{Error, Result};

#[async_trait]
pub trait CodexCredentialSource: Send + Sync {
    /// The provider config for the account a model string names with `@account=`.
    async fn credential(&self, account: &str) -> Result<CodexSubscriptionProviderConfig>;
}

static SOURCE: OnceLock<Arc<dyn CodexCredentialSource>> = OnceLock::new();

/// Install the process-wide source. Returns false when one is already installed.
pub fn install_codex_credential_source(source: Arc<dyn CodexCredentialSource>) -> bool {
    SOURCE.set(source).is_ok()
}

pub(crate) async fn resolve(account: &str) -> Result<CodexSubscriptionProviderConfig> {
    let source = SOURCE.get().ok_or_else(|| {
        Error::NonRetryable(
            "model names a subscription @account but no Codex credential source is installed"
                .to_string(),
        )
    })?;
    source.credential(account).await
}
