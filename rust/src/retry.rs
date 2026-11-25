//! Retry logic with exponential backoff
//!
//! Provides retry functionality for inference clients with configurable backoff strategy.

use std::time::Duration;
use tokio::time::sleep;

use crate::error::{Error, Result};

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Initial backoff duration
    pub initial_backoff: Duration,

    /// Maximum backoff duration
    pub max_backoff: Duration,

    /// Backoff multiplier (exponential)
    pub multiplier: f64,

    /// Maximum total time to spend retrying
    pub max_time: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            multiplier: 2.0,
            max_time: Duration::from_secs(60),
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set initial backoff duration
    pub fn with_initial_backoff(mut self, duration: Duration) -> Self {
        self.initial_backoff = duration;
        self
    }

    /// Set maximum backoff duration
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = duration;
        self
    }

    /// Set backoff multiplier
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Set maximum total time
    pub fn with_max_time(mut self, duration: Duration) -> Self {
        self.max_time = duration;
        self
    }
}

/// Retry a function with exponential backoff
///
/// # Arguments
/// * `config` - Retry configuration
/// * `f` - Async function to retry
///
/// # Example
/// ```no_run
/// use chevalier::retry::{retry_with_backoff, RetryConfig};
///
/// async fn example() {
///     let config = RetryConfig::default();
///     let result = retry_with_backoff(config, || async {
///         // Your retryable operation
///         Ok::<_, chevalier::error::Error>(42)
///     }).await;
/// }
/// ```
pub async fn retry_with_backoff<F, Fut, T>(config: RetryConfig, f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    retry_with_backoff_when(config, Error::is_retryable, f).await
}

/// [`retry_with_backoff`] for the errors `should_retry` accepts; any other error returns at
/// once, as it is.
pub async fn retry_with_backoff_when<F, Fut, T>(
    config: RetryConfig,
    should_retry: impl Fn(&Error) -> bool,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let start_time = std::time::Instant::now();
    let mut attempt = 0;
    let mut current_backoff = config.initial_backoff;
    let mut last_error = String::from("unknown error");

    loop {
        // Check if we've exceeded max time
        if start_time.elapsed() > config.max_time {
            return Err(Error::RetriesExceeded(last_error));
        }

        // Try the operation
        match f().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                if !should_retry(&err) {
                    return Err(err);
                }

                last_error = err.to_string();

                // Check if we've exceeded max retries
                attempt += 1;
                if attempt > config.max_retries {
                    return Err(Error::RetriesExceeded(last_error));
                }

                // Wait with exponential backoff
                sleep(current_backoff).await;

                // Calculate next backoff (capped at max_backoff)
                current_backoff = Duration::from_secs_f64(
                    (current_backoff.as_secs_f64() * config.multiplier)
                        .min(config.max_backoff.as_secs_f64()),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(60));
        assert_eq!(config.multiplier, 2.0);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(5)
            .with_initial_backoff(Duration::from_millis(200))
            .with_max_backoff(Duration::from_secs(30))
            .with_multiplier(1.5);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(200));
        assert_eq!(config.max_backoff, Duration::from_secs(30));
        assert_eq!(config.multiplier, 1.5);
    }

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        use std::sync::{Arc, Mutex};

        let config = RetryConfig::default();
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let result = retry_with_backoff(config, move || {
            let count = call_count_clone.clone();
            async move {
                *count.lock().unwrap() += 1;
                Ok::<i32, Error>(42)
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(*call_count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        use std::sync::{Arc, Mutex};

        let config = RetryConfig::default().with_initial_backoff(Duration::from_millis(1));
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let result = retry_with_backoff(config, move || {
            let count = call_count_clone.clone();
            async move {
                let mut c = count.lock().unwrap();
                *c += 1;
                let current = *c;
                drop(c);

                if current < 3 {
                    Err(Error::Inference("Temporary error".to_string()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(*call_count.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_error() {
        use std::sync::{Arc, Mutex};

        let config = RetryConfig::default();
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let result = retry_with_backoff(config, move || {
            let count = call_count_clone.clone();
            async move {
                *count.lock().unwrap() += 1;
                Err::<i32, Error>(Error::NonRetryable("Bad request".to_string()))
            }
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonRetryable(_)));
        assert_eq!(*call_count.lock().unwrap(), 1); // Should not retry
    }

    #[tokio::test]
    async fn test_retry_exceeds_max_retries() {
        use std::sync::{Arc, Mutex};

        let config = RetryConfig::default()
            .with_max_retries(2)
            .with_initial_backoff(Duration::from_millis(1));
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let result = retry_with_backoff(config, move || {
            let count = call_count_clone.clone();
            async move {
                *count.lock().unwrap() += 1;
                Err::<i32, Error>(Error::Inference("Persistent error".to_string()))
            }
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::RetriesExceeded(_)));
        assert_eq!(*call_count.lock().unwrap(), 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_retry_exceeds_max_time() {
        let config = RetryConfig::default()
            .with_max_time(Duration::from_millis(50))
            .with_initial_backoff(Duration::from_millis(30));

        let result = retry_with_backoff(config, || async {
            Err::<i32, Error>(Error::Inference("Persistent error".to_string()))
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::RetriesExceeded(_)));
    }

    #[tokio::test]
    async fn test_backoff_increases_exponentially() {
        use std::sync::{Arc, Mutex};

        let config = RetryConfig::default()
            .with_initial_backoff(Duration::from_millis(10))
            .with_multiplier(2.0)
            .with_max_retries(3);

        let start = std::time::Instant::now();
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let _ = retry_with_backoff(config, move || {
            let count = call_count_clone.clone();
            async move {
                *count.lock().unwrap() += 1;
                Err::<i32, Error>(Error::Inference("Error".to_string()))
            }
        })
        .await;

        let elapsed = start.elapsed();

        // Should have waited: 10ms + 20ms + 40ms = 70ms minimum
        assert!(elapsed >= Duration::from_millis(70));
        assert_eq!(*call_count.lock().unwrap(), 4); // Initial + 3 retries
    }

    #[tokio::test]
    async fn test_backoff_respects_max_backoff() {
        let config = RetryConfig::default()
            .with_initial_backoff(Duration::from_millis(10))
            .with_max_backoff(Duration::from_millis(15))
            .with_multiplier(2.0)
            .with_max_retries(3);

        let start = std::time::Instant::now();

        let _ = retry_with_backoff(config, || async {
            Err::<i32, Error>(Error::Inference("Error".to_string()))
        })
        .await;

        let elapsed = start.elapsed();

        // Should have waited: 10ms + 15ms (capped) + 15ms (capped) = 40ms minimum
        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(100)); // But not too much more
    }
}
