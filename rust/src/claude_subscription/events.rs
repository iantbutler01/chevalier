use super::{ClaudeSessionError, ClaudeSessionEvent};
use crate::types::{ProviderRateLimit, ProviderRateLimitScope, TokenUsage};
use claude_codes::ClaudeOutput;

pub(crate) fn map(output: &ClaudeOutput) -> Result<Vec<ClaudeSessionEvent>, ClaudeSessionError> {
    let mapped = match output {
        ClaudeOutput::StreamEvent(stream) => match stream.event["delta"]["type"].as_str() {
            Some("text_delta") => stream.event["delta"]["text"]
                .as_str()
                .map(|v| vec![ClaudeSessionEvent::TextDelta(v.into())])
                .unwrap_or_default(),
            Some("thinking_delta") => stream.event["delta"]["thinking"]
                .as_str()
                .map(|v| vec![ClaudeSessionEvent::ThinkingDelta(v.into())])
                .unwrap_or_default(),
            _ => Vec::new(),
        },
        ClaudeOutput::Assistant(message) => {
            let text = message
                .message
                .content
                .iter()
                .filter_map(|part| match part {
                    claude_codes::ContentBlock::Text(block) => Some(block.text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            if text.is_empty() {
                Vec::new()
            } else {
                vec![ClaudeSessionEvent::AssistantMessage { text }]
            }
        }
        ClaudeOutput::RateLimitEvent(event) => {
            let info = &event.rate_limit_info;
            let mut windows = Vec::new();
            if let Some(unified) = &info.unified_windows {
                for (name, value) in [
                    ("five_hour", &unified.five_hour),
                    ("seven_day", &unified.seven_day),
                    (
                        "seven_day_overage_included",
                        &unified.seven_day_overage_included,
                    ),
                ] {
                    if let Some(value) = value {
                        windows.push(limit(name, value.utilization, value.resets_at));
                    }
                }
            } else if let (Some(name), Some(utilization), Some(reset)) = (
                info.rate_limit_type.as_ref(),
                info.utilization,
                info.resets_at,
            ) {
                windows.push(limit(name.as_str(), utilization, reset));
            }
            vec![ClaudeSessionEvent::RateLimits(windows)]
        }
        ClaudeOutput::Result(result) => {
            let usage = result
                .usage
                .as_ref()
                .map(|usage| TokenUsage {
                    input_tokens: usage.input_tokens as u64,
                    output_tokens: usage.output_tokens as u64,
                    cached_tokens: usage.cache_read_input_tokens as u64,
                    cache_write_input_tokens: usage.cache_creation_input_tokens as u64,
                    reasoning_tokens: usage
                        .output_tokens_details
                        .as_ref()
                        .and_then(|v| v.thinking_tokens),
                    provider_cost_dollars: None,
                })
                .unwrap_or_default();
            vec![ClaudeSessionEvent::TurnComplete {
                usage,
                list_price_usd: Some(result.total_cost_usd),
                num_turns: result.num_turns.max(0) as u32,
                is_error: result.is_error,
                subtype: result.subtype.as_str().into(),
                result: result.result.clone(),
            }]
        }
        ClaudeOutput::System(system) if system.subtype.as_str() == "api_retry" => {
            let retry: claude_codes::ApiRetryMessage = serde_json::from_value(system.data.clone())
                .map_err(|error| {
                    ClaudeSessionError::Protocol(format!("invalid system/api_retry: {error}"))
                })?;
            vec![ClaudeSessionEvent::ApiRetry {
                attempt: retry.attempt as u32,
                delay_ms: retry.retry_delay_ms,
                error: retry.error,
            }]
        }
        _ => Vec::new(),
    };
    Ok(mapped)
}

fn limit(name: &str, utilization: f64, reset: u64) -> ProviderRateLimit {
    ProviderRateLimit {
        scope: ProviderRateLimitScope::Subscription,
        used_percent: (utilization * 100.0).round().max(0.0) as u32,
        window_minutes: match name {
            "five_hour" => 300,
            "seven_day" => 10080,
            _ => 0,
        },
        resets_at_epoch_sec: reset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits(line: &str) -> Vec<ProviderRateLimit> {
        let (output, _) = super::super::wire::decode(line).unwrap();
        let mut events = map(&output).unwrap();
        assert_eq!(events.len(), 1);
        match events.remove(0) {
            ClaudeSessionEvent::RateLimits(limits) => limits,
            _ => panic!("rate limits"),
        }
    }
    #[test]
    fn unified_windows() {
        let line = include_str!("../../tests/fixtures/claude-subscription/happy.stdout.jsonl")
            .lines()
            .find(|line| line.contains("\"type\": \"rate_limit_event\""))
            .unwrap();
        let windows = limits(line);
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().any(|window| window.window_minutes == 300));
        assert!(windows.iter().any(|window| window.window_minutes == 10080));
    }
    #[test]
    fn fallback_and_empty_windows() {
        let fallback = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","utilization":0.33,"resetsAt":123},"session_id":"s"}"#;
        assert_eq!(limits(fallback)[0].used_percent, 33);
        let empty = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed"},"session_id":"s"}"#;
        assert!(limits(empty).is_empty());
    }
}
