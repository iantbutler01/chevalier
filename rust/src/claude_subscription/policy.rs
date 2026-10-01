use super::{ClaudeSessionConfig, ClaudeSessionError, ClaudeSubscriptionStatus};
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::process::Command;

const MIN_VERSION: (u32, u32, u32) = (2, 1, 281);

pub(crate) fn spawn_env(client_app: &str) -> HashMap<String, String> {
    filter_env(env::vars(), client_app)
}

fn filter_env(
    source: impl IntoIterator<Item = (String, String)>,
    client_app: &str,
) -> HashMap<String, String> {
    let mut vars: HashMap<String, String> = source
        .into_iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "ANTHROPIC_API_KEY"
                    | "ANTHROPIC_AUTH_TOKEN"
                    | "ANTHROPIC_BASE_URL"
                    | "CLAUDECODE"
                    | "CLAUDE_PID"
                    | "CLAUDE_EFFORT"
                    | "CLAUDE_AGENT_SDK_VERSION"
            ) && !key.starts_with("CLAUDE_CODE_")
        })
        .collect();
    vars.insert("CLAUDE_AGENT_SDK_CLIENT_APP".into(), client_app.into());
    vars.insert("CLAUDE_CODE_DISABLE_AUTO_MEMORY".into(), "1".into());
    vars.insert("CLAUDE_CODE_DISABLE_TERMINAL_TITLE".into(), "1".into());
    vars
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ClaudeSessionConfig {
        ClaudeSessionConfig {
            server_name: "ob".into(),
            ..Default::default()
        }
    }
    #[test]
    fn rule_1_official_binary_owns_auth_and_api() {
        let env = filter_env(
            [
                ("ANTHROPIC_API_KEY".into(), "secret".into()),
                ("ANTHROPIC_AUTH_TOKEN".into(), "secret".into()),
            ],
            "test",
        );
        assert!(!env.contains_key("ANTHROPIC_API_KEY"));
        assert!(!env.contains_key("ANTHROPIC_AUTH_TOKEN"));
    }
    #[test]
    fn rule_2_login_is_not_intermediated() {
        let env = filter_env(
            [("CLAUDE_CODE_OAUTH_TOKEN".into(), "secret".into())],
            "test",
        );
        assert!(!env.contains_key("CLAUDE_CODE_OAUTH_TOKEN"));
    }
    #[test]
    fn rule_3_login_stays_on_machine() {
        let env = filter_env(
            [
                ("HOME".into(), "/home/user".into()),
                ("PATH".into(), "/usr/bin".into()),
            ],
            "test",
        );
        assert_eq!(env["HOME"], "/home/user");
        assert_eq!(env["PATH"], "/usr/bin");
    }
    #[test]
    fn rule_4_documented_lockdown_flags() {
        let args = argv(&config());
        assert!(args.windows(2).any(|w| w == ["--tools", ""]));
        for flag in [
            "--setting-sources=",
            "--strict-mcp-config",
            "--allowedTools",
        ] {
            assert!(args.contains(&flag.to_string()));
        }
        assert!(!args.iter().any(|arg| arg.contains("dangerously")));
    }
    #[test]
    fn rule_5_paid_routes_are_removed() {
        let env = filter_env(
            [
                ("ANTHROPIC_BASE_URL".into(), "x".into()),
                ("CLAUDE_CODE_USE_BEDROCK".into(), "1".into()),
                ("CLAUDE_CODE_USE_VERTEX".into(), "1".into()),
                ("CLAUDE_CODE_USE_FOUNDRY".into(), "1".into()),
            ],
            "test",
        );
        assert!(env.keys().all(|key| !key.starts_with("ANTHROPIC")
            && !key.contains("BEDROCK")
            && !key.contains("VERTEX")
            && !key.contains("FOUNDRY")));
    }
    #[test]
    fn rule_6_honest_client_identity() {
        let env = filter_env(
            [("CLAUDE_AGENT_SDK_VERSION".into(), "fake".into())],
            "openbracket",
        );
        assert_eq!(env["CLAUDE_AGENT_SDK_CLIENT_APP"], "openbracket");
        assert!(!env.contains_key("CLAUDE_AGENT_SDK_VERSION"));
    }
    #[test]
    fn rule_7_branding() {
        assert_eq!(
            super::super::CLAUDE_SUBSCRIPTION_LABEL,
            "Claude (subscription)"
        );
    }
    #[test]
    fn rule_8_cli_is_treated_as_black_box() {
        let path = std::env::temp_dir().join(format!("claude-blackbox-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, [0, 255, 1]).unwrap();
        assert_eq!(resolve_cli(Some(&path)).unwrap(), path);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cli_resolution_order_and_missing_override() {
        let root = std::env::temp_dir().join(format!("claude-policy-{}", uuid::Uuid::new_v4()));
        let path_dir = root.join("path");
        let home = root.join("home");
        std::fs::create_dir_all(&path_dir).unwrap();
        std::fs::create_dir_all(home.join(".local/bin")).unwrap();
        let path_cli = path_dir.join("claude");
        let home_cli = home.join(".local/bin/claude");
        std::fs::write(&path_cli, "").unwrap();
        std::fs::write(&home_cli, "").unwrap();
        let path_var = path_dir.as_os_str();
        assert_eq!(
            resolve_cli_from(None, None, Some(path_var), Some(home.as_os_str())).unwrap(),
            path_cli
        );
        assert_eq!(
            resolve_cli_from(None, Some(&home_cli), Some(path_var), None).unwrap(),
            home_cli
        );
        assert!(matches!(
            resolve_cli_from(
                Some(&root.join("missing")),
                Some(&path_cli),
                Some(path_var),
                None
            ),
            Err(ClaudeSessionError::CliNotFound { .. })
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn old_cli_is_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("claude-old-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, "#!/bin/sh\necho '2.1.280 (Claude Code)'\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(
            version(&path).await,
            Err(ClaudeSessionError::CliTooOld { .. })
        ));
        std::fs::remove_file(path).unwrap();
    }
}

pub(crate) fn resolve_cli(explicit: Option<&Path>) -> Result<PathBuf, ClaudeSessionError> {
    let override_path = env::var_os("CHEVALIER_CLAUDE_BIN").map(PathBuf::from);
    resolve_cli_from(
        explicit,
        override_path.as_deref(),
        env::var_os("PATH").as_deref(),
        env::var_os("HOME").as_deref(),
    )
}

fn resolve_cli_from(
    explicit: Option<&Path>,
    override_path: Option<&Path>,
    path_var: Option<&std::ffi::OsStr>,
    home_var: Option<&std::ffi::OsStr>,
) -> Result<PathBuf, ClaudeSessionError> {
    if let Some(path) = explicit.or(override_path) {
        return if path.is_file() {
            Ok(path.to_path_buf())
        } else {
            Err(ClaudeSessionError::CliNotFound {
                probed: vec![path.to_path_buf()],
            })
        };
    }
    let mut probed = Vec::new();
    if let Some(path) = path_var {
        for dir in env::split_paths(path) {
            let candidate = dir.join("claude");
            if candidate.is_file() {
                return Ok(candidate);
            }
            probed.push(candidate);
        }
    }
    if let Some(home) = home_var {
        let home = PathBuf::from(home);
        for suffix in [".local/bin/claude", ".claude/local/claude"] {
            let candidate = home.join(suffix);
            if candidate.is_file() {
                return Ok(candidate);
            }
            probed.push(candidate);
        }
    }
    for candidate in ["/opt/homebrew/bin/claude", "/usr/local/bin/claude"] {
        let candidate = PathBuf::from(candidate);
        if candidate.is_file() {
            return Ok(candidate);
        }
        probed.push(candidate);
    }
    Err(ClaudeSessionError::CliNotFound { probed })
}

pub(crate) fn argv(config: &ClaudeSessionConfig) -> Vec<String> {
    let mut args = vec![
        "--output-format",
        "stream-json",
        "--verbose",
        "--input-format",
        "stream-json",
        "--include-partial-messages",
        "--model",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    args.push(
        config
            .model
            .strip_prefix("claude-subscription:")
            .unwrap_or(&config.model)
            .split('@')
            .next()
            .unwrap_or(&config.model)
            .into(),
    );
    if let Some(effort) = config.effort {
        args.extend(["--effort".into(), effort.as_str().into()]);
    }
    if let Some(turns) = config.max_turns {
        args.extend(["--max-turns".into(), turns.to_string()]);
    }
    if let Some(resume) = &config.resume {
        args.push(format!("--resume={}", resume.session_id));
    }
    args.extend([
        "--tools".into(),
        "".into(),
        "--allowedTools".into(),
        format!("mcp__{}", config.server_name),
        "--setting-sources=".into(),
        "--strict-mcp-config".into(),
        "--permission-mode".into(),
        "default".into(),
    ]);
    args
}

pub(crate) async fn version(cli: &Path) -> Result<String, ClaudeSessionError> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, String>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(version) = cache.lock().unwrap().get(cli).cloned() {
        return Ok(version);
    }
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(cli)
            .arg("--version")
            .env_clear()
            .envs(spawn_env("chevalier"))
            .output(),
    )
    .await
    .map_err(|_| ClaudeSessionError::Protocol("claude --version timed out".into()))?
    .map_err(|e| ClaudeSessionError::Protocol(e.to_string()))?;
    let found = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let numeric = found.split_whitespace().next().unwrap_or("");
    let parts = numeric
        .split('.')
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>();
    match parts {
        Ok(parts) if parts.len() == 3 && (parts[0], parts[1], parts[2]) >= MIN_VERSION => {}
        _ => {
            return Err(ClaudeSessionError::CliTooOld {
                found,
                required: "2.1.281".into(),
            });
        }
    }
    cache
        .lock()
        .unwrap()
        .insert(cli.to_path_buf(), found.clone());
    Ok(found)
}

pub async fn claude_subscription_status(cli_path: Option<&Path>) -> ClaudeSubscriptionStatus {
    let cli = match resolve_cli(cli_path) {
        Ok(cli) => cli,
        Err(ClaudeSessionError::CliNotFound { probed }) => {
            return ClaudeSubscriptionStatus::CliNotFound { probed };
        }
        Err(error) => {
            return ClaudeSubscriptionStatus::Error {
                message: error.to_string(),
            };
        }
    };
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(cli)
            .args(["auth", "status", "--json"])
            .env_clear()
            .envs(spawn_env("chevalier"))
            .stdin(Stdio::null())
            .output(),
    )
    .await;
    let output = match output {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            return ClaudeSubscriptionStatus::Error {
                message: e.to_string(),
            };
        }
        Err(_) => {
            return ClaudeSubscriptionStatus::Error {
                message: "auth status timed out".into(),
            };
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(e) => {
            return ClaudeSubscriptionStatus::Error {
                message: e.to_string(),
            };
        }
    };
    if value["loggedIn"] != true {
        return ClaudeSubscriptionStatus::NotLoggedIn;
    }
    // A Console (API key) or third-party-provider login bills the API, not the subscription.
    if value["authMethod"] != "claude.ai" || value["apiProvider"] != "firstParty" {
        return ClaudeSubscriptionStatus::Error {
            message: format!(
                "claude is logged in with {} via {}, not a Claude subscription; run `claude auth login` with your Claude account",
                value["authMethod"].as_str().unwrap_or("an unknown method"),
                value["apiProvider"]
                    .as_str()
                    .unwrap_or("an unknown provider"),
            ),
        };
    }
    ClaudeSubscriptionStatus::Ready {
        email: value["email"].as_str().map(str::to_string),
        subscription_type: value["subscriptionType"].as_str().map(str::to_string),
    }
}
