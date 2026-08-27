// @dive-file: Loads managed guest-wide execution environment defaults for portproxy child processes.
// @dive-rel: Lets bootstrap-installed proxy settings apply to exec, daemon, and interactive shell flows despite env_clear.
// @dive-rel: Request-provided env vars still override ordinary defaults, but managed proxy keys stay authoritative.
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use std::sync::OnceLock;

const DEFAULT_PROXY_ENV_PATH: &str = "/etc/chevalier/proxy.env";
#[cfg(target_os = "macos")]
pub const DEFAULT_EXEC_PATH: &str =
    "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";
#[cfg(not(target_os = "macos"))]
pub const DEFAULT_EXEC_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
#[cfg(target_os = "macos")]
static DEFAULT_EXEC_HOME: OnceLock<String> = OnceLock::new();
const MANAGED_PROXY_ENV_KEYS: &[&str] = &[
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "all_proxy",
    "ALL_PROXY",
    "no_proxy",
    "NO_PROXY",
];

pub fn build_exec_env(
    default_path: &str,
    default_home: &str,
    overrides: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = HashMap::new();
    merged.insert("PATH".to_string(), default_path.to_string());
    merged.insert("HOME".to_string(), default_home.to_string());
    if let Ok(user) = env::var("CHEVALIER_PORTPROXY_EXEC_USER") {
        let user = user.trim();
        if !user.is_empty() {
            merged.insert("USER".to_string(), user.to_string());
            merged.insert("LOGNAME".to_string(), user.to_string());
        }
    }
    let managed_proxy_env = read_managed_proxy_env();
    merged.extend(overrides.clone());
    for key in MANAGED_PROXY_ENV_KEYS {
        if let Some(value) = managed_proxy_env.get(*key) {
            merged.insert((*key).to_string(), value.clone());
        }
    }
    for (key, value) in managed_proxy_env {
        merged.entry(key).or_insert(value);
    }
    merged
}

pub fn default_exec_home() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        DEFAULT_EXEC_HOME
            .get_or_init(|| {
                env::var("HOME")
                    .ok()
                    .filter(|home| home.starts_with('/'))
                    .unwrap_or_else(|| "/var/root".to_string())
            })
            .as_str()
    }
    #[cfg(not(target_os = "macos"))]
    {
        "/root"
    }
}

fn read_managed_proxy_env() -> HashMap<String, String> {
    let path = managed_proxy_env_path();
    let Ok(contents) = fs::read_to_string(&path) else {
        return HashMap::new();
    };

    contents
        .lines()
        .filter_map(parse_env_line)
        .collect::<HashMap<_, _>>()
}

fn managed_proxy_env_path() -> PathBuf {
    env::var("CHEVALIER_PORTPROXY_MANAGED_ENV_FILE")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_PROXY_ENV_PATH))
}

fn parse_env_line(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let (key, value) = trimmed.split_once('=')?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    Some((key.to_string(), value.trim().to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::{LazyLock, Mutex};

    use super::*;

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn platform_exec_defaults_match_guest_service_account() {
        #[cfg(target_os = "macos")]
        {
            assert!(default_exec_home().starts_with('/'));
            assert!(DEFAULT_EXEC_PATH.starts_with("/opt/homebrew/bin:"));
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(default_exec_home(), "/root");
            assert!(DEFAULT_EXEC_PATH.starts_with("/usr/local/sbin:"));
        }
    }

    #[test]
    fn build_exec_env_merges_managed_proxy_env_and_request_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let env_file = tmp.path().join("proxy.env");
        fs::write(
            &env_file,
            "http_proxy=http://10.0.2.100:3128\nHTTPS_PROXY=http://10.0.2.100:3128\n# comment\n",
        )
        .expect("write proxy env");

        unsafe {
            env::set_var(
                "CHEVALIER_PORTPROXY_MANAGED_ENV_FILE",
                env_file.to_string_lossy().to_string(),
            );
        }

        let overrides = HashMap::from([
            ("PATH".to_string(), "/custom/bin".to_string()),
            ("CUSTOM_FLAG".to_string(), "1".to_string()),
        ]);
        let merged = build_exec_env("/usr/bin", "/root", &overrides);

        assert_eq!(merged.get("PATH").map(String::as_str), Some("/custom/bin"));
        assert_eq!(merged.get("HOME").map(String::as_str), Some("/root"));
        assert_eq!(
            merged.get("http_proxy").map(String::as_str),
            Some("http://10.0.2.100:3128")
        );
        assert_eq!(
            merged.get("HTTPS_PROXY").map(String::as_str),
            Some("http://10.0.2.100:3128")
        );
        assert_eq!(merged.get("CUSTOM_FLAG").map(String::as_str), Some("1"));

        unsafe {
            env::remove_var("CHEVALIER_PORTPROXY_MANAGED_ENV_FILE");
        }
    }

    #[test]
    fn build_exec_env_reports_the_configured_guest_user() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        unsafe {
            env::set_var("CHEVALIER_PORTPROXY_EXEC_USER", "openbracket");
        }

        let merged = build_exec_env("/usr/bin", "/Users/openbracket", &HashMap::new());

        assert_eq!(merged.get("USER").map(String::as_str), Some("openbracket"));
        assert_eq!(
            merged.get("LOGNAME").map(String::as_str),
            Some("openbracket")
        );

        unsafe {
            env::remove_var("CHEVALIER_PORTPROXY_EXEC_USER");
        }
    }

    #[test]
    fn build_exec_env_keeps_managed_proxy_keys_authoritative() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let tmp = tempfile::tempdir().expect("tempdir");
        let env_file = tmp.path().join("proxy.env");
        fs::write(
            &env_file,
            "http_proxy=http://10.0.2.100:3128\nNO_PROXY=localhost,127.0.0.1\nCUSTOM_MANAGED=1\n",
        )
        .expect("write proxy env");

        unsafe {
            env::set_var(
                "CHEVALIER_PORTPROXY_MANAGED_ENV_FILE",
                env_file.to_string_lossy().to_string(),
            );
        }

        let overrides = HashMap::from([
            ("http_proxy".to_string(), String::new()),
            ("NO_PROXY".to_string(), "*".to_string()),
            ("CUSTOM_MANAGED".to_string(), "override-ok".to_string()),
        ]);
        let merged = build_exec_env("/usr/bin", "/root", &overrides);

        assert_eq!(
            merged.get("http_proxy").map(String::as_str),
            Some("http://10.0.2.100:3128")
        );
        assert_eq!(
            merged.get("NO_PROXY").map(String::as_str),
            Some("localhost,127.0.0.1")
        );
        assert_eq!(
            merged.get("CUSTOM_MANAGED").map(String::as_str),
            Some("override-ok")
        );

        unsafe {
            env::remove_var("CHEVALIER_PORTPROXY_MANAGED_ENV_FILE");
        }
    }
}
