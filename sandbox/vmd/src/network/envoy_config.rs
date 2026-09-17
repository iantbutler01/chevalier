// @dive-file: Structured Envoy bootstrap config builder for guest egress policy.
// @dive-rel: Keeps Envoy JSON generation separate from network service supervision.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;

use serde_json::{Map, Value, json};

use super::{FALLBACK_THREAT_HOSTS, VmProxyListener, VmProxyPolicyConfig};

const ENVOY_CLUSTER_NAME: &str = "dynamic_forward_proxy_cluster";
const ENVOY_ORIGINAL_DST_CLUSTER_NAME: &str = "original_dst_cluster";
const ENVOY_LAN_CLUSTER_NAME: &str = "original_dst_lan_cluster";
const ENVOY_DNS_CACHE_NAME: &str = "dynamic_forward_proxy_cache_config";

pub(super) fn render(
    default_listen_addr: Option<SocketAddr>,
    vm_proxy_policies: &BTreeMap<String, VmProxyListener>,
    admin_addr: SocketAddr,
    dns_resolver_addr: SocketAddr,
    access_log_path: &Path,
) -> String {
    Builder {
        dns_resolver_addr,
        access_log_path: access_log_path.display().to_string(),
    }
    .render(default_listen_addr, vm_proxy_policies, admin_addr)
}

struct Builder {
    dns_resolver_addr: SocketAddr,
    access_log_path: String,
}

impl Builder {
    fn render(
        &self,
        default_listen_addr: Option<SocketAddr>,
        vm_proxy_policies: &BTreeMap<String, VmProxyListener>,
        admin_addr: SocketAddr,
    ) -> String {
        let mut listeners = Vec::new();
        if let Some(listen_addr) = default_listen_addr {
            listeners.push(self.http_proxy_listener(
                "listener_http_proxy",
                listen_addr,
                None,
                None,
                None,
            ));
        }
        for (vm_id, listener) in vm_proxy_policies {
            listeners.push(self.transparent_tcp_listener(
                format!("listener_vm_{}", sanitize_listener_name(vm_id)).as_str(),
                listener.listen_addr,
                Some(&listener.policy),
                vm_id.as_str(),
            ));
        }

        let config = json!({
            "admin": {
                "access_log_path": self.access_log_path,
                "address": socket_address(admin_addr),
            },
            "static_resources": {
                "listeners": listeners,
                "clusters": [
                    self.dynamic_forward_proxy_cluster(),
                    original_dst_cluster(),
                    lan_original_dst_cluster(),
                ],
            },
        });
        serde_json::to_string_pretty(&config).expect("serialize envoy config")
    }

    fn dynamic_forward_proxy_cluster(&self) -> Value {
        json!({
            "name": ENVOY_CLUSTER_NAME,
            "connect_timeout": "10s",
            "lb_policy": "CLUSTER_PROVIDED",
            "cluster_type": {
                "name": "envoy.clusters.dynamic_forward_proxy",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.clusters.dynamic_forward_proxy.v3.ClusterConfig",
                    "dns_cache_config": self.dns_cache_config(),
                },
            },
        })
    }

    fn dns_cache_config(&self) -> Value {
        json!({
            "name": ENVOY_DNS_CACHE_NAME,
            "dns_lookup_family": "V4_ONLY",
            "typed_dns_resolver_config": {
                "name": "envoy.network.dns_resolver.cares",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.network.dns_resolver.cares.v3.CaresDnsResolverConfig",
                    "resolvers": [
                        socket_address(self.dns_resolver_addr),
                    ],
                    "dns_resolver_options": {
                        "use_tcp_for_dns_lookups": true,
                        "no_default_search_domain": true,
                    },
                },
            },
        })
    }

    fn transparent_tcp_listener(
        &self,
        name: &str,
        listen_addr: SocketAddr,
        policy: Option<&VmProxyPolicyConfig>,
        vm_id: &str,
    ) -> Value {
        let mut filters = tcp_policy_filters(name, policy);
        filters.push(tcp_proxy_filter(
            name,
            if policy.is_some_and(|policy| policy.allow_lan) {
                ENVOY_LAN_CLUSTER_NAME
            } else {
                ENVOY_ORIGINAL_DST_CLUSTER_NAME
            },
            tcp_access_log_format(name, policy, vm_id),
            self.access_log_path.as_str(),
        ));

        json!({
            "name": name,
            "transparent": true,
            "address": socket_address(listen_addr),
            "listener_filters": [
                {
                    "name": "envoy.filters.listener.original_dst",
                    "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.listener.original_dst.v3.OriginalDst",
                    },
                },
                {
                    "name": "envoy.filters.listener.tls_inspector",
                    "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector",
                    },
                },
            ],
            "filter_chains": [
                {
                    "filters": filters,
                },
            ],
        })
    }

    fn http_proxy_listener(
        &self,
        name: &str,
        listen_addr: SocketAddr,
        domain_blocklist: Option<&[String]>,
        policy: Option<&VmProxyPolicyConfig>,
        vm_id: Option<&str>,
    ) -> Value {
        json!({
            "name": name,
            "address": socket_address(listen_addr),
            "filter_chains": [
                {
                    "filters": [
                        {
                            "name": "envoy.filters.network.http_connection_manager",
                            "typed_config": {
                                "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                                "stat_prefix": name,
                                "access_log": [
                                    file_access_log(
                                        self.access_log_path.as_str(),
                                        http_access_log_format(name, policy, vm_id),
                                    ),
                                ],
                                "route_config": {
                                    "name": format!("{name}_route"),
                                    "virtual_hosts": [
                                        {
                                            "name": format!("{name}_proxy"),
                                            "domains": ["*"],
                                            "routes": http_proxy_routes(domain_blocklist, policy),
                                        },
                                    ],
                                },
                                "http_filters": [
                                    {
                                        "name": "envoy.filters.http.dynamic_forward_proxy",
                                        "typed_config": {
                                            "@type": "type.googleapis.com/envoy.extensions.filters.http.dynamic_forward_proxy.v3.FilterConfig",
                                            "dns_cache_config": self.dns_cache_config(),
                                        },
                                    },
                                    {
                                        "name": "envoy.filters.http.router",
                                        "typed_config": {
                                            "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        })
    }
}

fn socket_address(addr: SocketAddr) -> Value {
    json!({
        "socket_address": {
            "address": addr.ip().to_string(),
            "port_value": addr.port(),
        },
    })
}

fn original_dst_cluster() -> Value {
    json!({
        "name": ENVOY_ORIGINAL_DST_CLUSTER_NAME,
        "connect_timeout": "10s",
        "type": "ORIGINAL_DST",
        "lb_policy": "CLUSTER_PROVIDED",
    })
}

fn lan_original_dst_cluster() -> Value {
    let mut cluster = original_dst_cluster();
    cluster["name"] = json!(ENVOY_LAN_CLUSTER_NAME);
    cluster["upstream_bind_config"] = json!({ "socket_options": [{
        "description": "Opted-in VM LAN egress",
        "level": 1, "name": 36, "int_value": super::firewall::LAN_EGRESS_MARK,
        "state": "STATE_PREBIND"
    }] });
    cluster
}

fn tcp_policy_filters(name: &str, policy: Option<&VmProxyPolicyConfig>) -> Vec<Value> {
    let mut filters = vec![tcp_system_deny_filter(
        name,
        policy.is_some_and(|policy| policy.allow_lan),
    )];
    if let Some(policy) = policy {
        if !policy.domain_blocklist.is_empty() {
            filters.push(tcp_sni_deny_filter(
                format!("{name}_runtime_deny").as_str(),
                "runtime_domain_blocklist",
                sni_domain_regex(&policy.domain_blocklist).as_str(),
            ));
        }
        if policy.domain_allowlist.is_some() {
            filters.push(tcp_sni_allow_filter(
                format!("{name}_runtime_allow").as_str(),
                "runtime_domain_allowlist",
                sni_domain_regex(policy.domain_allowlist.as_deref().unwrap_or_default()).as_str(),
                &policy_allowed_ports(policy),
            ));
        }
    }
    filters
}

fn tcp_system_deny_filter(name: &str, allow_lan: bool) -> Value {
    let mut permissions = Vec::new();
    for (address_prefix, prefix_len) in [
        ("10.0.0.0", 8_u8),
        ("172.16.0.0", 12),
        ("192.168.0.0", 16),
        ("100.64.0.0", 10),
        ("127.0.0.0", 8),
        ("169.254.0.0", 16),
        ("198.18.0.0", 15),
    ] {
        if !allow_lan
            || !matches!(
                address_prefix,
                "10.0.0.0" | "172.16.0.0" | "192.168.0.0" | "100.64.0.0"
            )
        {
            permissions.push(rbac_destination_ip_permission(address_prefix, prefix_len));
        }
    }
    for port in [25_u16, 465, 587, 6667, 6697] {
        permissions.push(rbac_destination_port_permission(port));
    }
    permissions.extend(rbac_requested_server_name_host_permissions(
        FALLBACK_THREAT_HOSTS,
    ));
    network_rbac_filter(
        format!("{name}_system_deny").as_str(),
        "DENY",
        "runtime_system_denied_destination",
        permissions,
    )
}

fn tcp_sni_deny_filter(name: &str, policy_name: &str, sni_regex: &str) -> Value {
    network_rbac_filter(
        name,
        "DENY",
        policy_name,
        vec![rbac_requested_server_name_permission(sni_regex)],
    )
}

fn tcp_sni_allow_filter(
    name: &str,
    policy_name: &str,
    sni_regex: &str,
    allowed_ports: &[u16],
) -> Value {
    let port_rules = allowed_ports
        .iter()
        .copied()
        .map(rbac_destination_port_permission)
        .collect::<Vec<_>>();
    let permissions = vec![json!({
        "and_rules": {
            "rules": [
                rbac_requested_server_name_permission(sni_regex),
                {
                    "or_rules": {
                        "rules": port_rules,
                    },
                },
            ],
        },
    })];
    network_rbac_filter(name, "ALLOW", policy_name, permissions)
}

fn network_rbac_filter(
    stat_prefix: &str,
    action: &str,
    policy_name: &str,
    permissions: Vec<Value>,
) -> Value {
    let mut policies = Map::new();
    policies.insert(
        policy_name.to_string(),
        json!({
            "permissions": permissions,
            "principals": [
                {
                    "any": true,
                },
            ],
        }),
    );

    json!({
        "name": "envoy.filters.network.rbac",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.filters.network.rbac.v3.RBAC",
            "stat_prefix": stat_prefix,
            "rules": {
                "action": action,
                "policies": Value::Object(policies),
            },
        },
    })
}

fn rbac_requested_server_name_permission(regex: &str) -> Value {
    json!({
        "requested_server_name": {
            "safe_regex": {
                "regex": regex,
            },
        },
    })
}

fn rbac_requested_server_name_host_permissions(hosts: &[&str]) -> Vec<Value> {
    hosts
        .iter()
        .flat_map(|host| {
            let mut permissions = vec![rbac_requested_server_name_match_permission("exact", host)];
            if host.parse::<std::net::IpAddr>().is_err() {
                let suffix = format!(".{host}");
                permissions.push(rbac_requested_server_name_match_permission(
                    "suffix",
                    suffix.as_str(),
                ));
            }
            permissions
        })
        .collect()
}

fn rbac_requested_server_name_match_permission(kind: &str, value: &str) -> Value {
    let mut matcher = Map::new();
    matcher.insert(kind.to_string(), json!(value));
    matcher.insert("ignore_case".to_string(), json!(true));
    json!({
        "requested_server_name": Value::Object(matcher),
    })
}

fn rbac_destination_ip_permission(address_prefix: &str, prefix_len: u8) -> Value {
    json!({
        "destination_ip": {
            "address_prefix": address_prefix,
            "prefix_len": prefix_len,
        },
    })
}

fn rbac_destination_port_permission(port: u16) -> Value {
    json!({
        "destination_port": port,
    })
}

fn policy_allowed_ports(policy: &VmProxyPolicyConfig) -> Vec<u16> {
    let mut allowed_ports = vec![80_u16, 443];
    allowed_ports.extend(policy.custom_port_allowlist.iter().copied());
    allowed_ports.sort_unstable();
    allowed_ports.dedup();
    allowed_ports
}

fn sni_domain_regex(domains: &[String]) -> String {
    if domains.is_empty() {
        return String::from("__chevalier_no_hosts_allowed__");
    }
    let hosts = domains
        .iter()
        .map(|domain| format!(r"(.*\.)?{}", escape_regex_literal(domain)))
        .collect::<Vec<_>>()
        .join("|");
    format!(r"(?i)^({hosts})$")
}

fn tcp_proxy_filter(
    stat_prefix: &str,
    cluster: &str,
    log_format: Value,
    access_log_path: &str,
) -> Value {
    json!({
        "name": "envoy.filters.network.tcp_proxy",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy",
            "stat_prefix": stat_prefix,
            "cluster": cluster,
            "access_log": [
                file_access_log(access_log_path, log_format),
            ],
        },
    })
}

fn file_access_log(access_log_path: &str, log_format: Value) -> Value {
    json!({
        "name": "envoy.access_loggers.file",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog",
            "path": access_log_path,
            "log_format": {
                "json_format": log_format,
            },
        },
    })
}

fn http_access_log_format(
    name: &str,
    policy: Option<&VmProxyPolicyConfig>,
    vm_id: Option<&str>,
) -> Value {
    json!({
        "timestamp": "%START_TIME(%Y-%m-%dT%H:%M:%S.%3fZ)%",
        "vm_id": vm_id.unwrap_or_default(),
        "owner_id": policy.and_then(|value| value.owner_id.as_deref()).unwrap_or_default(),
        "listener": name,
        "authority": "%REQ(:AUTHORITY)%",
        "method": "%REQ(:METHOD)%",
        "path": "%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%",
        "response_code": "%RESPONSE_CODE%",
        "response_code_details": "%RESPONSE_CODE_DETAILS%",
        "bytes_received": "%BYTES_RECEIVED%",
        "bytes_sent": "%BYTES_SENT%",
        "requested_server_name": "%REQUESTED_SERVER_NAME%",
        "upstream_host": "%UPSTREAM_HOST%",
    })
}

fn tcp_access_log_format(name: &str, policy: Option<&VmProxyPolicyConfig>, vm_id: &str) -> Value {
    json!({
        "timestamp": "%START_TIME(%Y-%m-%dT%H:%M:%S.%3fZ)%",
        "vm_id": vm_id,
        "owner_id": policy.and_then(|value| value.owner_id.as_deref()).unwrap_or_default(),
        "listener": name,
        "authority": "%REQUESTED_SERVER_NAME%",
        "method": "TCP",
        "path": "",
        "response_code": "200",
        "response_code_details": "%RESPONSE_FLAGS%",
        "bytes_received": "%BYTES_RECEIVED%",
        "bytes_sent": "%BYTES_SENT%",
        "requested_server_name": "%REQUESTED_SERVER_NAME%",
        "upstream_host": "%UPSTREAM_HOST%",
    })
}

fn http_proxy_routes(
    domain_blocklist: Option<&[String]>,
    policy: Option<&VmProxyPolicyConfig>,
) -> Vec<Value> {
    let mut routes = system_block_routes();
    if let Some(additional_block_regex) = domain_blocklist
        .filter(|entries| !entries.is_empty())
        .map(|entries| domain_list_regex(entries, None))
    {
        routes.extend(regex_deny_routes(additional_block_regex.as_str()));
    }

    let allow_regex = policy.map(|policy| {
        let allowed_ports = policy_allowed_ports(policy);
        authority_allow_regex(policy.domain_allowlist.as_deref(), &allowed_ports)
    });
    if let Some(allow_regex) = allow_regex {
        routes.push(connect_route(
            Some(authority_regex_header(allow_regex.as_str())),
            ENVOY_CLUSTER_NAME,
        ));
        routes.push(prefix_route(
            Some(authority_regex_header(allow_regex.as_str())),
            ENVOY_CLUSTER_NAME,
        ));
        routes.push(connect_direct_response_route(None, 403));
        routes.push(prefix_direct_response_route(None, 403));
    } else {
        routes.push(connect_route(None, ENVOY_CLUSTER_NAME));
        routes.push(prefix_route(None, ENVOY_CLUSTER_NAME));
    }
    routes
}

fn system_block_routes() -> Vec<Value> {
    [
        r"(?i)^10\..*",
        r"(?i)^172\.(1[6-9]|2[0-9]|3[0-1])\..*",
        r"(?i)^192\.168\..*",
        r"(?i)^100\.(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-7])\..*",
        r"(?i)^(.*\.)?svc\.cluster\.local(?::\d+)?$",
        r"(?i)^(dns\.google|cloudflare-dns\.com|dns\.quad9\.net|one\.one\.one\.one)(?::\d+)?$",
        r"(?i)^.*:(25|465|587|6667|6697)$",
    ]
    .into_iter()
    .flat_map(regex_deny_routes)
    .collect()
}

fn regex_deny_routes(regex: &str) -> Vec<Value> {
    let header = authority_regex_header(regex);
    vec![
        connect_direct_response_route(Some(header.clone()), 403),
        prefix_direct_response_route(Some(header), 403),
    ]
}

fn authority_regex_header(regex: &str) -> Value {
    json!({
        "name": ":authority",
        "string_match": {
            "safe_regex": {
                "regex": regex,
            },
        },
    })
}

fn connect_route(header: Option<Value>, cluster: &str) -> Value {
    json!({
        "match": connect_match(header),
        "route": {
            "cluster": cluster,
            "upgrade_configs": [
                {
                    "upgrade_type": "CONNECT",
                    "connect_config": {},
                },
            ],
        },
    })
}

fn prefix_route(header: Option<Value>, cluster: &str) -> Value {
    json!({
        "match": prefix_match(header),
        "route": {
            "cluster": cluster,
            "timeout": "0s",
        },
    })
}

fn connect_direct_response_route(header: Option<Value>, status: u16) -> Value {
    json!({
        "match": connect_match(header),
        "direct_response": {
            "status": status,
        },
    })
}

fn prefix_direct_response_route(header: Option<Value>, status: u16) -> Value {
    json!({
        "match": prefix_match(header),
        "direct_response": {
            "status": status,
        },
    })
}

fn connect_match(header: Option<Value>) -> Value {
    let mut value = Map::new();
    value.insert("connect_matcher".to_string(), json!({}));
    if let Some(header) = header {
        value.insert("headers".to_string(), Value::Array(vec![header]));
    }
    Value::Object(value)
}

fn prefix_match(header: Option<Value>) -> Value {
    let mut value = Map::new();
    value.insert("prefix".to_string(), json!("/"));
    if let Some(header) = header {
        value.insert("headers".to_string(), Value::Array(vec![header]));
    }
    Value::Object(value)
}

fn authority_allow_regex(domain_allowlist: Option<&[String]>, allowed_ports: &[u16]) -> String {
    let host_regex = match domain_allowlist {
        Some(domains) => {
            if domains.is_empty() {
                String::from("__chevalier_no_hosts_allowed__")
            } else {
                domains
                    .iter()
                    .map(|domain| format!(r"(.*\.)?{}", escape_regex_literal(domain)))
                    .collect::<Vec<_>>()
                    .join("|")
            }
        }
        _ => String::from(r"[^:]+"),
    };
    let ports_regex = allowed_ports
        .iter()
        .map(|port| port.to_string())
        .collect::<Vec<_>>()
        .join("|");
    format!(r"(?i)^({host_regex})(?::({ports_regex}))?$")
}

fn domain_list_regex(domains: &[String], allowed_ports: Option<&[u16]>) -> String {
    let hosts = domains
        .iter()
        .map(|domain| format!(r"(.*\.)?{}", escape_regex_literal(domain)))
        .collect::<Vec<_>>()
        .join("|");
    match allowed_ports {
        Some(ports) if !ports.is_empty() => {
            let ports = ports
                .iter()
                .map(|port| port.to_string())
                .collect::<Vec<_>>()
                .join("|");
            format!(r"(?i)^({hosts})(?::({ports}))?$")
        }
        _ => format!(r"(?i)^({hosts})(?::\d+)?$"),
    }
}

fn escape_regex_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn sanitize_listener_name(vm_id: &str) -> String {
    vm_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use serde_json::Value;

    use super::*;

    fn test_vm_proxy_listener(listen_addr: &str) -> VmProxyListener {
        VmProxyListener {
            listen_addr: listen_addr.parse().expect("listen addr"),
            policy: VmProxyPolicyConfig {
                allow_lan: false,
                dns_servers: Vec::new(),
                owner_id: Some("owner-123".to_string()),
                domain_allowlist: Some(vec!["api.github.com".to_string()]),
                domain_blocklist: vec!["bad.example".to_string()],
                custom_port_allowlist: vec![8080],
                bandwidth_cap_mb_per_hour: 1024,
                max_connections_per_minute: 1000,
            },
        }
    }

    fn parse_envoy_config(rendered: &str) -> Value {
        serde_json::from_str(rendered).expect("rendered envoy config should parse as json")
    }

    fn value_contains_str(value: &Value, needle: &str) -> bool {
        match value {
            Value::String(value) => value.contains(needle),
            Value::Array(values) => values.iter().any(|value| value_contains_str(value, needle)),
            Value::Object(values) => values
                .iter()
                .any(|(key, value)| key.contains(needle) || value_contains_str(value, needle)),
            _ => false,
        }
    }

    fn value_contains_u64(value: &Value, needle: u64) -> bool {
        match value {
            Value::Number(value) => value.as_u64() == Some(needle),
            Value::Array(values) => values.iter().any(|value| value_contains_u64(value, needle)),
            Value::Object(values) => values
                .values()
                .any(|value| value_contains_u64(value, needle)),
            _ => false,
        }
    }

    #[test]
    fn render_includes_listener_admin_and_dns_cache() {
        let rendered = render(
            Some("127.0.0.1:3128".parse().expect("listen addr")),
            &BTreeMap::new(),
            "127.0.0.1:9901".parse().expect("admin addr"),
            "127.0.0.53:53".parse().expect("dns resolver addr"),
            Path::new("/tmp/envoy-access.log"),
        );
        let parsed = parse_envoy_config(&rendered);
        assert_eq!(
            parsed
                .pointer("/static_resources/listeners/0/address/socket_address/port_value")
                .and_then(Value::as_u64),
            Some(3128)
        );
        assert_eq!(
            parsed
                .pointer("/admin/address/socket_address/port_value")
                .and_then(Value::as_u64),
            Some(9901)
        );
        assert_eq!(
            parsed
                .pointer("/static_resources/listeners/0/address/socket_address/address")
                .and_then(Value::as_str),
            Some("127.0.0.1")
        );
        assert_eq!(
            parsed
                .pointer("/static_resources/clusters/0/cluster_type/typed_config/dns_cache_config/typed_dns_resolver_config/typed_config/resolvers/0/socket_address/address")
                .and_then(Value::as_str),
            Some("127.0.0.53")
        );
        assert_eq!(
            parsed
                .pointer("/static_resources/clusters/0/cluster_type/typed_config/dns_cache_config/typed_dns_resolver_config/typed_config/resolvers/0/socket_address/port_value")
                .and_then(Value::as_u64),
            Some(53)
        );
        assert!(value_contains_str(&parsed, "connect_matcher"));
        assert!(value_contains_str(
            &parsed,
            "envoy.filters.http.dynamic_forward_proxy"
        ));
        assert!(value_contains_str(
            &parsed,
            "envoy.clusters.dynamic_forward_proxy"
        ));
        assert!(value_contains_str(&parsed, "/tmp/envoy-access.log"));
        assert!(value_contains_str(&parsed, r"svc\.cluster\.local"));
        assert!(value_contains_str(&parsed, r"cloudflare-dns\.com"));
        assert!(value_contains_str(&parsed, "json_format"));
        assert!(value_contains_str(&parsed, "%REQ(:AUTHORITY)%"));
    }

    #[test]
    fn authority_allow_regex_includes_custom_ports_and_domains() {
        let regex = authority_allow_regex(Some(&["github.com".to_string()]), &[80, 443, 8080]);
        assert!(regex.contains("github\\.com"));
        assert!(regex.contains("8080"));
    }

    #[test]
    fn render_embeds_vm_listener_identity() {
        let mut listeners = BTreeMap::new();
        listeners.insert(
            "vm-123".to_string(),
            test_vm_proxy_listener("127.0.0.1:43128"),
        );
        let rendered = render(
            None,
            &listeners,
            "127.0.0.1:9901".parse().expect("admin addr"),
            "127.0.0.53:53".parse().expect("dns resolver addr"),
            Path::new("/tmp/envoy-access.log"),
        );
        let parsed = parse_envoy_config(&rendered);
        assert!(value_contains_str(&parsed, "vm-123"));
        assert!(value_contains_str(&parsed, "owner-123"));
        assert!(value_contains_str(&parsed, r"bad\.example"));
        assert!(value_contains_str(&parsed, r"api\.github\.com"));
        assert!(value_contains_u64(&parsed, 8080));
    }

    #[test]
    fn render_uses_custom_dns_resolver_for_vm_listener() {
        let mut listeners = BTreeMap::new();
        listeners.insert(
            "vm-123".to_string(),
            test_vm_proxy_listener("127.0.0.1:43128"),
        );
        let rendered = render(
            None,
            &listeners,
            "127.0.0.1:9901".parse().expect("admin addr"),
            "127.0.0.77:5301".parse().expect("dns resolver addr"),
            Path::new("/tmp/envoy-access.log"),
        );
        let parsed = parse_envoy_config(&rendered);
        assert_eq!(
            parsed
                .pointer("/static_resources/clusters/0/cluster_type/typed_config/dns_cache_config/typed_dns_resolver_config/typed_config/resolvers/0/socket_address/address")
                .and_then(Value::as_str),
            Some("127.0.0.77")
        );
        assert_eq!(
            parsed
                .pointer("/static_resources/clusters/0/cluster_type/typed_config/dns_cache_config/typed_dns_resolver_config/typed_config/resolvers/0/socket_address/port_value")
                .and_then(Value::as_u64),
            Some(5301)
        );
    }

    #[test]
    fn render_serializes_regexes_without_manual_escaping() {
        let rendered = render(
            Some("127.0.0.1:3128".parse().expect("listen addr")),
            &BTreeMap::new(),
            "127.0.0.1:9901".parse().expect("admin addr"),
            "127.0.0.53:53".parse().expect("dns resolver addr"),
            Path::new("/tmp/envoy-access.log"),
        );
        let parsed = parse_envoy_config(&rendered);
        assert!(value_contains_str(&parsed, r"svc\.cluster\.local"));
        assert!(value_contains_str(&parsed, r"cloudflare-dns\.com"));
        assert!(value_contains_str(&parsed, r"10\..*"));
    }
    #[test]
    fn lan_opt_in_is_scoped_to_its_listener_and_cluster() {
        let mut allowed = test_vm_proxy_listener("0.0.0.0:15001");
        allowed.policy.allow_lan = true;
        allowed.policy.domain_allowlist = None;
        let listeners = BTreeMap::from([
            ("allowed".into(), allowed),
            ("blocked".into(), test_vm_proxy_listener("0.0.0.0:15002")),
        ]);
        let config = parse_envoy_config(&render(
            None,
            &listeners,
            "127.0.0.1:9901".parse().unwrap(),
            "127.0.0.1:15053".parse().unwrap(),
            Path::new("/tmp/access.log"),
        ));
        let allowed = &config["static_resources"]["listeners"][0];
        let blocked = &config["static_resources"]["listeners"][1];
        assert!(!value_contains_str(allowed, "192.168.0.0"));
        assert!(value_contains_str(blocked, "192.168.0.0"));
        for reserved in ["127.0.0.0", "169.254.0.0", "198.18.0.0"] {
            assert!(value_contains_str(allowed, reserved));
        }
        assert!(value_contains_str(allowed, ENVOY_LAN_CLUSTER_NAME));
        assert!(!value_contains_str(blocked, ENVOY_LAN_CLUSTER_NAME));
        let clusters = &config["static_resources"]["clusters"];
        assert!(clusters[1].get("upstream_bind_config").is_none());
        assert_eq!(
            clusters[2]["upstream_bind_config"]["socket_options"][0]["int_value"],
            json!(super::super::firewall::LAN_EGRESS_MARK)
        );
    }
}
