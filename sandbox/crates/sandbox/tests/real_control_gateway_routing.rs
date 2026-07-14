// @dive-file: Real-machinery facade integration test validating gateway endpoint routing with an unhealthy primary endpoint.
// @dive-rel: Executed by scripts/integration/verify_two_node_registry.sh when --run-facade-test is enabled.
// @dive-rel: Uses public Sandbox facade APIs only, avoiding test-only direct runtime shortcuts.
use std::env;
use std::time::Duration;

use chevalier_sandbox::{Sandbox, SandboxConfig};

fn real_gateway_endpoints() -> Vec<String> {
    env::var("CHEVALIER_SANDBOX_REAL_GATEWAY_ENDPOINTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[tokio::test]
#[ignore = "requires real control-plane daemons; run via scripts/integration/verify_two_node_registry.sh --run-facade-test"]
async fn facade_routes_through_control_gateways_on_real_daemons() {
    let primary_endpoint = env::var("CHEVALIER_SANDBOX_REAL_PRIMARY_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:19079".to_string());
    let gateway_endpoints = real_gateway_endpoints();
    assert!(
        !gateway_endpoints.is_empty(),
        "CHEVALIER_SANDBOX_REAL_GATEWAY_ENDPOINTS must provide at least one endpoint"
    );

    let cfg = SandboxConfig {
        auto_spawn: false,
        prewarm_on_start: false,
        default_image: "ghcr.io/bracketdevelopers/uv-builder:main".to_string(),
        connect_timeout: Duration::from_secs(3),
        control_gateway_endpoints: gateway_endpoints,
        ..SandboxConfig::default()
    };

    // @dive: Primary endpoint is intentionally unhealthy; connect must succeed through control gateways only.
    let sandbox = Sandbox::connect(primary_endpoint, cfg)
        .await
        .expect("facade should connect via healthy control gateway endpoint");
    let _ = sandbox
        .list_sessions()
        .await
        .expect("facade should issue RPCs through control gateways");
}
