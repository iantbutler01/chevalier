import chevalier_sandbox


def test_sandbox_rejects_unknown_provider_before_connecting():
    try:
        chevalier_sandbox.Sandbox.connect(
            "http://127.0.0.1:1", {"provider": "invented"}
        )
    except RuntimeError as error:
        assert "unsupported sandbox provider `invented`" in str(error)
    else:
        raise AssertionError("unknown provider was accepted")


def test_sandbox_rejects_zero_resources():
    for field in ("default_vcpu", "default_memory_mb", "default_disk_gb"):
        try:
            chevalier_sandbox.Sandbox.connect(
                "http://127.0.0.1:1", {field: 0}
            )
        except RuntimeError as error:
            assert "must be greater than zero" in str(error)
        else:
            raise AssertionError(f"zero {field} was accepted")


def test_sandbox_validates_distributed_control_before_connecting():
    for options, message in [
        ({"etcd_endpoints": [], "nats_url": "nats://localhost"}, "non-empty"),
        ({"etcd_endpoints": ["http://localhost"], "nats_url": "nats://localhost", "nats_stream_replicas": 0}, "positive"),
        ({"etcd_endpoints": ["http://localhost"], "nats_url": "nats://localhost", "required_continuity_tier": "invented"}, "tier-a or tier-b"),
    ]:
        try:
            chevalier_sandbox.Sandbox.connect("http://127.0.0.1:1", {"distributed_control": options})
        except RuntimeError as error:
            assert message in str(error)
        else:
            raise AssertionError("invalid distributed control accepted")
