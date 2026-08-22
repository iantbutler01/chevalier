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
