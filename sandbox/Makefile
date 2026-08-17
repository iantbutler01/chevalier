# @dive-file: Convenience entrypoints for strict verifier gates and operator drills.
# @dive-rel: Mirrors scripts/verify_chevalier_sandbox.sh gate coverage to enforce checklist completeness.
# @dive-rel: Surfaces per-gate commands used by CI and local contract validation loops.
SHELL := /bin/bash

.PHONY: verify verify-strict verify-e2e verify-e2e-leaks verify-api verify-fork verify-mq verify-reconcile verify-chaos verify-soak verify-security verify-slo evaluate-rollout-policy verify-outbox verify-gateway verify-storage verify-admission verify-dr verify-security-ops verify-envelope verify-fence verify-partition verify-gameday verify-continuity verify-drain verify-restore verify-exactly-once verify-fidelity verify-warm-pool verify-strict-real verify-real-gate41 verify-real-gate42 verify-real-gate43 verify-real-gate44 verify-real-gate45 verify-real-gate46 verify-real-gate47 verify-real-gate48 integration-up integration-down integration-reset integration-artifacts integration-verify-two-node integration-verify-gateway-routing integration-verify-real-failover integration-verify-real-failover-distributed integration-verify-control-plane-failures integration-verify-real-warm-pool integration-verify-planned-drain drill-gateway-failover replay-dlq
.PHONY: build-vz-helper build-vmd-macos verify-vz-helper verify-vmd-macos

verify:
	./scripts/verify_chevalier_sandbox.sh

build-vz-helper:
	swift build --package-path vz-helper -c release

build-vmd-macos:
	cargo build --manifest-path vmd/Cargo.toml --no-default-features --features macos-fskit --bin vmd --bin vmdctl --bin chevalier-vfs-fuse

verify-vz-helper:
	swift test --package-path vz-helper

verify-vmd-macos:
	cargo test --manifest-path vmd/Cargo.toml --no-default-features --features macos-fskit --lib

verify-strict:
	./scripts/verify_chevalier_sandbox.sh --strict

verify-e2e:
	./scripts/verify_chevalier_sandbox.sh --strict --with-e2e

verify-e2e-leaks:
	./scripts/verify_chevalier_sandbox.sh --strict --with-e2e --with-leak-check

verify-api:
	./scripts/verify_api_facade.sh --strict

verify-fork:
	./scripts/verify_fork_cow.sh --strict

verify-mq:
	./scripts/verify_mq_control.sh --strict

verify-reconcile:
	./scripts/verify_reconcile.sh --strict

verify-chaos:
	./scripts/verify_distributed_chaos.sh --strict

verify-soak:
	./scripts/verify_distributed_soak.sh --strict

verify-security:
	./scripts/verify_security_profile.sh --strict

verify-slo:
	./scripts/verify_slo_profile.sh --strict

evaluate-rollout-policy:
	./scripts/evaluate_rollout_policy.sh

verify-outbox:
	./scripts/verify_outbox.sh --strict

verify-gateway:
	./scripts/verify_control_gateway_failover.sh --strict

verify-storage:
	./scripts/verify_storage_profile.sh --strict

verify-admission:
	./scripts/verify_admission_control.sh --strict

verify-dr:
	./scripts/verify_dr_restore.sh --strict

verify-security-ops:
	./scripts/verify_security_ops.sh --strict

verify-envelope:
	./scripts/verify_envelope_compat.sh --strict

verify-fence:
	./scripts/verify_ownership_fence.sh --strict

verify-partition:
	./scripts/verify_network_partition.sh --strict

verify-gameday:
	./scripts/verify_operational_gameday.sh --strict

verify-continuity:
	./scripts/verify_tierb_continuity.sh --strict

verify-drain:
	./scripts/verify_planned_drain_handoff.sh --strict

verify-restore:
	./scripts/verify_tierb_execution_restore.sh --strict

verify-exactly-once:
	./scripts/verify_tierb_exactly_once.sh --strict

verify-fidelity:
	./scripts/verify_execution_fidelity_policy.sh --strict

verify-warm-pool:
	./scripts/verify_warm_pool_pipeline.sh --strict

verify-strict-real:
	./scripts/verify_strict_real.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate41:
	./scripts/verify_real_gate41.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate42:
	./scripts/verify_real_gate42.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate43:
	./scripts/verify_real_gate43.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate44:
	./scripts/verify_real_gate44.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate45:
	./scripts/verify_real_gate45.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate46:
	./scripts/verify_real_gate46.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate47:
	./scripts/verify_real_gate47.sh --profile $(or $(PROFILE),local-dev)

verify-real-gate48:
	./scripts/verify_real_gate48.sh --profile $(or $(PROFILE),local-dev)

integration-up:
	./scripts/integration/up.sh --profile local-dev

integration-down:
	./scripts/integration/down.sh --profile local-dev

integration-reset:
	./scripts/integration/reset.sh --profile local-dev

integration-artifacts:
	./scripts/integration/collect-artifacts.sh --profile local-dev --reason make-target

integration-verify-two-node:
	./scripts/integration/verify_two_node_registry.sh --profile local-dev

integration-verify-gateway-routing:
	./scripts/integration/verify_two_node_registry.sh --profile local-dev --run-facade-test

integration-verify-real-failover:
	./scripts/integration/verify_real_failover.sh --profile local-dev

integration-verify-real-failover-distributed:
	./scripts/integration/verify_real_failover.sh --profile local-dev --distributed

integration-verify-control-plane-failures:
	./scripts/integration/verify_control_plane_failures.sh --profile local-dev

integration-verify-real-warm-pool:
	./scripts/integration/verify_real_warm_pool.sh --profile local-dev

integration-verify-planned-drain:
	./scripts/integration/verify_planned_drain_handoff.sh --profile local-dev

drill-gateway-failover:
	./scripts/run_control_gateway_failover_drill.sh --strict

replay-dlq:
	./scripts/replay_mq_dead_letters.sh $(or $(LIMIT),100)
