# Contributing to Chevalier

Thanks for your interest in contributing to Chevalier! This document outlines the process for contributing.

## Getting Started

1. **Fork the repository** and clone your fork
2. Set up your development environment (see READMEs in `rust/` and `py/`)
3. Create a feature branch from `main`

## Requirements for Contributions

### Integration Tests Are Mandatory

**No new feature gets merged without integration tests that run against real LLM APIs.**

Unit tests are welcome and appreciated, but they're not sufficient on their own. Every feature must be battle-tested with live API calls before it can be merged. This means:

- If you touch a provider (Anthropic, OpenAI, Google, OpenRouter, Bedrock), you need to run integration tests against that provider's real API
- New features require new integration tests that make actual LLM calls
- Existing integration tests for affected code paths must pass

### Running Integration Tests

```bash
# Set API keys for providers you're testing
export ANTHROPIC_API_KEY=your_key
export OPENAI_API_KEY=your_key
export GOOGLE_GEMINI_API_KEY=your_key
export OPENROUTER_API_KEY=your_key

# Python integration tests
cd chevalier
source .venv/bin/activate
pytest integration_tests/ -v

# Rust tests
cd rust
cargo test --features full
```

### Linting and Formatting Requirements

All code must pass linting and be properly formatted before it can be merged:

```bash
cd rust

# Rust - clippy with warnings as errors
cargo clippy --all-features -- -D warnings

# Rust - format your code
cargo fmt --all

# Rust - verify formatting (CI runs this)
cargo fmt --all -- --check
```

### What We Look For

- Integration tests that exercise the feature with real LLM responses
- Tests covering both success and error paths where applicable
- Clear test names that describe what's being validated

## Pull Request Process

1. Ensure all integration tests pass locally with real API calls
2. Add integration tests for any new functionality
3. Update documentation if needed
4. Submit your PR with a clear description of the changes

## A Note on Direction

PRs are welcome and appreciated! That said, if a contribution doesn't align with the project's direction, it doesn't mean the work isn't valued - it's just a matter of fit. Chevalier has a specific vision (agents as functions, minimal abstractions, native provider APIs), and contributions should move in that direction.

If you're unsure whether something aligns, feel free to open an issue first to discuss.

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 license.

## Continuous Integration

Ordinary pull requests and pushes to `main` run Rust tests, Clippy, formatting,
Python binding integration and type-contract checks, and the Linux TypeScript binding
checks. Python integration uses development builds; release wheel compilation and the
full native artifact platform matrix run on `v*` tags or manual workflow dispatch.
Use manual dispatch before release when changing platform-specific native code.

Rust dependency caches cover each actual Cargo workspace and are keyed by the pinned
toolchain and dependency inputs. npm caches use the binding lockfiles. New pushes
cancel superseded branch checks; tagged release builds are preserved. Check workflow
changes locally with `actionlint` before pushing.
