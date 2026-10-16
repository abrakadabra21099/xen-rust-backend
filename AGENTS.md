# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust library crate. `src/lib.rs` is the crate root, `src/cluster/` contains cluster membership and consensus state types, and `schema/` contains the JSON Schema for Xen domain configuration. Architecture notes live in `docs/`. Keep code close to its domain module and update this guide when the layout changes.

## Build, Test, and Development Commands

The project is configured as a Cargo crate in `Cargo.toml`. From the repository root, `cargo fmt --check` checks formatting, `cargo check` compiles the crate, and `cargo test` runs the integration suite. These commands have been verified with the installed Rust toolchain.

## Coding Style & Naming Conventions

Follow the conventions of the language and framework introduced by the change. Use consistent indentation, descriptive names, and small modules with clear responsibilities. Preserve existing formatting and use the repository's configured formatter or linter when one is added. Name tests after the behavior or module they cover (for example, `test_parser.py` or `parser.test.ts`).

## Testing Guidelines

Integration tests live in `tests/` and cover weighted quorum, membership transitions and commit tracking, leaderless Paxos rounds, and SQLite durability/recovery. Every newly implemented function or behavior must receive comprehensive tests in the same change, including success paths, invalid inputs, boundary conditions, and persistence/recovery cases when applicable. Keep assertions based on protocol and storage invariants; do not weaken expected behavior to match an implementation defect. Run `cargo test` after each implementation change.

## Commit & Pull Request Guidelines

Git history is unavailable in this checkout, so no repository-specific commit convention can be confirmed. Use concise imperative commit subjects (for example, `Add input validation`). Pull requests should explain the change and its motivation, list relevant validation commands and results, and link related issues. Include screenshots only when the change affects a visual interface.

## Security & Configuration

Do not commit credentials, private keys, or machine-specific configuration. Keep secrets in environment variables or an approved local secret store, and provide sanitized examples when documenting configuration.
