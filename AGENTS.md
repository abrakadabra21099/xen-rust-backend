# Repository Guidelines

## Project Structure & Module Organization

The current checkout contains no application source, tests, or asset directories. Add new code in clearly named, language-appropriate directories (for example, `src/`, `tests/`, and `assets/`) and keep tests or documentation close to the components they describe. Update this guide when the repository gains an established layout.

## Build, Test, and Development Commands

No build system or runnable project commands are present yet. When adding a toolchain, document its required runtime and canonical commands in the project README and here. Prefer reproducible commands that work from the repository root, such as `make test` or the package manager's test script; do not document commands until they are configured and verified.

## Coding Style & Naming Conventions

Follow the conventions of the language and framework introduced by the change. Use consistent indentation, descriptive names, and small modules with clear responsibilities. Preserve existing formatting and use the repository's configured formatter or linter when one is added. Name tests after the behavior or module they cover (for example, `test_parser.py` or `parser.test.ts`).

## Testing Guidelines

There is no test framework or test suite in the current checkout. Add focused tests for new behavior alongside the chosen framework, and include the exact command needed to run them in project documentation. Run the relevant checks before proposing a change and report any checks that could not be run.

## Commit & Pull Request Guidelines

Git history is unavailable in this checkout, so no repository-specific commit convention can be confirmed. Use concise imperative commit subjects (for example, `Add input validation`). Pull requests should explain the change and its motivation, list relevant validation commands and results, and link related issues. Include screenshots only when the change affects a visual interface.

## Security & Configuration

Do not commit credentials, private keys, or machine-specific configuration. Keep secrets in environment variables or an approved local secret store, and provide sanitized examples when documenting configuration.
