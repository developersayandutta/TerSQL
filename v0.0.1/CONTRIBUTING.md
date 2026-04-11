# Contributing to TerSQL

First off — thank you for taking the time to contribute! TerSQL is a community-driven project and every contribution, large or small, makes a difference.

This document covers everything you need to know to go from idea to merged pull request.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Ways to Contribute](#ways-to-contribute)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)
- [Your First Contribution](#your-first-contribution)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Commit Messages](#commit-messages)
- [Pull Request Process](#pull-request-process)
- [Style Guide](#style-guide)
- [Project Structure](#project-structure)
- [Adding Dot Commands](#adding-dot-commands)
- [Adding SQL Auto-corrections](#adding-sql-auto-corrections)
- [Running Tests](#running-tests)
- [Community](#community)

---

## Code of Conduct

This project follows a simple rule: **be kind**. We welcome contributors of all experience levels. Disrespectful, exclusionary, or harassing behaviour will not be tolerated. If something feels off, open an issue or email the maintainers.

---

## Ways to Contribute

You don't have to write code to make a meaningful contribution:

- **Report a bug** — something broken? Let us know.
- **Suggest a feature** — have an idea? Open a discussion.
- **Improve the docs** — typos, unclear wording, missing examples.
- **Write tests** — coverage is always welcome.
- **Review pull requests** — a second pair of eyes helps everyone.
- **Share the project** — star the repo, write a blog post, tell a colleague.

---

## Reporting Bugs

Before opening a bug report, please:

1. Check the [existing issues](https://github.com/developersayandutta/TerSQL/issues) to avoid duplicates.
2. Make sure you are running the latest version.

When filing a bug, please include:

| Field | What to provide |
|---|---|
| **OS** | e.g. Ubuntu 22.04, macOS 14, Windows 11 |
| **Python version** | Output of `python --version` |
| **MySQL server version** | Output of `SELECT VERSION();` |
| **TerSQL version** | Output of `python TerSQL.py --version` |
| **Steps to reproduce** | Exact commands or queries that trigger the bug |
| **Expected behaviour** | What you expected to happen |
| **Actual behaviour** | What actually happened, including full error output |

> **Security vulnerabilities** should **not** be filed as public issues. Please email the maintainers directly instead. See [SECURITY.md](SECURITY.md) for the full policy.

---

## Suggesting Features

Open a [GitHub Discussion](https://github.com/developersayandutta/TerSQL/discussions) or an issue tagged `enhancement`. Please describe:

- The problem you are trying to solve
- How your proposed feature would solve it
- Any alternatives you have considered

For large changes (new commands, refactored internals), please open a discussion **before** writing code so we can agree on the approach first. This saves everyone time.

---

## Your First Contribution

Not sure where to start? Look for issues labelled:

- [`good first issue`](https://github.com/developersayandutta/TerSQL/labels/good%20first%20issue) — small, well-defined tasks ideal for newcomers
- [`help wanted`](https://github.com/developersayandutta/TerSQL/labels/help%20wanted) — tasks where extra hands are needed

If you are brand new to open-source contribution, [this guide](https://opensource.guide/how-to-contribute/) is a great starting point.

---

## Development Setup

### 1. Fork and clone

```bash
# Fork the repo on GitHub, then:
git clone https://github.com/<your-username>/TerSQL.git
cd TerSQL
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install mysql-connector-python tabulate prompt_toolkit Pygments
pip install pytest pytest-cov    # for running tests
```

### 4. Set up a local MySQL instance

The easiest way is Docker:

```bash
docker run -d \
  --name tersql-dev \
  -e MYSQL_ROOT_PASSWORD=root \
  -p 3306:3306 \
  mysql:8.0
```

Or use an existing local MySQL installation.

### 5. Verify your setup

```bash
python TerSQL.py -u root -p root -e "SELECT 'setup ok';"
```

---

## Making Changes

### Branch naming

Always work on a dedicated branch, never directly on `main`:

| Type | Branch format | Example |
|---|---|---|
| Bug fix | `fix/<short-description>` | `fix/safe-mode-truncate-crash` |
| New feature | `feature/<short-description>` | `feature/cmd-rename` |
| Documentation | `docs/<short-description>` | `docs/improve-ssl-section` |
| Refactor | `refactor/<short-description>` | `refactor/completer-cleanup` |

```bash
git checkout -b fix/safe-mode-truncate-crash
```

### Keep your fork up to date

```bash
git remote add upstream https://github.com/developersayandutta/TerSQL.git
git fetch upstream
git rebase upstream/main
```

---

## Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<scope>): <short summary>

[optional body]

[optional footer]
```

**Types:**

| Type | When to use |
|---|---|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `refactor` | Code change that is neither a fix nor a feature |
| `test` | Adding or updating tests |
| `chore` | Build process, dependency updates, tooling |

**Examples:**

```
feat(commands): add .rename command to rename tables
fix(safety): safe-mode now correctly blocks TRUNCATE without confirmation
docs(readme): add SSL example for RDS connections
```

Keep the summary line under 72 characters. Use the body to explain *why*, not *what*.

---

## Pull Request Process

1. **Ensure all tests pass** before opening a PR (see [Running Tests](#running-tests)).
2. **Update the docs** — if you add or change behaviour, update `README.md` and any relevant docstrings.
3. **One concern per PR** — keep PRs focused. Mixing unrelated fixes makes review harder.
4. **Fill in the PR template** — describe what you changed and why, and link any related issues with `Closes #<issue>`.
5. **Respond to review feedback** — maintainers may request changes; please address them or explain your reasoning.
6. **Do not force-push after review starts** — it makes it hard to track what changed. Add new commits instead.

A pull request is ready to merge when:

- At least one maintainer has approved it
- All CI checks pass
- No unresolved review comments remain

---

## Style Guide

TerSQL follows standard Python conventions with a few specifics:

- **PEP 8** for formatting. Use 4-space indentation, no tabs.
- **Line length**: 100 characters maximum.
- **Type hints**: use them for all new public methods. Python 3.10+ syntax (`tuple[bool, str]`, `str | None`) is preferred.
- **Docstrings**: use concise one-line docstrings for methods. Multi-line where the behaviour is non-obvious.
- **String formatting**: use f-strings. Avoid `%`-formatting and `.format()` in new code.
- **Private methods**: prefix with a single underscore (`_cmd_mything`).
- **No external dependencies** beyond the five listed in `Requirements`. The goal is a single-file script.

A quick auto-format check:

```bash
pip install flake8
flake8 TerSQL.py --max-line-length=100
```

---

## Project Structure

TerSQL intentionally lives in a single file. Here is how the internals are organised:

```
TerSQL.py
│
├── Module-level constants
│   ├── SQL_KEYWORDS        — autocomplete keyword list
│   ├── TABLE_FORMATS       — valid tabulate format names
│   ├── OUTPUT_MODES        — valid output modes
│   ├── COMMAND_ALIASES     — dot-command aliases map
│   └── SQL_FIXES           — auto-correction regex patterns
│
├── SmartCompleter          — prompt_toolkit Completer subclass
│
└── MySQLTerminal           — main class
    ├── __init__            — configuration and state
    ├── _load/_save_bookmarks
    ├── _refresh_completer / _fetch_schema_words
    ├── _render_result / _print_output
    ├── _check_safety       — safe-mode and read-only guards
    ├── connect / reconnect / _ensure_connection
    ├── _safe_execute / execute / execute_script
    ├── handle_command      — dot-command dispatcher
    ├── _cmd_*              — individual dot-command handlers
    ├── export_result
    ├── run                 — main REPL loop
    └── close
```

---

## Adding Dot Commands

Adding a new dot command takes three steps:

### 1. Write the handler method

```python
def _cmd_mycommand(self, parts):
    """Short description of what this command does."""
    if len(parts) < 2:
        print("[usage] .mycommand <argument>")
        return
    arg = parts[1]
    # your logic here
    print(f"[ok] Did something with {arg}")
```

### 2. Register it in the dispatch dict

Inside `handle_command`, add an entry to `dispatch`:

```python
".mycommand": lambda: self._cmd_mycommand(parts),
```

### 3. Add it to the completer and help text

- Add `".mycommand"` to the `dot_commands` list in `SmartCompleter.__init__`.
- Add a row to `HELP_TEXT`.
- Document it in `README.md`.

If your command accepts `on`/`off` or other subcommands, follow the pattern in `_cmd_timer` or `_cmd_profile`.

---

## Adding SQL Auto-corrections

Auto-corrections live in the `SQL_FIXES` dict at the top of the file. Each entry is a `{pattern: replacement}` pair using Python `re` syntax.

```python
SQL_FIXES = {
    ...
    # Show columns shorthand
    r"^show\s+columns\s+(\S+);?$": r"SHOW COLUMNS FROM \1;",
}
```

Rules for a good auto-correction:

- The pattern must be anchored (`^...$`) to avoid false matches.
- It should fix something a user would genuinely type by mistake.
- The corrected form is printed with `[auto-fix]` so the user always knows what ran.
- Add a test case in the test suite.

---

## Running Tests

```bash
# Run the full suite
pytest tests/ -v

# With coverage report
pytest tests/ --cov=TerSQL --cov-report=term-missing

# Run a single test file
pytest tests/test_safety.py -v
```

When adding new functionality, please add corresponding tests under `tests/`. Tests that require a live database connection should be marked with `@pytest.mark.integration` and skipped by default in CI unless `TERSQL_TEST_DSN` is set.

---

## Community

- **Issues & PRs**: [github.com/developersayandutta/TerSQL](https://github.com/developersayandutta/TerSQL)
- **Discussions**: [GitHub Discussions](https://github.com/developersayandutta/TerSQL/discussions)

We review pull requests on a best-effort basis. If you haven't heard back within a week, feel free to leave a polite comment on your PR.

---

Thank you for helping make TerSQL better. 🙏