# Contributing to TerSQL

First off - thank you for taking the time to contribute! TerSQL is a community-driven project and every contribution, large or small, makes a difference.

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
- [Writing Database Plugins](#writing-database-plugins)
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
| **Database version** | The version of the database you connected to (MySQL, PG, Mongo) |
| **TerSQL version** | Output of `python main.py -V` |
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

For large changes (new plugins, command sets, architecture modifications), please open a discussion **before** writing code so we can agree on the approach first. This saves everyone time.

---

## Your First Contribution

Not sure where to start? Look for issues labelled:

- [`good first issue`](https://github.com/developersayandutta/TerSQL/labels/good%20first%20issue) — small, well-defined tasks ideal for newcomers
- [`help wanted`](https://github.com/developersayandutta/TerSQL/labels/help%20wanted) — tasks where extra hands are needed

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

### 3. Install core dependencies

```bash
pip install mysql-connector-python psycopg2-binary pymongo tabulate prompt_toolkit
pip install pytest pytest-cov    # for running tests
```

### 4. Set up local databases

The easiest way to perform local multi-database testing is utilizing Docker:

```bash
# MySQL
docker run -d --name tersql-mysql -e MYSQL_ROOT_PASSWORD=root -p 3306:3306 mysql:8.0

# PostgreSQL
docker run -d --name tersql-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:15

# MongoDB
docker run -d --name tersql-mongo -p 27017:27017 mongo:latest
```

---

## Making Changes

### Branch naming

Always work on a dedicated branch, never directly on `main`:

| Type | Branch format | Example |
|---|---|---|
| Bug fix | `fix/<short-description>` | `fix/safe-mode-truncate-crash` |
| New feature | `feature/<short-description>` | `feature/mongodb-atlas` |
| Documentation | `docs/<short-description>` | `docs/improve-ssl-section` |
| Refactor | `refactor/<short-description>` | `refactor/nlp-engine-cleanup` |

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

---

## Pull Request Process

1. **Ensure all tests pass** before opening a PR.
2. **Update the docs** — if you add or change behaviour, update `README.md` and any relevant docstrings.
3. **One concern per PR** — keep PRs focused. Mixing unrelated fixes makes review harder.
4. **Fill in the PR template** — describe what you changed and why, and link any related issues.
5. **Respond to review feedback** — maintainers may request changes; please address them or explain your reasoning.
6. **Do not force-push after review starts** — it makes it hard to track what changed. Add new commits instead.

---

## Style Guide

TerSQL follows standard Python conventions with a few specifics:

- **PEP 8** for formatting. Use 4-space indentation, no tabs.
- **Line length**: 100 characters maximum.
- **Type hints**: use them for all new public methods. Python 3.10+ syntax (`tuple[bool, str]`, `str | None`) is preferred.
- **Docstrings**: use concise one-line docstrings for methods. Multi-line where the behaviour is non-obvious.
- **String formatting**: use f-strings. Avoid `%`-formatting and `.format()` in new code.
- **Private methods**: prefix with a single underscore (`_cmd_mything`).

---

## Project Structure

TerSQL v0.0.2 embraces a modular, extensible structure decoupled across logic layers:

```
TerSQL/
├── main.py                 — CLI entry point and argument parsing
├── TerSQL.py               — REPL handling (prompt_toolkit), Autocompleters, and dot-commands
├── Core.py                 — Validation logic, execution orchestrator, rendering pipelines
├── NLP.py                  — Safety regex interceptors and intent-matching engine
└── plugins/
    ├── base.py             — Abstract BaseDB class interface defining standard protocols
    ├── mysql.py            — MySQL database driver functionality
    ├── postgre.py          — PostgreSQL database driver functionality
    └── mongodb.py          — MongoDB database driver functionality
```

---

## Writing Database Plugins

TerSQL connects to databases dynamically using the `plugins/` framework. To add support for SQLite, Oracle, Redis, etc.:

1. **Create** a script inside `plugins/your_db.py`.
2. **Subclass** the `BaseDB` interface defined in `plugins/base.py`.
3. **Implement** abstract methods (`connect`, `execute`, `get_schema`, `health`).
4. **Register** the plugin decorator at the top of your class.

```python
from plugins.base import BaseDB, QueryResult, register_plugin

@register_plugin("sqlite")
class SQLitePlugin(BaseDB):
    def connect(self, **kwargs):
        pass # Handle SQLite setup
        
    def execute(self, sql: str) -> QueryResult:
        pass # Route to driver and format QueryResult return
```

Ensure mapping your database responses into the strictly standardized `QueryResult` dataclass models so `TerSQL` renders them reliably!

---

## Adding Dot Commands

Adding a new REPL command (`.mycommand`) requires modifying `TerSQL.py`:

### 1. Write the handler method

Inside `TerSQLREPL`:
```python
def _cmd_mycommand(self, parts):
    """Short description of what this command does."""
    if len(parts) < 2:
        print("  [usage] .mycommand <argument>")
        return
    arg = parts[1]
    print(f"  Did something with {arg}")
```

### 2. Register it in `_handle_dot()`

Inside the dispatch tree in `TerSQL.py`:
```python
elif command == ".mycommand":
    self._cmd_mycommand(parts)
```

### 3. Add to the autocompleter array

In `TerSQL.py`'s `DOT_COMMANDS` constant:
```python
DOT_COMMANDS = [ ..., ".mycommand" ]
```

---

## Adding SQL Auto-corrections

TerSQL automatically sanitizes user typos via pattern configurations inside `Core.py`.

```python
# Core.py
SQL_FIXES = {
    ...
    r"^truncate\s+table\s+([^;\s]+);?$": r"TRUNCATE \1;",
}
```

Rules for a good auto-correction:
- The pattern must be anchored (`^...$`) to avoid false matches.
- Enforce literal regex boundaries restricting wide groupings capturing semicolons (e.g., use `([^;\s]+)`).
- It should fix something a user would genuinely type by mistake.

---

## Running Tests

```bash
# Run the full suite
pytest tests/ -v

# With coverage report
pytest tests/ --cov=TerSQL --cov-report=term-missing
```

When adding new functionality, please add corresponding tests under `tests/`. Tests that require live connections should be marked with `@pytest.mark.integration` and conditionally skipped depending on the CI variables.

---

## Community

- **Issues & PRs**: [github.com/developersayandutta/TerSQL](https://github.com/developersayandutta/TerSQL)
- **Discussions**: [GitHub Discussions](https://github.com/developersayandutta/TerSQL/discussions)

We review pull requests on a best-effort basis. If you haven't heard back within a week, feel free to leave a polite comment on your PR.

---

Thank you for helping make TerSQL better. 🙏