# TerSQL

<div align="center">

**An intelligent, multi-database command-line client written in Python.**

[![Version](https://img.shields.io/badge/version-0.0.2--beta-blue.svg)](https://github.com/developer/TerSQL)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-beta-orange.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

[Features](#features) · [Installation](#installation) · [Quick Start](#quick-start) · [Documentation](#dot-commands-reference) · [Contributing](#contributing)

</div>

---

TerSQL is a modern reboot of the standard database CLI, offering a modular drop-in replacement that unifies connectivity to MySQL, PostgreSQL, and MongoDB. It features a modern developer experience: strict safe modes, offline NLP intent parsing, query bookmarks, smart autocompletion, query profiling, and multi-format outputs—all with zero external NLP/LLM API dependencies.

## Features

- **Multi-Database Support** — Effortless connections to MySQL, PostgreSQL, and MongoDB via a local unified architecture.
- **Natural Language & Auto-fix** — Write "show me users" and let the offline regex rule-engine seamlessly structure valid SQL.
- **Smart Autocompletion** — Pulls live SQL keywords, table structures, and columns directly from your database via `Tab`.
- **Multiple output formats** — `grid`, `psql`, `markdown`, `html`, `latex`, `json`, `csv`, `vertical`, and more.
- **Query bookmarks** — save, list, and replay frequently used queries across sessions.
- **Safety guards** — safe mode securely blocks `DELETE`/`UPDATE` without `WHERE`; read-only mode blocks all writes to production.
- **Persistent history** — arrow-key history recall and fuzzy search with `Ctrl+R`.
- **Tee & pager** — pipe output through `less`, and/or mirror output to a log file.
- **Script execution** — run `.sql` files directly with `.source`.
- **Non-interactive mode** — single-query batch execution for lightweight shell scripting.

---

## Requirements

| Dependency | Minimum version | Purpose |
|---|---|---|
| Python | 3.10 | Core Engine |
| mysql-connector-python | 8.0 | MySQL Plugin |
| psycopg2-binary | 2.9 | PostgreSQL Plugin |
| pymongo | 4.6 | MongoDB Plugin |
| tabulate | 0.9 | Table Rendering |
| prompt_toolkit | 3.0 | Completions |

Install all dependencies at once:

```bash
pip install mysql-connector-python psycopg2-binary pymongo tabulate prompt_toolkit
```

---

## Installation

TerSQL currently operates directly from its modular repository.

**Clone and run locally:**

```bash
git clone https://github.com/developersayandutta/TerSQL.git
cd TerSQL
python main.py
```

---

## Quick Start

```bash
# Enter the interactive connection wizard
python main.py

# Connect directly to Postgres
python main.py -d postgresql -H localhost -u postgres -p

# Connect to MongoDB with a specific database
python main.py -d mongodb -H localhost:27017 --database testdb

# Run a single query in batch mode
python main.py -d mysql -H localhost -e "SELECT VERSION();"
```

After connecting:

```
  ╔══════════════════════════════════════════════════╗
  ║   TerSQL  v0.0.2 (beta)                          ║
  ║   Intelligent Multi-Database Query Interface     ║
  ╚══════════════════════════════════════════════════╝
  Type SQL or natural language.  .help for commands.

TerSQL [mysql:?] > 
```

---

## Command-line Arguments

| Flag | Default | Description |
|---|---|---|
| `-d`, `--db-type` | *(wizard)* | Database plugin: `mysql`, `postgresql`, `mongodb` |
| `-H`, `--host` | `localhost` | Server hostname or IP |
| `-P`, `--port` | *(auto)* | Port (Mapped per database type automatically) |
| `-u`, `--user` | `root` | Database username |
| `-p`, `--password` | *(prompt)* | Password. Omit to be prompted securely. |
| `--database` | *(none)* | Default initial database |
| `--format` | `grid` | Initial table display format |
| `--output` | `table` | Initial output mode: `table`, `csv`, `json`, `vertical` |
| `--log` | *(none)* | Write a timestamped query log to this file |
| `--history` | `~/.tersql_history` | Persistent history file path |
| `--safe-mode` | off | Block dangerous queries without `WHERE` |
| `--read-only` | off | Block all write operations (DML/DDL) |
| `--no-timer` | off | Disable per-query execution timing |
| `-e`, `--execute` | *(none)* | Execute one statement, print result, and exit |
| `-V`, `--version` | — | Print version and exit |

---

## Dot Commands Reference

Dot commands begin with `.` and control the terminal itself rather than executing SQL.

### Information & Navigation

| Command | Description |
|---|---|
| `.help` | Display the full command reference |
| `.status` | Show connection details, session stats, and active settings |
| `.use <db>` | Switch currently connected database or schema |
| `.dbs` | List all databases |
| `.tables` | List tables or collections |
| `.schema <table>` | Describe table columns or collections |
| `.explain <cmd>` | Intercept command and print translation rules without execution |

### Output Control

| Command | Description |
|---|---|
| `.format <fmt>` | Set the table display format (see Output Formats) |
| `.output <mode>` | Set output mode: `table`, `csv`, `json` |
| `.clear` | Clear the terminal screen |

### Export

| Command | Description |
|---|---|
| `.export <file> [csv\|json]` | Export the last `SELECT` result to a file. Format inferred natively from file extension. |

### Bookmarks

| Command | Description |
|---|---|
| `.bookmark <name> <SQL>` | Save a SQL statement under a short alias |
| `.bookmarks` | List all saved bookmarks |
| `.run <name>` | Execute a saved bookmark by alias |
| `.delbookmark <name>` | Remove a saved bookmark |

Bookmarks persist to `~/.tersql_bookmarks.json` across sessions.

### Diagnostics & Settings

| Command | Description |
|---|---|
| `.history [n]` | View lightweight session history of last `n` executed commands |
| `.safe on\|off` | Toggle safe-mode bounds to prevent untracked updates |
| `.readonly on\|off` | Toggle strictly guarded read-only pipeline states |
| `.timer on\|off` | Toggle query delay timers |
| `.reconnect` | Restart network connections |
| `exit` / `.q` | Quit out of the running interface safely |

### Aliases

| Alias | Canonical |
|---|---|
| `.table` | `.tables` |
| `.database` | `.use` |
| `.databases` | `.dbs` |
| `.desc` / `.describe` | `.schema` |
| `.quit` / `.exit` | `.q` |
| `.ex` | `.export` |
| `.fmt` | `.format` |
| `.out` | `.output` |
| `.hist` | `.history` |
| `.st` | `.status` |
| `.bm` | `.bookmark` |
| `.fav` | `.bookmarks` |
| `.h` / `.?` | `.help` |

---

## Output Formats and Modes

### Table formats (`--format` / `.format`)

| Format | Description |
|---|---|
| `grid` | Box-drawing characters *(default)* |
| `psql` | PostgreSQL-style borders |
| `pipe` / `markdown` | Markdown pipe table |
| `github` | GitHub-flavored Markdown |
| `plain` | No borders, space-aligned |
| `html` | Raw HTML `<table>` |
| `latex` | LaTeX tabular environment |

### Output modes (`--output` / `.output`)

| Mode | Description |
|---|---|
| `table` | Formatted table using the current format *(default)* |
| `json` | Pretty-printed JSON array of objects |
| `csv` | CSV with header row |

---

## Safety Guards

### Safe Mode (`--safe-mode`)

Blocks `DELETE` and `UPDATE` without a `WHERE` clause, and safely mitigates unhandled exceptions on transaction drops.

```bash
python main.py --safe-mode -d mysql
```

```
TerSQL [mysql:?] > DELETE FROM users;
[safe-mode] DELETE statement without WHERE clause is blocked.
```

### Read-only Mode (`--read-only`)

Blocks all write operations at the terminal NLP layer before it even hits the engine drivers. 
Blocked intents: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`.

```bash
python main.py --read-only -d postgresql
```

---

## Non-interactive (Batch) Mode

Use `-e` / `--execute` to run a single query and exit.

```bash
# Print result to stdout formatting markdown tables
python main.py -d postgresql -u postgres --format markdown -e "SELECT * FROM orders;"

# Machine-readable JSON directly exported locally
python main.py -d mongodb --output json -e "db.products.find()" > export.json
```

---

## Query History & Bookmarks

**History** is stored globally inside `~/.tersql_history`. Navigate buffers smoothly via `↑`/`↓` keys, or hit `Ctrl+R` for fuzzy recall searches!

**Bookmarks** persist locally in `~/.tersql_bookmarks.json`, loading cleanly every setup initialization.

```
TerSQL [postgre:?] > .bookmark staging_query SELECT * FROM logs WHERE env='staging';
[ok] Bookmark created!

TerSQL [postgre:?] > .run staging_query
```

---

## Auto-correction

The `Core.py` engine fixes common shell developer shorthands:

| You type | Executed as |
|---|---|
| `show database` | `SHOW DATABASES;` |
| `show table` | `SHOW TABLES;` |
| `desc users` | `DESCRIBE users;` |
| `select * users` | `SELECT * FROM users;` |
| `select from users` | `SELECT * FROM users;` |

---

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `↑` / `↓` | Navigate persistent command history |
| `Ctrl+R` | Fuzzy search through command history |
| `Tab` | Autocomplete SQL keywords, table/column names, dot commands |
| `Ctrl+C` | Clear the current input buffer (does not exit) |
| `Ctrl+D` | Exit the terminal |

---

## Configuration Files

| File | Purpose |
|---|---|
| `~/.tersql_history` | Persistent query history memory buffer. |
| `~/.tersql_bookmarks.json` | Persistent query bookmarks storage file. |

---

## Extending TerSQL

TerSQL strictly leverages an extensible architecture split between `TerSQL.py`, `Core.py`, `NLP.py`, and `plugins/base.py`.

### Writing a new Database Driver Plugin

Create a script bridging inside the `plugins/` directory subclassing `BaseDB`:
```python
from plugins.base import BaseDB, QueryResult, register_plugin

@register_plugin("redis")
class RedisPlugin(BaseDB):
    def connect(self, **kwargs):
        pass
    def execute(self, query: str) -> QueryResult:
        pass
```

### Adding an NLP regex auto-correction

Inject tightly bordered rules inside `Core.py`:

```python
SQL_FIXES = {
    ...
    r"^truncate\s+table\s+([^;\s]+);?$": r"TRUNCATE \1;",
}
```

---

## Known Limitations

- **Large result sets**: Results are fully bundled into memory strings before output renders.
- **Python < 3.10**: Strictly leverages modern dictionary syntax (`|` operator) restricting pre-2021 python engines natively.

---

## Contributing

Review our detailed contribution pathways inside [CONTRIBUTING.md](CONTRIBUTING.md).

For bugs, outline the standard environments (`Python --version`, specific database drivers, and local crash traces). 

---

## Changelog

### v0.0.2 (beta) — Multi-Database Component Release

- **Plugin Architecture Refactor**: Separated SQL orchestration components from execution modules parsing `plugins/` interfaces natively.
- **Database Diversity**: Integrated driver architectures spanning native `postgresql` and non-relational `mongodb` integrations beside the core mysql baseline.
- **NLP Engine Pipeline**: Created strict Python regex natural language rules to execute structural tasks omitting LLM fallback integrations entirely.
- **Smart Completer Sync**: Rewritten smart completing syntax injecting dynamic engine dictionaries live across active sessions!
- Strict UI terminal encapsulation splitting REPL bindings from Core logic executions. 
- Repaired unhandled runtime crashes bypassing transaction aborts natively.

### v0.0.1 (beta) — Initial Release

- Interactive REPL with syntax highlighting and smart autocompletion
- Persistent query history with fuzzy search (`Ctrl+R`)
- Base Output configurations and `.bookmark` states mappings.
- Safe mode and read-only mode architectures.

---

## License

MIT — see [LICENSE](LICENSE) for details.