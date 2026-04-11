# TerSQL

<div align="center">

**An interactive, feature-rich command-line MySQL client written in Python.**

[![Version](https://img.shields.io/badge/version-0.0.1--beta-blue.svg)](https://github.com/developer/TerSQL)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-beta-orange.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

[Features](#features) · [Installation](#installation) · [Quick Start](#quick-start) · [Documentation](#dot-commands-reference) · [Contributing](#contributing)

</div>

---

TerSQL is a drop-in replacement for the standard `mysql` CLI with a modern developer experience: syntax highlighting, smart autocompletion, multiple output formats, query bookmarks, safety guards, SSL support, query profiling, and much more — all in a single Python file with zero AI/NLP dependencies.

## Features

- **Syntax highlighting** — powered by Pygments, color-coded as you type
- **Smart autocompletion** — SQL keywords, live table/column names, and dot commands via `Tab`
- **Multiple output formats** — `grid`, `psql`, `markdown`, `html`, `latex`, `json`, `csv`, `vertical`, and more
- **Query bookmarks** — save, list, and replay frequently used queries across sessions
- **Safety guards** — safe mode blocks `DELETE`/`UPDATE` without `WHERE`; read-only mode blocks all writes
- **Query profiling** — MySQL `SHOW PROFILES` integration with per-stage timing
- **Persistent history** — arrow-key history recall and fuzzy search with `Ctrl+R`
- **Tee & pager** — pipe output through `less`, and/or mirror output to a log file
- **Script execution** — run `.sql` files directly with `.source`
- **Non-interactive mode** — single-query batch execution for shell scripting
- **SSL/TLS** — full certificate support for encrypted connections
- **Auto-correction** — silently rewrites common shorthand (e.g. `show table` → `SHOW TABLES;`)

---

## Requirements

| Dependency | Minimum version |
|---|---|
| Python | 3.10 |
| mysql-connector-python | 8.0 |
| tabulate | 0.9 |
| prompt_toolkit | 3.0 |
| Pygments | 2.0 |

Install all dependencies at once:

```bash
pip install mysql-connector-python tabulate prompt_toolkit Pygments
```

---

## Installation

TerSQL is a single-file script — no package installation or build step required.

**Download and run:**

```bash
curl -O https://raw.githubusercontent.com/your-org/TerSQL/main/TerSQL.py
python TerSQL.py
```

**Make it executable (Unix/Linux/macOS):**

```bash
chmod +x TerSQL.py
./TerSQL.py
```

**Install system-wide (Unix/Linux/macOS):**

```bash
sudo cp TerSQL.py /usr/local/bin/TerSQL
sudo chmod +x /usr/local/bin/TerSQL
TerSQL
```

---

## Quick Start

```bash
# Connect to localhost with defaults
python TerSQL.py

# Connect to a remote host with a specific database
python TerSQL.py -H db.example.com -u myuser -d mydb

# Run a single query and exit (batch mode)
python TerSQL.py -e "SELECT VERSION();"
```

After connecting:

```
TerSQL  v0.0.1 (beta)
Type '.help' for commands, 'exit' to quit.

[connected] localhost:3306  server=8.0.35  user=root

mysql>
```

---

## Command-line Arguments

| Flag | Default | Description |
|---|---|---|
| `-H`, `--host` | `localhost` | MySQL server hostname or IP |
| `-P`, `--port` | `3306` | MySQL server port |
| `-u`, `--user` | `root` | MySQL username |
| `-p`, `--password` | *(prompt)* | Password. Omit to be prompted securely. |
| `-d`, `--database` | *(none)* | Default database on connect |
| `--format` | `grid` | Initial table display format |
| `--output` | `table` | Initial output mode: `table`, `csv`, `json`, `vertical` |
| `--log` | *(none)* | Write a timestamped query log to this file |
| `--history` | `~/.TerSQL_history` | Persistent history file path |
| `--ssl-ca` | *(none)* | Path to SSL CA certificate |
| `--ssl-cert` | *(none)* | Path to SSL client certificate |
| `--ssl-key` | *(none)* | Path to SSL client key |
| `--connect-timeout` | `10` | Connection timeout in seconds |
| `--charset` | `utf8mb4` | MySQL character set |
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
| `.version` | Show terminal, server, Python, and library versions |
| `.status` | Show connection details, session stats, and active settings |
| `.db` | Print the currently selected database |
| `.dbs` | List all databases (`SHOW DATABASES`) |
| `.tables` | List tables in the current database |
| `.schema <table>` | Describe a table's columns (`DESCRIBE table`) |
| `.indexes <table>` | Show all indexes for a table |
| `.rowcount` | Show row counts for every table in the current database |
| `.variables [pattern]` | Show MySQL system variables, optionally filtered |
| `.processlist` | Show the full process list (`SHOW FULL PROCESSLIST`) |

### Output Control

| Command | Description |
|---|---|
| `.format <fmt>` | Set the table display format (see [Output Formats](#output-formats-and-modes)) |
| `.output <mode>` | Set output mode: `table`, `csv`, `json`, `vertical` |
| `.clear` | Clear the terminal screen |
| `.pager [cmd]` | Pipe output through a program (e.g. `less -S`). Omit to disable. |
| `.tee <file>` | Mirror all output to a file while printing to screen |
| `.notee` | Stop the tee file output |

### Export

| Command | Description |
|---|---|
| `.export <file> [csv\|json]` | Export the last `SELECT` result to a file. Format inferred from extension if omitted. |

### Bookmarks

| Command | Description |
|---|---|
| `.bookmark <name> <SQL>` | Save a SQL statement under a short name |
| `.bookmarks` | List all saved bookmarks |
| `.run <name>` | Execute a saved bookmark by name |
| `.delbookmark <name>` | Remove a saved bookmark |

Bookmarks persist to `~/.TerSQL_bookmarks.json` across sessions.

### Maintenance

| Command | Description |
|---|---|
| `.drop <table>` | Drop a table with an interactive confirmation step |
| `.copy <src> <dst>` | Create a new empty table with the same structure as an existing one |
| `.diff <t1> <t2>` | Show column-level structural differences between two tables |
| `.kill <pid>` | Kill a running process by ID (with confirmation) |
| `.source <file.sql>` | Read and execute all SQL statements from a file |
| `.reconnect` | Close and re-open the database connection |

### Diagnostics

| Command | Description |
|---|---|
| `.explain <SELECT>` | Run `EXPLAIN` on a `SELECT` statement |
| `.warnings` | Show warnings raised by the last statement |
| `.profile on\|off` | Enable or disable query profiling |
| `.timer on\|off` | Toggle per-query elapsed-time display |
| `.history [n]` | Show the last `n` queries (default: 20) |
| `.charset <name>` | Change the session character set |

### Aliases

| Alias | Canonical |
|---|---|
| `.table` | `.tables` |
| `.database` | `.db` |
| `.databases` | `.dbs` |
| `.desc` / `.describe` | `.schema` |
| `.quit` / `.exit` / `.q` | `exit` |
| `.ex` | `.export` |
| `.fmt` | `.format` |
| `.out` | `.output` |
| `.hist` | `.history` |
| `.vars` | `.variables` |
| `.proc` | `.processlist` |
| `.st` | `.status` |
| `.bm` | `.bookmark` |
| `.fav` | `.bookmarks` |
| `.prof` | `.profile` |
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
| `simple` | Dashes as separators only |
| `orgtbl` | Emacs Org-mode table |
| `html` | Raw HTML `<table>` |
| `latex` | LaTeX tabular environment |
| `rst` | reStructuredText grid table |

```
mysql> .format psql
mysql> SELECT id, name FROM users LIMIT 3;

 id | name
----+-------
  1 | Alice
  2 | Bob
  3 | Carol
```

### Output modes (`--output` / `.output`)

| Mode | Description |
|---|---|
| `table` | Formatted table using the current format *(default)* |
| `json` | Pretty-printed JSON array of objects |
| `csv` | CSV with header row |
| `vertical` | One column per line, MySQL `\G` style |

```
mysql> .output json
mysql> SELECT id, name FROM users LIMIT 2;

[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

---

## Safety Guards

### Safe Mode (`--safe-mode`)

Blocks `DELETE` and `UPDATE` without a `WHERE` clause, and requires interactive confirmation before `DROP TABLE` or `TRUNCATE TABLE`.

```bash
python TerSQL.py --safe-mode -u root -d mydb
```

```
mysql> DELETE FROM users;
[safe-mode] DELETE without WHERE is blocked.

mysql> DROP TABLE old_logs;
[confirm] Are you sure you want to DROP? (yes/no): yes
Query OK  0 row(s) affected  (0.0031 sec)
```

### Read-only Mode (`--read-only`)

Blocks all write operations at the terminal level before they reach the server. Useful when connecting to a production replica.

Blocked statements: `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `TRUNCATE`, `DROP`, `CREATE`, `ALTER`, `RENAME`, `GRANT`, `REVOKE`

```bash
python TerSQL.py --read-only -H prod-replica -u analyst
```

---

## SSL / TLS Connections

```bash
python TerSQL.py \
  -H secure.db.example.com \
  -u myuser \
  --ssl-ca  /etc/ssl/mysql/ca.pem \
  --ssl-cert /etc/ssl/mysql/client-cert.pem \
  --ssl-key  /etc/ssl/mysql/client-key.pem
```

---

## Non-interactive (Batch) Mode

Use `-e` / `--execute` to run a single query and exit — useful in shell scripts and CI pipelines.

```bash
# Print result to stdout
python TerSQL.py -u root -d mydb -e "SELECT COUNT(*) FROM orders;"

# Machine-readable JSON
python TerSQL.py -u root -d mydb --output json \
  -e "SELECT id, name FROM products LIMIT 10;"

# Export directly to CSV
python TerSQL.py -u root -d mydb --output csv \
  -e "SELECT * FROM customers;" > customers.csv
```

---

## Query History & Bookmarks

**History** is stored in `~/.TerSQL_history`. Use `↑`/`↓` to navigate and `Ctrl+R` for fuzzy search.

```
mysql> .history 5

  #  Timestamp            Elapsed  Query
---  -------------------  -------  ---------------------------------------
 18  2025-01-15 14:02:11   0.0021  SELECT * FROM orders WHERE status='open'
 19  2025-01-15 14:02:45   0.0009  UPDATE orders SET status='closed' WHER…
```

**Bookmarks** are saved to `~/.TerSQL_bookmarks.json`:

```
mysql> .bookmark active_orders SELECT * FROM orders WHERE status = 'active';
[ok] Bookmark 'active_orders' saved.

mysql> .run active_orders
[running] SELECT * FROM orders WHERE status = 'active';
```

---

## Auto-correction

The terminal silently rewrites common shorthand before execution:

| You type | Executed as |
|---|---|
| `show database` | `SHOW DATABASES;` |
| `show table` | `SHOW TABLES;` |
| `desc users` | `DESCRIBE users;` |
| `select * users` | `SELECT * FROM users;` |
| `use mydb` *(no semicolon)* | `USE mydb;` |

---

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `↑` / `↓` | Navigate persistent command history |
| `Ctrl+R` | Fuzzy search through command history |
| `Tab` | Autocomplete SQL keywords, table/column names, dot commands |
| `Ctrl+C` | Clear the current input buffer (does not exit) |
| `Ctrl+D` | Exit the terminal |
| `Ctrl+A` | Move cursor to beginning of line |
| `Ctrl+E` | Move cursor to end of line |
| `Ctrl+W` | Delete the word before the cursor |

---

## Configuration Files

| File | Purpose |
|---|---|
| `~/.TerSQL_history` | Persistent query history |
| `~/.TerSQL_bookmarks.json` | Saved query bookmarks |

Both files are created automatically on first use. Override the history path with `--history <path>`.

---

## Extending TerSQL

The `MySQLTerminal` class is designed to be subclassed.

### Adding a dot command

```python
# 1. Add a handler method
def _cmd_mycommand(self, parts):
    print("Hello from .mycommand!")

# 2. Register it in the dispatch dict inside handle_command()
".mycommand": lambda: self._cmd_mycommand(parts),
```

### Adding SQL auto-corrections

```python
SQL_FIXES = {
    ...
    r"^show\s+columns\s+(\S+);?$": r"SHOW COLUMNS FROM \1;",
}
```

### Adding autocomplete words

```python
terminal._refresh_completer(extra_words=["my_function", "my_stored_proc"])
```

---

## Known Limitations

- **Windows pager**: `.pager` may behave differently — use `.pager more` on Windows.
- **Large result sets**: Results are loaded entirely into memory before display. Use `LIMIT` or `.pager less` for large tables.
- **Stored procedures**: Only the first result set from a multi-result procedure is displayed.
- **Python < 3.10**: The `tuple[bool, str]` type hint requires Python 3.10+. Replace with `Tuple[bool, str]` from `typing` for older versions.

---

## Contributing

Contributions are welcome! Here's how to get started:

1. **Fork** the repository and clone it locally.
2. **Create a branch** for your feature or fix: `git checkout -b feature/my-feature`
3. **Make your changes** and test them against a local MySQL instance.
4. **Open a pull request** with a clear description of what you've changed and why.

Please open an issue before starting large changes so we can discuss the approach first.

### Reporting Bugs

Please include:
- Your OS and Python version (`python --version`)
- MySQL server version
- The exact command or query that triggered the bug
- The full error output

---

## Logging

```bash
python TerSQL.py --log /var/log/TerSQL.log
```

Log format:

```
2025-01-15 14:02:11  INFO      Connected to localhost:3306 as root (server 8.0.35)
2025-01-15 14:02:45  INFO      QUERY: SELECT * FROM users LIMIT 5 | rows=5 | elapsed=0.0021s
2025-01-15 14:03:10  ERROR     Query error: Table 'mydb.foo' doesn't exist
2025-01-15 14:05:01  INFO      Session ended. Total queries: 12
```

---

## Changelog

### v0.0.1 (beta) — Initial Release

- Interactive REPL with syntax highlighting and smart autocompletion
- Persistent query history with fuzzy search (`Ctrl+R`)
- In-session history with timestamps and elapsed times (`.history`)
- Output formats: `grid`, `psql`, `pipe`, `plain`, `simple`, `github`, `markdown`, `html`, `latex`, `rst`, `orgtbl`
- Output modes: `table`, `json`, `csv`, `vertical`
- Export last result to CSV or JSON (`.export`)
- Query bookmarks with persistence (`.bookmark`, `.run`, `.delbookmark`)
- Safe mode and read-only mode
- SSL/TLS connection support
- Query profiling via `SHOW PROFILES` (`.profile`)
- Pager support (`.pager less -S`)
- Tee output logging (`.tee`, `.notee`)
- Source SQL files (`.source`)
- Table diff, copy, row count, and index inspection
- `EXPLAIN` shorthand, process list, and kill
- Non-interactive single-query batch mode (`-e`)
- File-based query log (`--log`)
- Automatic reconnect on dropped connection
- SQL auto-correction for common shorthands
- Full command alias system

---

## License

MIT — see [LICENSE](LICENSE) for details.