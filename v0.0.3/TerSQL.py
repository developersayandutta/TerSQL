#!/usr/bin/env python3
"""
TerSQL.py — CLI entry point v0.0.3
Intelligent multi-database query interface.

Usage:
    python TerSQL.py                          # interactive wizard
    python TerSQL.py -d mysql -H localhost    # direct connect
    python TerSQL.py -e "show users"          # single query, then exit
    python TerSQL.py --read-only -d postgresql

Changes in v0.0.3:
  - Version bumped to 0.0.3
  - .dbs dispatches to list_databases() for MongoDB instead of SHOW DATABASES
  - New dot-commands: .info <table>, .pks <table>, .plugins, .stats
  - HELP_TEXT and DOT_COMMANDS updated accordingly
  - _handle_dot: .schema enriched with table_info() when available
  - Graceful EXPLAIN guard for non-SQL backends wired through TerSQLCore
  - Command aliases expanded
"""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import re
import signal
import sys
from pathlib import Path

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.completion import WordCompleter, Completer, Completion
from prompt_toolkit.styles import Style

from tabulate import tabulate

from NLP import NLPEngine
from Core import TerSQLCore
from plugins.base import PluginRegistry

# Auto-register built-in plugins
import plugins.mysql    # noqa: F401
try:
    import plugins.postgre   # noqa: F401
except ImportError:
    pass
try:
    import plugins.mongodb   # noqa: F401
except ImportError:
    pass

__version__ = "0.0.3"
__status__  = "beta"

# ─────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────

def _setup_logging(log_file: str = None, debug: bool = False):
    level = logging.DEBUG if debug else logging.WARNING
    fmt   = logging.Formatter("%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s")
    root  = logging.getLogger("tersql")
    root.setLevel(level)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    root.addHandler(logging.NullHandler())


# ─────────────────────────────────────────────────────────────
#  Banner
# ─────────────────────────────────────────────────────────────

BANNER = f"""
  ╔══════════════════════════════════════════════════╗
  ║   TerSQL  v{__version__} ({__status__})                      ║
  ║   Intelligent Multi-Database Query Interface     ║
  ╚══════════════════════════════════════════════════╝
  Type SQL or natural language.  .help for commands.
"""

HELP_TEXT = """
╔─────────────────────────────────────────────────────────────╗
│  TerSQL Commands                                            │
├──────────────────────────┬──────────────────────────────────┤
│  .help                   │  Show this help                  │
│  .status                 │  Connection & session status     │
│  .tables                 │  List tables                     │
│  .schema <table>         │  Describe table structure        │
│  .info <table>           │  Enriched table metadata (v0.0.3)│
│  .pks <table>            │  Show primary keys    (v0.0.3)   │
│  .dbs                    │  List databases                  │
│  .use <db>               │  Switch database                 │
│  .plugins                │  List registered plugins(v0.0.3) │
│  .stats                  │  Session query statistics(v0.0.3)│
│  .history [n]            │  Show last n queries             │
│  .export <file> [fmt]    │  Export last result (csv/json)   │
│  .format <fmt>           │  Table display format            │
│  .output <mode>          │  Output mode (table/json/csv/…)  │
│  .explain on/off         │  Toggle EXPLAIN before SELECT    │
│  .timer on/off           │  Toggle timing display           │
│  .safe on/off            │  Toggle safe mode                │
│  .readonly on/off        │  Toggle read-only mode           │
│  .backup                 │  Manual backup now               │
│  .source <file.sql>      │  Execute SQL from file           │
│  .reconnect              │  Reconnect to database           │
│  .clear                  │  Clear screen                    │
│  exit / quit / .q        │  Exit TerSQL                     │
├──────────────────────────┴──────────────────────────────────┤
│  Natural language examples:                                  │
│    show all users                                           │
│    find products where price > 100                          │
│    count orders grouped by status                           │
│    join users and orders on users.id = orders.user_id       │
│    show top 5 employees by salary descending                │
│    average salary in employees per department               │
│    show users where age between 20 and 40      (v0.0.3)    │
│    find products where category in electronics, books       │
╚─────────────────────────────────────────────────────────────╝
"""

DOT_COMMANDS = [
    ".help", ".status", ".tables", ".schema", ".info", ".pks",
    ".dbs", ".use", ".plugins", ".stats",
    ".history", ".export", ".format", ".output", ".explain",
    ".timer", ".safe", ".readonly", ".backup", ".source",
    ".reconnect", ".clear", ".q",
    ".bookmark", ".bookmarks", ".run", ".delbookmark",
]

# ─────────────────────────────────────────────────────────────
#  SQL Keywords for autocompletion
# ─────────────────────────────────────────────────────────────
SQL_KEYWORDS = [
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE',
    'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'ON',
    'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'USE', 'SHOW',
    'DATABASES', 'TABLES', 'DESCRIBE', 'VALUES', 'INTO', 'SET', 'AS', 'DISTINCT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'AND', 'OR', 'NOT', 'NULL', 'IS',
    'IN', 'LIKE', 'ILIKE', 'BETWEEN', 'EXISTS', 'UNION', 'ALL', 'INDEX',
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'DEFAULT', 'AUTO_INCREMENT',
    'UNIQUE', 'CONSTRAINT', 'TRUNCATE', 'COMMIT', 'ROLLBACK', 'BEGIN',
    'TRANSACTION', 'SAVEPOINT', 'RELEASE', 'LOCK', 'UNLOCK', 'CALL',
    'PROCEDURE', 'FUNCTION', 'TRIGGER', 'VIEW', 'EVENT', 'PARTITION',
    'EXPLAIN', 'ANALYZE', 'OPTIMIZE', 'REPAIR', 'CHECK', 'FLUSH', 'RESET',
    'GRANT', 'REVOKE', 'PRIVILEGES', 'IDENTIFIED', 'REPLACE', 'IGNORE',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'IFNULL', 'COALESCE',
    'CAST', 'CONVERT', 'CONCAT', 'SUBSTRING', 'LENGTH', 'TRIM', 'UPPER',
    'LOWER', 'NOW', 'DATE', 'YEAR', 'MONTH', 'DAY', 'TIMESTAMP', 'INTERVAL',
    'CHAR', 'VARCHAR', 'TEXT', 'BLOB', 'INT', 'BIGINT', 'TINYINT', 'SMALLINT',
    'FLOAT', 'DOUBLE', 'DECIMAL', 'BOOLEAN', 'ENUM', 'JSON',
    'WITH', 'RECURSIVE', 'WINDOW', 'OVER', 'ROWS', 'RANGE',
    'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT ROW',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LEAD', 'LAG', 'FIRST_VALUE', 'LAST_VALUE',
]

COMMAND_ALIASES = {
    ".table":     ".tables",
    ".database":  ".db",
    ".databases": ".dbs",
    ".desc":      ".schema",
    ".describe":  ".schema",
    ".quit":      "exit",
    ".exit":      "exit",
    ".ex":        ".export",
    ".fmt":       ".format",
    ".q":         "exit",
    ".h":         ".help",
    ".?":         ".help",
    ".hist":      ".history",
    ".vars":      ".variables",
    ".proc":      ".processlist",
    ".st":        ".status",
    ".bm":        ".bookmark",
    ".prof":      ".profile",
    ".out":       ".output",
    ".fav":       ".bookmarks",
    ".pk":        ".pks",       # v0.0.3
    ".plug":      ".plugins",   # v0.0.3
    ".tableinfo": ".info",      # v0.0.3
}


# ─────────────────────────────────────────────────────────────
#  Connection wizard
# ─────────────────────────────────────────────────────────────

def _connection_wizard() -> dict:
    print("\n  Available databases:", ", ".join(PluginRegistry.available()))
    db_type  = input("  Database type [mysql]: ").strip() or "mysql"
    host     = input("  Host [localhost]: ").strip() or "localhost"
    port_raw = input("  Port [auto]: ").strip()
    defaults = {"mysql": 3306, "postgresql": 5432, "mongodb": 27017}
    port     = int(port_raw) if port_raw else defaults.get(db_type.lower(), 3306)
    user     = input("  User [root]: ").strip() or "root"
    password = getpass.getpass("  Password: ")
    database = input("  Database (optional): ").strip() or None
    return dict(db_type=db_type, host=host, port=port,
                user=user, password=password, database=database)


# ─────────────────────────────────────────────────────────────
#  Smart completer
# ─────────────────────────────────────────────────────────────

class SmartCompleter(Completer):
    """Completer that offers SQL keywords + live schema words."""

    def __init__(self, keywords, schema_words=None, dot_commands=None):
        self.keywords     = [k.upper() for k in keywords]
        self.schema_words = schema_words or []
        self.dot_commands = dot_commands or []

    def get_completions(self, document, complete_event):
        text   = document.text_before_cursor
        word   = document.get_word_before_cursor(WORD=False)
        word_u = word.upper()

        # dot-command completion
        if text.lstrip().startswith("."):
            for cmd in self.dot_commands:
                if cmd.startswith(text.lstrip()):
                    yield Completion(cmd, start_position=-len(text.lstrip()))
            return

        # keyword + schema completion
        candidates = self.keywords + self.schema_words
        for candidate in candidates:
            if candidate.upper().startswith(word_u):
                yield Completion(
                    candidate,
                    start_position=-len(word),
                    display_meta="keyword" if candidate in self.keywords else "schema",
                )


# ─────────────────────────────────────────────────────────────
#  REPL
# ─────────────────────────────────────────────────────────────

class TerSQLREPL:
    def __init__(self, core: TerSQLCore, db_type: str):
        self.core    = core
        self.db_type = db_type
        self._style  = Style.from_dict({
            "prompt":  "#00ccff bold",
        })
        hist_file = os.path.expanduser("~/.tersql_history")
        try:
            self._history = FileHistory(hist_file)
        except Exception:
            self._history = InMemoryHistory()

        self._completer = SmartCompleter(
            SQL_KEYWORDS, dot_commands=DOT_COMMANDS
        )

    def _sync_completer_schema(self):
        words = []
        if getattr(self.core, "_schema_cache", None):
            for tbl, cols in self.core._schema_cache.items():
                words.append(tbl)
                words.extend(c.split(".")[-1] for c in cols)
        self._completer.schema_words = list(set(words))

    def _prompt_str(self) -> str:
        h  = self.core.db.health()
        db = h.current_db or "?"
        return f"TerSQL [{self.db_type}:{db}]> "

    def run(self):
        print(BANNER)
        self.core.sync_schema()
        self._sync_completer_schema()
        self.core.status()

        # Graceful SIGTERM
        def _on_sigterm(sig, frame):
            print("\n  [exit] SIGTERM received")
            self._cleanup()
            sys.exit(0)
        signal.signal(signal.SIGTERM, _on_sigterm)

        buffer = []

        while True:
            try:
                continuation = bool(buffer)
                prompt_str   = "      → " if continuation else self._prompt_str()

                line = prompt(
                    prompt_str,
                    history=self._history,
                    completer=self._completer,
                    style=self._style,
                ).strip()

                if not line:
                    continue

                # Exit
                if not buffer and line.lower() in ("exit", "quit", ".q"):
                    break

                # Comments
                if line.startswith("--") or line.startswith("#"):
                    continue

                # Dot-commands (not in multi-line buffer)
                if not buffer and line.startswith("."):
                    self._handle_dot(line)
                    continue

                # Buffer multi-line SQL
                buffer.append(line)
                if line.rstrip().endswith(";"):
                    full = " ".join(buffer).strip()
                    buffer = []
                    self.core.run(full)
                    self._sync_completer_schema()

            except KeyboardInterrupt:
                if buffer:
                    print("\n  [cleared] Query buffer reset.")
                    buffer = []
                else:
                    print("\n  Use 'exit' to quit.")

            except EOFError:
                break

        self._cleanup()

    def _handle_dot(self, line: str):
        parts   = line.split(None, 2)
        command = parts[0].lower()
        command = COMMAND_ALIASES.get(command, command)

        if command == ".help":
            print(HELP_TEXT)

        elif command == ".status":
            self.core.status()

        elif command == ".tables":
            self.core.run("SHOW TABLES")

        elif command == ".schema":
            if len(parts) < 2:
                print("  Usage: .schema <table>")
            else:
                self.core.run(f"DESCRIBE {parts[1]}")

        # v0.0.3: enriched table metadata
        elif command == ".info":
            if len(parts) < 2:
                print("  Usage: .info <table>")
            else:
                table = parts[1]
                info  = self.core.table_info(table)
                if not info:
                    print(f"  No info available for '{table}'.")
                else:
                    rows = [(k, v) for k, v in info.items()]
                    print(tabulate(rows, headers=["Key", "Value"], tablefmt="simple"))

        # v0.0.3: primary keys
        elif command == ".pks":
            if len(parts) < 2:
                print("  Usage: .pks <table>")
            else:
                table = parts[1]
                pks   = self.core.primary_keys(table)
                if pks:
                    print(f"  Primary key(s) for '{table}': {', '.join(pks)}")
                else:
                    print(f"  No primary keys found for '{table}'.")

        elif command == ".dbs":
            # v0.0.3: MongoDB has its own list_databases(); use it directly
            if self.db_type == "mongodb" and hasattr(self.core.db, "list_databases"):
                dbs = self.core.db.list_databases()
                if dbs:
                    print(tabulate([[d] for d in dbs], headers=["Database"], tablefmt="simple"))
                else:
                    print("  (no databases found)")
            else:
                self.core.run("SHOW DATABASES")

        elif command == ".use":
            if len(parts) < 2:
                print("  Usage: .use <database>")
            else:
                self.core.run(f"USE {parts[1]}")

        # v0.0.3: list registered plugins
        elif command == ".plugins":
            metas = self.core.plugins_info()
            if not metas:
                print("  (no plugins registered)")
            else:
                rows = [
                    (m.name, m.version, m.db_type,
                     "✓" if m.supports_transactions else "✗",
                     "✓" if m.supports_streaming   else "✗",
                     "✓" if m.supports_backup       else "✗",
                     m.description or "—")
                    for m in metas
                ]
                print(tabulate(rows,
                               headers=["Plugin", "Ver", "Type", "Tx", "Stream", "Backup", "Description"],
                               tablefmt="simple"))

        # v0.0.3: session query statistics
        elif command == ".stats":
            if hasattr(self.core.db, "_stats"):
                st = self.core.db.stats.as_dict()
                rows = [(k, v) for k, v in st.items()]
                print(tabulate(rows, headers=["Metric", "Value"], tablefmt="simple"))
            else:
                print("  Stats not available for this plugin.")

        elif command == ".history":
            n = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 20
            self.core.print_history(n)

        elif command == ".export":
            if len(parts) < 2:
                print("  Usage: .export <filename> [csv|json]")
            else:
                fmt = parts[2].lower() if len(parts) >= 3 else (
                    "json" if parts[1].endswith(".json") else "csv"
                )
                self.core.export(parts[1], fmt)

        elif command == ".format":
            if len(parts) < 2:
                print(f"  Current: {self.core.renderer.fmt}")
                print(f"  Options: {', '.join(self.core.renderer.FORMATS)}")
            else:
                self.core.renderer.fmt = parts[1].lower()
                print(f"  Format → {parts[1].lower()}")

        elif command == ".output":
            if len(parts) < 2:
                print(f"  Current: {self.core.renderer.mode}")
                print(f"  Options: {', '.join(self.core.renderer.MODES)}")
            else:
                self.core.renderer.mode = parts[1].lower()
                print(f"  Output mode → {parts[1].lower()}")

        elif command == ".explain":
            toggle = parts[1].lower() if len(parts) >= 2 else None
            if toggle == "on":
                self.core.explain_mode = True
                print("  Explain mode ON")
            elif toggle == "off":
                self.core.explain_mode = False
                print("  Explain mode OFF")
            else:
                self.core.explain_mode = not self.core.explain_mode
                print(f"  Explain mode {'ON' if self.core.explain_mode else 'OFF'}")

        elif command == ".timer":
            toggle = parts[1].lower() if len(parts) >= 2 else None
            if toggle in ("on", "off"):
                self.core.timer_on = (toggle == "on")
            else:
                self.core.timer_on = not self.core.timer_on
            print(f"  Timer {'ON' if self.core.timer_on else 'OFF'}")

        elif command == ".safe":
            toggle = parts[1].lower() if len(parts) >= 2 else "on"
            self.core.safety.safe_mode = (toggle == "on")
            print(f"  Safe mode {'ON' if self.core.safety.safe_mode else 'OFF'}")

        elif command == ".readonly":
            toggle = parts[1].lower() if len(parts) >= 2 else "on"
            self.core.safety.read_only = (toggle == "on")
            print(f"  Read-only {'ON' if self.core.safety.read_only else 'OFF'}")

        elif command == ".backup":
            try:
                path = self.core.db.backup(self.core.BACKUP_DIR)
                print(f"  Backup saved → {path}")
            except NotImplementedError as e:
                print(f"  Backup not supported: {e}")
            except Exception as e:
                print(f"  Backup failed: {e}")

        elif command == ".source":
            if len(parts) < 2:
                print("  Usage: .source <file.sql>")
            else:
                self._exec_file(parts[1])

        elif command == ".reconnect":
            try:
                self.core.db.reconnect()
                self.core.sync_schema()
                self._sync_completer_schema()
                print("  Reconnected.")
            except Exception as e:
                print(f"  Reconnect failed: {e}")

        elif command == ".clear":
            os.system("cls" if os.name == "nt" else "clear")

        elif command == ".bookmark":
            if len(parts) < 3:
                print("  Usage: .bookmark <name> <SQL statement>")
            else:
                self.core.add_bookmark(parts[1], parts[2])

        elif command == ".bookmarks":
            self.core.list_bookmarks()

        elif command == ".run":
            if len(parts) < 2:
                print("  Usage: .run <bookmark_name>")
            else:
                self.core.run_bookmark(parts[1])

        elif command == ".delbookmark":
            if len(parts) < 2:
                print("  Usage: .delbookmark <name>")
            else:
                self.core.del_bookmark(parts[1])

        else:
            print(f"  Unknown command: '{command}'  —  type .help for a list")

    def _exec_file(self, path: str):
        if not os.path.isfile(path):
            print(f"  File not found: {path}")
            return
        try:
            with open(path, encoding="utf-8") as f:
                sql = f.read()
            statements = [s.strip() for s in sql.split(";") if s.strip()
                          and not s.strip().startswith("--")]
            print(f"  Executing {len(statements)} statement(s) from {path}")
            for stmt in statements:
                self.core.run(stmt + ";")
        except Exception as e:
            print(f"  Error reading file: {e}")

    def _cleanup(self):
        try:
            self.core.db.disconnect()
        except Exception:
            pass
        print("\n  Goodbye.\n")


# ─────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=f"TerSQL v{__version__} — intelligent multi-database CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"TerSQL {__version__} ({__status__})")
    parser.add_argument("-d", "--db-type",  default=None,
                        choices=["mysql", "postgresql", "mongodb"],
                        help="Database type")
    parser.add_argument("-H", "--host",     default="localhost")
    parser.add_argument("-P", "--port",     default=None, type=int)
    parser.add_argument("-u", "--user",     default=None)
    parser.add_argument("-p", "--password", default=None)
    parser.add_argument("--database",       default=None, dest="database",
                        help="Initial database/schema")
    parser.add_argument("--format",         default="grid",
                        choices=["grid","psql","pipe","plain","simple","github","markdown","html"])
    parser.add_argument("--output",         default="table",
                        choices=["table","json","csv","vertical"])
    parser.add_argument("--log",            default=None)
    parser.add_argument("--debug",          action="store_true")
    parser.add_argument("--safe-mode",      action="store_true")
    parser.add_argument("--read-only",      action="store_true")
    parser.add_argument("--no-backup",      action="store_true")
    parser.add_argument("--no-timer",       action="store_true")
    parser.add_argument("-e", "--execute",  default=None,
                        help="Execute a single query and exit")

    args = parser.parse_args()
    _setup_logging(args.log, args.debug)

    # ── Gather connection params ──────────────────────────────
    if args.db_type:
        db_type  = args.db_type
        host     = args.host
        defaults = {"mysql": 3306, "postgresql": 5432, "mongodb": 27017}
        port     = args.port or defaults.get(db_type, 3306)
        user     = args.user or input("  User: ").strip() or "root"
        password = args.password or getpass.getpass(f"  Password for {user}@{host}: ")
        database = args.database
    else:
        conn = _connection_wizard()
        db_type  = conn["db_type"]
        host     = conn["host"]
        port     = conn["port"]
        user     = conn["user"]
        password = conn["password"]
        database = conn["database"]

    # ── Load plugin + connect ────────────────────────────────
    try:
        PluginClass = PluginRegistry.load_plugin(db_type)
    except (ImportError, KeyError) as e:
        print(f"\n  Error loading plugin for '{db_type}': {e}")
        sys.exit(1)

    db = PluginClass()
    try:
        db.connect(
            host=host, port=port,
            user=user, password=password,
            database=database or "",
        )
    except ConnectionError as e:
        print(f"\n  Connection failed: {e}")
        sys.exit(1)

    # ── NLP engine ───────────────────────────────────────────
    intent_path = str(Path(__file__).parent / "Intent.json")
    nlp = NLPEngine(
        intent_path=intent_path,
        dialect=db_type,
    )

    # ── Core ─────────────────────────────────────────────────
    core = TerSQLCore(
        db=db,
        nlp=nlp,
        output_mode=args.output,
        table_format=args.format,
        safe_mode=args.safe_mode,
        read_only=args.read_only,
        auto_backup=not args.no_backup,
        timer_on=not args.no_timer,
    )

    # ── Single-query mode ────────────────────────────────────
    if args.execute:
        core.sync_schema()
        q = args.execute.strip()
        if not q.endswith(";"):
            q += ";"
        core.run(q)
        db.disconnect()
        return

    # ── Interactive REPL ─────────────────────────────────────
    repl = TerSQLREPL(core, db_type)
    repl.run()


if __name__ == "__main__":
    main()