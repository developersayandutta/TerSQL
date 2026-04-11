#!/usr/bin/env python3
"""
mysql_terminal.py — Interactive MySQL Terminal
Version : 0.0.1 (beta)
Author  : mysql-terminal contributors
License : MIT
"""

__version__ = "0.0.1"
__status__  = "beta"

import os
import re
import csv
import sys
import json
import time
import logging
import argparse
import getpass
import textwrap
import datetime
import signal
import shutil
import subprocess
from collections import deque
from pathlib import Path

import mysql.connector
from mysql.connector import Error
from tabulate import tabulate
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.completion import WordCompleter, Completer, Completion
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML
from pygments.lexers.sql import SqlLexer

# ─────────────────────────────────────────────
#  Version banner
# ─────────────────────────────────────────────
VERSION_BANNER = f"""
MySQL NLP Terminal  v{__version__} ({__status__})
Type '.help' for commands, 'exit' to quit.
"""

# ─────────────────────────────────────────────
#  SQL Keywords for autocompletion
# ─────────────────────────────────────────────
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
    'WITH', 'RECURSIVE', 'WINDOW', 'OVER', 'PARTITION', 'ROWS', 'RANGE',
    'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT ROW',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LEAD', 'LAG', 'FIRST_VALUE', 'LAST_VALUE',
]

TABLE_FORMATS = ["grid", "psql", "pipe", "orgtbl", "plain", "simple", "github", "markdown", "html", "latex", "rst"]

OUTPUT_MODES = ["table", "csv", "json", "vertical"]

# ─────────────────────────────────────────────
#  Input normalisation
# ─────────────────────────────────────────────
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
}

SQL_FIXES = {
    r"^show\s+database;?$":          "SHOW DATABASES;",
    r"^show\s+table;?$":             "SHOW TABLES;",
    r"^show\s+dbs?;?$":              "SHOW DATABASES;",
    r"^desc\s+(\S+);?$":             r"DESCRIBE \1;",
    r"^select\s+\*\s+(\S+);?$":      r"SELECT * FROM \1;",
    r"^use\s+(\S+)$":                r"USE \1;",
}

HELP_TEXT = """
+------------------------------------------------------------------+
|          MySQL Terminal v{ver} ({status}) - Commands           |
+--------------------------------+---------------------------------+
| .help                          | Show this help menu             |
| .version                       | Show version information        |
| .clear                         | Clear the terminal screen       |
| .db                            | Show currently selected DB      |
| .tables                        | List tables in current DB       |
| .schema <table>                | Describe table structure        |
| .dbs                           | List all databases              |
| .export <file> [format]        | Export last result (csv/json)   |
| .format <fmt>                  | Set table display format        |
| .output <mode>                 | Set output mode (table/json/…)  |
| .drop <table>                  | Drop a table (with confirm)     |
| .reconnect                     | Re-connect to MySQL server      |
| .status                        | Show connection status          |
| .processlist                   | Show running processes          |
| .variables [pattern]           | Show MySQL variables            |
| .history [n]                   | Show last n query history       |
| .bookmark <name> <sql>         | Save a query bookmark           |
| .bookmarks                     | List all saved bookmarks        |
| .run <name>                    | Run a saved bookmark            |
| .delbookmark <name>            | Delete a bookmark               |
| .profile on/off                | Toggle query profiling          |
| .warnings                      | Show warnings from last query   |
| .source <file.sql>             | Execute SQL from a file         |
| .pager [cmd]                   | Set pager (e.g. less, more)     |
| .notee                         | Stop writing output to log file |
| .tee <file>                    | Copy output to a file (append)  |
| .charset <name>                | Set character set               |
| .indexes <table>               | Show indexes for a table        |
| .rowcount                      | Show row counts for all tables  |
| .copy <src> <dst>              | Copy table structure            |
| .diff <t1> <t2>                | Diff two table schemas          |
| .kill <pid>                    | Kill a process by ID            |
| .explain <query>               | EXPLAIN a SELECT statement      |
| .timer on/off                  | Toggle query execution timer    |
| exit / quit                    | Exit the terminal               |
+--------------------------------+---------------------------------+
| Output formats : grid (default), psql, pipe, plain, markdown,   |
|                  html, latex, rst, orgtbl, github               |
| Output modes   : table (default), csv, json, vertical           |
| Aliases: .h / .? -> .help   .q -> exit   .hist -> .history      |
|          .vars -> .variables   .proc -> .processlist            |
|          .st -> .status   .bm -> .bookmark   .out -> .output    |
+------------------------------------------------------------------+
""".format(ver=__version__, status=__status__)


# ─────────────────────────────────────────────────────────────
#  Smart completer
# ─────────────────────────────────────────────────────────────

class SmartCompleter(Completer):
    """Completer that offers SQL keywords + live schema words."""

    def __init__(self, keywords, schema_words=None):
        self.keywords     = [k.upper() for k in keywords]
        self.schema_words = schema_words or []
        self.dot_commands = [
            ".help", ".clear", ".db", ".tables", ".schema", ".dbs", ".export",
            ".format", ".output", ".drop", ".reconnect", ".status", ".processlist",
            ".variables", ".history", ".bookmark", ".bookmarks", ".run",
            ".delbookmark", ".profile", ".warnings", ".source", ".pager",
            ".tee", ".notee", ".charset", ".indexes", ".rowcount", ".copy",
            ".diff", ".kill", ".explain", ".timer", ".version",
        ]

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
#  Main class
# ─────────────────────────────────────────────────────────────

class MySQLTerminal:
    def __init__(self, host, port, user, password, database=None,
                 table_format="grid", log_file=None,
                 history_file=None, output_mode="table",
                 ssl_ca=None, ssl_cert=None, ssl_key=None,
                 connect_timeout=10, charset="utf8mb4",
                 safe_mode=False, read_only=False):

        self.config = {
            "host":             host,
            "port":             port,
            "user":             user,
            "password":         password,
            "connection_timeout": connect_timeout,
            "charset":          charset,
        }
        if database:
            self.config["database"] = database
        if ssl_ca:
            self.config["ssl_ca"]   = ssl_ca
        if ssl_cert:
            self.config["ssl_cert"] = ssl_cert
        if ssl_key:
            self.config["ssl_key"]  = ssl_key

        self.conn          = None
        self.cursor        = None
        self.current_db    = database
        self.last_result   = None
        self.last_columns  = None
        self.last_query    = None
        self.last_error    = None
        self.table_format  = table_format if table_format in TABLE_FORMATS else "grid"
        self.output_mode   = output_mode if output_mode in OUTPUT_MODES else "table"
        self.safe_mode     = safe_mode    # refuse DELETE/DROP without WHERE/confirm
        self.read_only     = read_only    # refuse DML entirely
        self.timer_on      = True
        self.profile_on    = False
        self.pager         = None         # e.g. "less -S"
        self.tee_file      = None         # file handle for .tee
        self.charset       = charset
        self.connect_time  = None
        self.query_count   = 0
        self.session_start = datetime.datetime.now()

        # Query history (in-memory ring buffer + optional file)
        self._query_history: deque = deque(maxlen=500)
        history_path = history_file or os.path.expanduser("~/.mysql_terminal_history")
        try:
            self._pt_history = FileHistory(history_path)
        except Exception:
            self._pt_history = InMemoryHistory()

        # Bookmarks: {name: sql}
        self._bookmarks: dict = {}
        self._bookmarks_file = os.path.expanduser("~/.mysql_terminal_bookmarks.json")
        self._load_bookmarks()

        # ── Logger ─────────────────────────────────────────────
        self.logger = logging.getLogger("mysql_terminal")
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        if log_file:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(fmt)
            self.logger.addHandler(fh)
        self.logger.addHandler(logging.NullHandler())

        # ── Prompt style ───────────────────────────────────────
        self.style = Style.from_dict({
            "prompt":       "#00aaff bold",
            "continuation": "#888888",
        })
        self._refresh_completer()

    # ──────────────────────────────────────────
    #  Bookmarks persistence
    # ──────────────────────────────────────────
    def _load_bookmarks(self):
        try:
            if os.path.exists(self._bookmarks_file):
                with open(self._bookmarks_file, encoding="utf-8") as f:
                    self._bookmarks = json.load(f)
        except Exception:
            self._bookmarks = {}

    def _save_bookmarks(self):
        try:
            with open(self._bookmarks_file, "w", encoding="utf-8") as f:
                json.dump(self._bookmarks, f, indent=2)
        except Exception as e:
            print(f"[warn] Could not save bookmarks: {e}")

    # ──────────────────────────────────────────
    #  Autocompletion
    # ──────────────────────────────────────────
    def _refresh_completer(self, extra_words=None):
        words = list(SQL_KEYWORDS)
        if extra_words:
            words.extend(extra_words)
        self.sql_completer = SmartCompleter(words, schema_words=extra_words or [])

    def _fetch_schema_words(self):
        words = []
        if not self.current_db or not self.conn:
            return words
        try:
            cur = self.conn.cursor(buffered=True)
            cur.execute("SHOW TABLES;")
            tables = [row[0] for row in cur.fetchall()]
            words.extend(tables)
            for tbl in tables:
                cur.execute(f"DESCRIBE `{tbl}`;")
                words.extend(row[0] for row in cur.fetchall())
            cur.close()
        except Error:
            pass
        return words

    # ──────────────────────────────────────────
    #  Output helpers
    # ──────────────────────────────────────────
    def _render_result(self, rows, columns):
        """Render a result set according to the current output_mode."""
        if self.output_mode == "json":
            output = json.dumps(
                [dict(zip(columns, [str(v) if not isinstance(v, (int, float, type(None))) else v
                                    for v in row]))
                 for row in rows],
                indent=2, default=str, ensure_ascii=False
            )
        elif self.output_mode == "csv":
            import io
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(columns)
            w.writerows(rows)
            output = buf.getvalue().rstrip()
        elif self.output_mode == "vertical":
            parts = []
            width = max((len(c) for c in columns), default=0)
            for i, row in enumerate(rows):
                parts.append(f"*************************** {i+1}. row ***************************")
                for col, val in zip(columns, row):
                    parts.append(f"{col.rjust(width)}: {val}")
            output = "\n".join(parts)
        else:
            output = tabulate(rows, headers=columns, tablefmt=self.table_format)

        self._print_output(output)

    def _print_output(self, text: str):
        """Print text, optionally via pager and/or tee file."""
        if self.pager:
            try:
                proc = subprocess.Popen(
                    self.pager, shell=True, stdin=subprocess.PIPE,
                    encoding="utf-8", errors="replace"
                )
                proc.communicate(input=text)
                return
            except Exception:
                pass
        print(text)
        if self.tee_file:
            try:
                self.tee_file.write(text + "\n")
                self.tee_file.flush()
            except Exception:
                pass

    # ──────────────────────────────────────────
    #  Guard rails
    # ──────────────────────────────────────────
    def _check_safety(self, query: str) -> bool:
        """
        If safe_mode or read_only is active, inspect the query and optionally
        block or require confirmation.  Returns True = proceed, False = cancel.
        """
        q = query.strip().upper()
        first_token = q.split()[0] if q.split() else ""

        if self.read_only:
            dml = {"INSERT", "UPDATE", "DELETE", "REPLACE", "TRUNCATE",
                   "DROP", "CREATE", "ALTER", "RENAME", "GRANT", "REVOKE"}
            if first_token in dml:
                print(f"[read-only mode] Statement blocked: {first_token}")
                return False
            return True

        if self.safe_mode:
            dangerous = {
                "DELETE": r"\bWHERE\b",
                "UPDATE": r"\bWHERE\b",
                "DROP":   None,
                "TRUNCATE": None,
            }
            if first_token in dangerous:
                pattern = dangerous[first_token]
                if pattern and not re.search(pattern, q):
                    print(f"[safe-mode] {first_token} without WHERE is blocked. "
                          f"Add a WHERE clause or use --no-safe-mode.")
                    return False
                if first_token in ("DROP", "TRUNCATE"):
                    confirm = input(f"[safe-mode] Are you sure you want to {first_token}? (yes/no): ").strip().lower()
                    return confirm == "yes"
        return True

    # ──────────────────────────────────────────
    #  Connection
    # ──────────────────────────────────────────
    def connect(self):
        try:
            self.conn         = mysql.connector.connect(**self.config)
            self.cursor       = self.conn.cursor(buffered=True)
            self.connect_time = datetime.datetime.now()
            ver = self.conn.get_server_info()
            print(f"[connected] {self.config['host']}:{self.config['port']}  "
                  f"server={ver}  user={self.config['user']}")
            self.logger.info("Connected to %s:%s as %s (server %s)",
                             self.config["host"], self.config["port"],
                             self.config["user"], ver)
            if self.current_db:
                self._refresh_completer(self._fetch_schema_words())
        except Error as e:
            print(f"[error] Connection failed: {e}")
            self.logger.error("Connection failed: %s", e)
            raise SystemExit(1)

    def reconnect(self):
        try:
            if self.cursor: self.cursor.close()
            if self.conn:   self.conn.close()
        except Exception:
            pass
        print("[info] Reconnecting...")
        self.connect()
        if self.current_db:
            self._safe_execute(f"USE `{self.current_db}`;", internal=True)

    def _ensure_connection(self):
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=1)
        except Exception:
            print("[warn] Connection lost, attempting reconnect...")
            self.logger.warning("Connection lost, attempting reconnect")
            self.reconnect()

    # ──────────────────────────────────────────
    #  Query execution
    # ──────────────────────────────────────────
    def _safe_execute(self, query, params=None, internal=False) -> tuple[bool, str]:
        self._ensure_connection()
        try:
            q = query.strip()
            # Auto-corrections
            for pattern, fix in SQL_FIXES.items():
                if fix is None:
                    continue
                m = re.match(pattern, q, re.IGNORECASE)
                if m:
                    corrected = re.sub(pattern, fix, q, flags=re.IGNORECASE)
                    if corrected.lower() != q.lower() and not internal:
                        print(f"[auto-fix] {corrected}")
                    query = corrected
                    break

            # Profiling
            if self.profile_on and not internal:
                self._safe_execute("SET profiling = 1;", internal=True)

            t0 = time.perf_counter()
            self.cursor.execute(query, params or ())
            elapsed = time.perf_counter() - t0

            if self.cursor.with_rows:
                rows    = self.cursor.fetchall()
                columns = [desc[0] for desc in self.cursor.description]
                self.last_result  = rows
                self.last_columns = columns
                if not internal:
                    if rows:
                        self._render_result(rows, columns)
                    else:
                        print("(empty set)")
                    print(f"\n{len(rows)} row(s) returned", end="")
                    if self.timer_on:
                        print(f"  ({elapsed:.4f} sec)", end="")
                    print()
            else:
                self.conn.commit()
                self.last_result = None
                if not internal:
                    print(f"Query OK  {self.cursor.rowcount} row(s) affected", end="")
                    if self.timer_on:
                        print(f"  ({elapsed:.4f} sec)", end="")
                    print()

            # Profiling output
            if self.profile_on and not internal:
                self._safe_execute("SHOW PROFILES;", internal=False)

            # Track USE <db>
            match = re.match(r'use\s+[`"]?(\w+)[`"]?', query.strip(), re.IGNORECASE)
            if match:
                self.current_db = match.group(1)
                if not internal:
                    print(f"[database] {self.current_db}")
                self._refresh_completer(self._fetch_schema_words())

            # Warnings check
            if not internal and self.cursor.warning_count and self.cursor.warning_count > 0:
                print(f"[warning] {self.cursor.warning_count} warning(s). Run .warnings to view.")

            self.last_query = query
            self.last_error = None
            self.query_count += 1
            self._query_history.append({
                "n":       self.query_count,
                "ts":      datetime.datetime.now().isoformat(timespec="seconds"),
                "elapsed": round(elapsed, 4),
                "query":   query[:200],
            })
            self.logger.info("QUERY: %s | rows=%s | elapsed=%.4fs",
                             query[:120], self.cursor.rowcount, elapsed)
            return True, ""

        except Error as e:
            err_msg = str(e)
            if not internal:
                print(f"[error] {e}")
            self.logger.error("Query error: %s | query=%s", e, query[:120])
            self.last_error = err_msg
            try:
                self.cursor = self.conn.cursor(buffered=True)
            except Exception:
                pass
            return False, err_msg

    def execute(self, query) -> bool:
        if not self._check_safety(query):
            return False
        ok, _ = self._safe_execute(query)
        return ok

    # ──────────────────────────────────────────
    #  Multi-statement support
    # ──────────────────────────────────────────
    def execute_script(self, sql: str):
        """Execute multiple semicolon-delimited statements."""
        # Split on ; not inside quotes
        statements = re.split(r';\s*(?=(?:[^\'\"]*[\'"][^\'\"]*[\'"])*[^\'\"]*$)', sql)
        for stmt in statements:
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--") and not stmt.startswith("#"):
                self.execute(stmt + ";")

    # ──────────────────────────────────────────
    #  Dot-commands
    # ──────────────────────────────────────────
    def handle_command(self, cmd: str):
        parts   = cmd.strip().split(None, 2)   # max 3 parts for bookmark names w/ spaces
        command = parts[0].lower()
        command = COMMAND_ALIASES.get(command, command)
        parts[0] = command

        dispatch = {
            ".help":         self._cmd_help,
            ".version":      self._cmd_version,
            ".clear":        self._cmd_clear,
            ".db":           self._cmd_db,
            ".dbs":          lambda: self.execute("SHOW DATABASES;"),
            ".tables":       self._cmd_tables,
            ".schema":       lambda: self._cmd_schema(parts),
            ".export":       lambda: self._cmd_export(parts),
            ".format":       lambda: self._cmd_format(parts),
            ".output":       lambda: self._cmd_output(parts),
            ".drop":         lambda: self._cmd_drop(parts),
            ".reconnect":    self.reconnect,
            ".status":       self._cmd_status,
            ".processlist":  lambda: self.execute("SHOW FULL PROCESSLIST;"),
            ".variables":    lambda: self._cmd_variables(parts),
            ".history":      lambda: self._cmd_history(parts),
            ".bookmark":     lambda: self._cmd_bookmark(parts),
            ".bookmarks":    self._cmd_list_bookmarks,
            ".run":          lambda: self._cmd_run_bookmark(parts),
            ".delbookmark":  lambda: self._cmd_del_bookmark(parts),
            ".profile":      lambda: self._cmd_profile(parts),
            ".warnings":     lambda: self.execute("SHOW WARNINGS;"),
            ".source":       lambda: self._cmd_source(parts),
            ".pager":        lambda: self._cmd_pager(parts),
            ".tee":          lambda: self._cmd_tee(parts),
            ".notee":        self._cmd_notee,
            ".charset":      lambda: self._cmd_charset(parts),
            ".indexes":      lambda: self._cmd_indexes(parts),
            ".rowcount":     self._cmd_rowcount,
            ".copy":         lambda: self._cmd_copy(parts),
            ".diff":         lambda: self._cmd_diff(parts),
            ".kill":         lambda: self._cmd_kill(parts),
            ".explain":      lambda: self._cmd_explain(parts),
            ".timer":        lambda: self._cmd_timer(parts),
        }

        fn = dispatch.get(command)
        if fn:
            fn()
        else:
            print(f"[error] Unknown command: '{command}'")
            known = list(dispatch.keys())
            close = [k for k in known if k.startswith(command[:3])]
            if close:
                print(f"[hint] Did you mean: {' | '.join(close[:4])}")
            else:
                print("[hint] Type .help for available commands.")

    # ── Individual command handlers ─────────────

    def _cmd_help(self):
        self._print_output(HELP_TEXT)

    def _cmd_version(self):
        server_ver = "(not connected)"
        if self.conn:
            try:
                server_ver = self.conn.get_server_info()
            except Exception:
                pass
        print(f"MySQL Terminal   : v{__version__} ({__status__})")
        print(f"MySQL Server     : {server_ver}")
        print(f"Python           : {sys.version.split()[0]}")
        try:
            import mysql.connector as mc
            print(f"mysql-connector  : {mc.__version__}")
        except Exception:
            pass
        try:
            import tabulate as tb
            print(f"tabulate         : {tb.__version__}")
        except Exception:
            pass

    def _cmd_clear(self):
        os.system("cls" if os.name == "nt" else "clear")

    def _cmd_db(self):
        print(f"Current database: {self.current_db or '(none)'}")

    def _cmd_tables(self):
        if not self.current_db:
            print("[warn] Select a database first:  USE db_name;")
            return
        self.execute("SHOW TABLES;")

    def _cmd_schema(self, parts):
        if len(parts) < 2:
            print("[usage] .schema <table_name>")
            return
        self.execute(f"DESCRIBE `{parts[1]}`;")

    def _cmd_export(self, parts):
        """
        .export <filename> [csv|json]
        Exports the last SELECT result to a file.
        """
        if len(parts) < 2:
            print("[usage] .export <filename.csv|filename.json> [csv|json]")
            return
        filename = parts[1]
        fmt = parts[2].lower() if len(parts) >= 3 else None
        if fmt is None:
            fmt = "json" if filename.endswith(".json") else "csv"
        self.export_result(filename, fmt)

    def _cmd_format(self, parts):
        if len(parts) < 2:
            print(f"[usage] .format <{'|'.join(TABLE_FORMATS)}>")
            print(f"[current] {self.table_format}")
            return
        fmt = parts[1].lower()
        if fmt not in TABLE_FORMATS:
            print(f"[error] Unknown format '{fmt}'. Choose: {', '.join(TABLE_FORMATS)}")
            return
        self.table_format = fmt
        print(f"[ok] Table format set to '{fmt}'")

    def _cmd_output(self, parts):
        if len(parts) < 2:
            print(f"[usage] .output <{'|'.join(OUTPUT_MODES)}>")
            print(f"[current] {self.output_mode}")
            return
        mode = parts[1].lower()
        if mode not in OUTPUT_MODES:
            print(f"[error] Unknown output mode '{mode}'. Choose: {', '.join(OUTPUT_MODES)}")
            return
        self.output_mode = mode
        print(f"[ok] Output mode set to '{mode}'")

    def _cmd_drop(self, parts):
        if len(parts) < 2:
            print("[usage] .drop <table_name>")
            return
        table   = parts[1]
        confirm = input(f"[confirm] Drop table '{table}'? Type 'yes' to proceed: ")
        if confirm.strip().lower() == "yes":
            self.execute(f"DROP TABLE `{table}`;")
        else:
            print("[cancelled]")

    def _cmd_status(self):
        now   = datetime.datetime.now()
        uptime = str(now - self.session_start).split(".")[0]
        print(f"{'─'*50}")
        print(f"  Terminal version : v{__version__} ({__status__})")
        print(f"  Host             : {self.config['host']}:{self.config['port']}")
        print(f"  User             : {self.config['user']}")
        print(f"  Database         : {self.current_db or '(none)'}")
        print(f"  Charset          : {self.charset}")
        print(f"  Session start    : {self.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Session uptime   : {uptime}")
        print(f"  Queries run      : {self.query_count}")
        print(f"  Timer            : {'on' if self.timer_on else 'off'}")
        print(f"  Safe mode        : {'on' if self.safe_mode else 'off'}")
        print(f"  Read-only mode   : {'on' if self.read_only else 'off'}")
        print(f"  Output mode      : {self.output_mode}")
        print(f"  Table format     : {self.table_format}")
        print(f"  Profiling        : {'on' if self.profile_on else 'off'}")
        if self.tee_file:
            print(f"  Tee file         : {self.tee_file.name}")
        if self.pager:
            print(f"  Pager            : {self.pager}")
        print(f"{'─'*50}")

    def _cmd_variables(self, parts):
        pattern = parts[1] if len(parts) >= 2 else None
        if pattern:
            self.execute(f"SHOW VARIABLES LIKE '%{pattern}%';")
        else:
            self.execute("SHOW VARIABLES;")

    def _cmd_history(self, parts):
        n = 20
        if len(parts) >= 2:
            try:
                n = int(parts[1])
            except ValueError:
                pass
        history = list(self._query_history)[-n:]
        if not history:
            print("(no query history)")
            return
        rows = [(h["n"], h["ts"], h["elapsed"], h["query"]) for h in history]
        print(tabulate(rows,
                       headers=["#", "Timestamp", "Elapsed", "Query"],
                       tablefmt="simple"))

    def _cmd_bookmark(self, parts):
        # .bookmark <name> <sql ...>
        if len(parts) < 3:
            print("[usage] .bookmark <name> <SQL statement>")
            return
        name = parts[1]
        sql  = parts[2]
        self._bookmarks[name] = sql
        self._save_bookmarks()
        print(f"[ok] Bookmark '{name}' saved.")

    def _cmd_list_bookmarks(self):
        if not self._bookmarks:
            print("(no bookmarks saved)")
            return
        rows = [(k, v[:80]) for k, v in self._bookmarks.items()]
        print(tabulate(rows, headers=["Name", "SQL"], tablefmt="simple"))

    def _cmd_run_bookmark(self, parts):
        if len(parts) < 2:
            print("[usage] .run <bookmark_name>")
            return
        name = parts[1]
        sql  = self._bookmarks.get(name)
        if not sql:
            print(f"[error] Bookmark '{name}' not found. Use .bookmarks to list.")
            return
        print(f"[running] {sql}")
        self.execute(sql if sql.endswith(";") else sql + ";")

    def _cmd_del_bookmark(self, parts):
        if len(parts) < 2:
            print("[usage] .delbookmark <name>")
            return
        name = parts[1]
        if name in self._bookmarks:
            del self._bookmarks[name]
            self._save_bookmarks()
            print(f"[ok] Bookmark '{name}' deleted.")
        else:
            print(f"[error] Bookmark '{name}' not found.")

    def _cmd_profile(self, parts):
        if len(parts) < 2:
            state = "on" if self.profile_on else "off"
            print(f"[profile] Profiling is currently {state}. Use: .profile on / .profile off")
            return
        toggle = parts[1].lower()
        if toggle == "on":
            self.profile_on = True
            self._safe_execute("SET profiling = 1;", internal=True)
            print("[profile] Query profiling ON")
        elif toggle == "off":
            self.profile_on = False
            self._safe_execute("SET profiling = 0;", internal=True)
            print("[profile] Query profiling OFF")
        else:
            print("[usage] .profile on | .profile off")

    def _cmd_source(self, parts):
        if len(parts) < 2:
            print("[usage] .source <file.sql>")
            return
        path = parts[1]
        if not os.path.isfile(path):
            print(f"[error] File not found: {path}")
            return
        try:
            with open(path, encoding="utf-8") as f:
                sql = f.read()
            print(f"[source] Executing {path} ...")
            self.execute_script(sql)
        except Exception as e:
            print(f"[error] Could not read file: {e}")

    def _cmd_pager(self, parts):
        if len(parts) < 2:
            self.pager = None
            print("[pager] Pager disabled.")
            return
        self.pager = parts[1]
        print(f"[pager] Pager set to: {self.pager}")

    def _cmd_tee(self, parts):
        if len(parts) < 2:
            print("[usage] .tee <filename>")
            return
        path = parts[1]
        if self.tee_file:
            self.tee_file.close()
        try:
            self.tee_file = open(path, "a", encoding="utf-8")
            print(f"[tee] Appending output to: {path}")
        except Exception as e:
            print(f"[error] Cannot open file: {e}")
            self.tee_file = None

    def _cmd_notee(self):
        if self.tee_file:
            self.tee_file.close()
            self.tee_file = None
            print("[tee] Output logging stopped.")
        else:
            print("[tee] No tee file active.")

    def _cmd_charset(self, parts):
        if len(parts) < 2:
            print(f"[charset] Current charset: {self.charset}")
            return
        cs = parts[1]
        try:
            self._safe_execute(f"SET NAMES {cs};", internal=True)
            self.charset = cs
            self.config["charset"] = cs
            print(f"[charset] Character set changed to: {cs}")
        except Exception as e:
            print(f"[error] {e}")

    def _cmd_indexes(self, parts):
        if len(parts) < 2:
            print("[usage] .indexes <table_name>")
            return
        self.execute(f"SHOW INDEXES FROM `{parts[1]}`;")

    def _cmd_rowcount(self):
        if not self.current_db:
            print("[warn] No database selected.")
            return
        try:
            cur = self.conn.cursor(buffered=True)
            cur.execute("SHOW TABLES;")
            tables = [row[0] for row in cur.fetchall()]
            rows = []
            for tbl in tables:
                cur.execute(f"SELECT COUNT(*) FROM `{tbl}`;")
                cnt = cur.fetchone()[0]
                rows.append((tbl, cnt))
            cur.close()
            rows.sort(key=lambda x: x[1], reverse=True)
            print(tabulate(rows, headers=["Table", "Row Count"], tablefmt="simple"))
        except Error as e:
            print(f"[error] {e}")

    def _cmd_copy(self, parts):
        """Copy the DDL of a table to create a new (empty) table with the same structure."""
        if len(parts) < 3:
            print("[usage] .copy <source_table> <dest_table>")
            return
        src, dst = parts[1], parts[2]
        self.execute(f"CREATE TABLE `{dst}` LIKE `{src}`;")

    def _cmd_diff(self, parts):
        """Show structural differences between two tables."""
        if len(parts) < 3:
            print("[usage] .diff <table1> <table2>")
            return
        t1, t2 = parts[1], parts[2]
        try:
            cur = self.conn.cursor(buffered=True)
            def get_cols(tbl):
                cur.execute(f"DESCRIBE `{tbl}`;")
                return {row[0]: row for row in cur.fetchall()}
            c1 = get_cols(t1)
            c2 = get_cols(t2)
            cur.close()
        except Error as e:
            print(f"[error] {e}")
            return

        all_cols = sorted(set(c1) | set(c2))
        diff_rows = []
        for col in all_cols:
            in1 = col in c1
            in2 = col in c2
            if in1 and not in2:
                diff_rows.append((col, str(c1[col][1]), "<only in {}>".format(t1)))
            elif in2 and not in1:
                diff_rows.append((col, "<only in {}>".format(t2), str(c2[col][1])))
            elif c1[col][1] != c2[col][1]:
                diff_rows.append((col, str(c1[col][1]), str(c2[col][1])))

        if not diff_rows:
            print(f"[diff] Tables '{t1}' and '{t2}' have identical structures.")
        else:
            print(tabulate(diff_rows, headers=["Column", t1, t2], tablefmt="simple"))

    def _cmd_kill(self, parts):
        if len(parts) < 2:
            print("[usage] .kill <process_id>")
            return
        pid = parts[1]
        confirm = input(f"[confirm] Kill process {pid}? (yes/no): ").strip().lower()
        if confirm == "yes":
            self.execute(f"KILL {pid};")
        else:
            print("[cancelled]")

    def _cmd_explain(self, parts):
        if len(parts) < 2:
            print("[usage] .explain <SELECT ...>")
            return
        sql = " ".join(parts[1:])
        if not sql.upper().startswith("SELECT"):
            print("[warn] EXPLAIN is most useful with SELECT statements.")
        self.execute(f"EXPLAIN {sql};")

    def _cmd_timer(self, parts):
        if len(parts) < 2:
            state = "on" if self.timer_on else "off"
            print(f"[timer] Timer is currently {state}. Use: .timer on / .timer off")
            return
        toggle = parts[1].lower()
        if toggle == "on":
            self.timer_on = True
            print("[timer] Query timing ON")
        elif toggle == "off":
            self.timer_on = False
            print("[timer] Query timing OFF")
        else:
            print("[usage] .timer on | .timer off")

    # ──────────────────────────────────────────
    #  Export
    # ──────────────────────────────────────────
    def export_result(self, filename: str, fmt: str = "csv"):
        if not self.last_result or not self.last_columns:
            print("[error] No data to export. Run a SELECT query first.")
            return
        if os.path.isabs(filename) or ".." in filename:
            confirm = input(f"[confirm] Writing to '{filename}'. Proceed? (yes/no): ")
            if confirm.lower() != "yes":
                print("[cancelled]")
                return
        try:
            if fmt == "json":
                data = [dict(zip(self.last_columns,
                                 [str(v) if not isinstance(v, (int, float, type(None))) else v
                                  for v in row]))
                        for row in self.last_result]
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, default=str, ensure_ascii=False)
            else:
                with open(filename, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(self.last_columns)
                    writer.writerows(self.last_result)
            print(f"[ok] Exported {len(self.last_result)} row(s) to {filename} ({fmt})")
            self.logger.info("Exported %d rows to %s (%s)", len(self.last_result), filename, fmt)
        except Exception as e:
            print(f"[error] Export failed: {e}")
            self.logger.error("Export failed: %s", e)

    # ──────────────────────────────────────────
    #  Main REPL loop
    # ──────────────────────────────────────────
    def _get_prompt(self, continuation=False):
        if continuation:
            return "      -> "
        if self.current_db:
            return f"mysql [{self.current_db}]> "
        return "mysql> "

    def run(self):
        print(VERSION_BANNER)
        query_buffer = []

        # Graceful SIGTERM handling
        def _sigterm(sig, frame):
            print("\n[exit] SIGTERM received, exiting.")
            self.close()
            sys.exit(0)
        signal.signal(signal.SIGTERM, _sigterm)

        while True:
            try:
                line = prompt(
                    self._get_prompt(bool(query_buffer)),
                    history=self._pt_history,
                    completer=self.sql_completer,
                    lexer=PygmentsLexer(SqlLexer),
                    style=self.style,
                ).strip()

                if not line:
                    continue

                # exit / quit
                if not query_buffer and line.lower() in ("exit", "quit"):
                    break

                # single-line comment passthrough
                if line.startswith("--") or line.startswith("#"):
                    continue

                # dot-commands
                if not query_buffer and line.startswith("."):
                    self.handle_command(line)
                    continue

                # Multi-line SQL buffer
                query_buffer.append(line)
                if line.rstrip().endswith(";"):
                    full_query = " ".join(query_buffer)
                    self.execute(full_query)
                    query_buffer = []

            except KeyboardInterrupt:
                if query_buffer:
                    print("\n[info] Query buffer cleared.")
                    query_buffer = []
                else:
                    print("\n[exit] Interrupted. Use 'exit' to quit.")
            except EOFError:
                break

    # ──────────────────────────────────────────
    #  Cleanup
    # ──────────────────────────────────────────
    def close(self):
        if self.tee_file:
            try:
                self.tee_file.close()
            except Exception:
                pass
        try:
            if self.cursor: self.cursor.close()
            if self.conn:   self.conn.close()
        except Exception:
            pass
        self._save_bookmarks()
        print("[disconnected]")
        self.logger.info("Session ended. Total queries: %d", self.query_count)


# ──────────────────────────────────────────────
#  Entry point
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description=f"MySQL Terminal v{__version__} ({__status__}) — interactive MySQL client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"%(prog)s {__version__} ({__status__})")
    parser.add_argument("-H", "--host",     default="localhost",  help="MySQL host")
    parser.add_argument("-P", "--port",     default=3306, type=int, help="MySQL port")
    parser.add_argument("-u", "--user",     default="root",       help="MySQL user")
    parser.add_argument("-p", "--password", default=None,
                        help="MySQL password (omit to prompt securely)")
    parser.add_argument("-d", "--database", default=None,         help="Initial database")
    parser.add_argument("--format",         default="grid",
                        choices=TABLE_FORMATS,                    help="Output table format")
    parser.add_argument("--output",         default="table",
                        choices=OUTPUT_MODES,                     help="Output mode")
    parser.add_argument("--log",            default=None,         help="Log file path")
    parser.add_argument("--history",        default=None,
                        help="History file path (default: ~/.mysql_terminal_history)")
    parser.add_argument("--ssl-ca",         default=None,         help="SSL CA certificate")
    parser.add_argument("--ssl-cert",       default=None,         help="SSL client certificate")
    parser.add_argument("--ssl-key",        default=None,         help="SSL client key")
    parser.add_argument("--connect-timeout",default=10, type=int, help="Connection timeout (seconds)")
    parser.add_argument("--charset",        default="utf8mb4",    help="Character set")
    parser.add_argument("--safe-mode",      action="store_true",
                        help="Block DELETE/UPDATE without WHERE; confirm DROP/TRUNCATE")
    parser.add_argument("--read-only",      action="store_true",
                        help="Block all DML (INSERT/UPDATE/DELETE/DROP/ALTER…)")
    parser.add_argument("--no-timer",       action="store_true",  help="Disable query timer")
    parser.add_argument("--execute", "-e",  default=None,
                        help="Execute a single SQL statement and exit")

    args = parser.parse_args()
    password = args.password or getpass.getpass(f"Password for {args.user}@{args.host}: ")

    terminal = MySQLTerminal(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        database=args.database,
        table_format=args.format,
        log_file=args.log,
        history_file=args.history,
        output_mode=args.output,
        ssl_ca=args.ssl_ca,
        ssl_cert=args.ssl_cert,
        ssl_key=args.ssl_key,
        connect_timeout=args.connect_timeout,
        charset=args.charset,
        safe_mode=args.safe_mode,
        read_only=args.read_only,
    )
    if args.no_timer:
        terminal.timer_on = False

    terminal.connect()

    if args.execute:
        # Non-interactive single-query mode
        q = args.execute.strip()
        if not q.endswith(";"):
            q += ";"
        terminal.execute(q)
        terminal.close()
        return

    terminal.run()
    terminal.close()


if __name__ == "__main__":
    main()