"""
Core.py — TerSQL Execution Core v0.0.3

Changes in v0.0.3:
  - SafetyGate: covers DROP SCHEMA / DROP VIEW / DROP FUNCTION / DROP PROCEDURE
    (now fully aligned with NLP.SafetyChecker v0.0.3)
  - TerSQLCore.run(): removed dead 'groq' source branch; handles source=='none'
  - status(): displays HealthStatus.warnings and full PluginStats summary
  - _do_backup(): gracefully handles NotImplementedError (e.g. MongoDB)
  - Added table_info() convenience wrapper (surfaces v0.0.3 plugin enrichment)
  - Added primary_keys() convenience wrapper
  - Added plugins_info() via PluginRegistry.list_meta()
  - sync_schema(): FK guard is explicit — skips gracefully for non-SQL backends
  - QueryRecord.source: docstring updated to reflect v0.0.3 ParseResult sources
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from tabulate import tabulate

from NLP import NLPEngine, ParseResult
from plugins.base import BaseDB, QueryResult, PluginRegistry

logger = logging.getLogger("tersql.core")


# ─────────────────────────────────────────────────────────────
#  Auto-corrections
# ─────────────────────────────────────────────────────────────

SQL_FIXES = {
    r"^show\s+database;?$":           "SHOW DATABASES;",
    r"^show\s+table;?$":              "SHOW TABLES;",
    r"^show\s+dbs?;?$":               "SHOW DATABASES;",
    r"^desc\s+([^;\s]+);?$":          r"DESCRIBE \1;",
    r"^select\s+\*\s+([^;\s]+);?$":   r"SELECT * FROM \1;",
    r"^select\s+from\s+([^;\s]+);?$": r"SELECT * FROM \1;",
    r"^use\s+([^;\s]+);?$":           r"USE \1;",
}


# ─────────────────────────────────────────────────────────────
#  Session record
# ─────────────────────────────────────────────────────────────

@dataclass
class QueryRecord:
    n:        int
    ts:       str
    input:    str
    sql:      str
    intent:   str
    source:   str       # "rule" | "passthrough" | "none"
    elapsed:  float
    rows:     int
    ok:       bool
    error:    str = ""


# ─────────────────────────────────────────────────────────────
#  Output renderer
# ─────────────────────────────────────────────────────────────

class OutputRenderer:
    """Renders QueryResult in multiple formats."""

    MODES   = ("table", "json", "csv", "vertical")
    FORMATS = ("grid", "psql", "pipe", "plain", "simple", "github", "markdown", "html")

    def __init__(self, mode: str = "table", fmt: str = "grid"):
        self.mode = mode if mode in self.MODES else "table"
        self.fmt  = fmt  if fmt in self.FORMATS else "grid"

    def render(self, result: QueryResult) -> str:
        if not result.rows:
            return "(empty set)"

        if self.mode == "json":
            return json.dumps(result.as_dicts(), indent=2, default=str, ensure_ascii=False)

        if self.mode == "csv":
            buf = io.StringIO()
            w   = csv.writer(buf)
            w.writerow(result.columns)
            w.writerows(result.rows)
            return buf.getvalue().rstrip()

        if self.mode == "vertical":
            parts = []
            width = max((len(c) for c in result.columns), default=0)
            for i, row in enumerate(result.rows, 1):
                parts.append(f"{'*' * 27} {i}. row {'*' * 27}")
                for col, val in zip(result.columns, row):
                    parts.append(f"{col.rjust(width)}: {val}")
            return "\n".join(parts)

        # Default: tabulate
        return tabulate(result.rows, headers=result.columns, tablefmt=self.fmt)


# ─────────────────────────────────────────────────────────────
#  Safety gate
# ─────────────────────────────────────────────────────────────

class SafetyGate:
    """
    Pre-execution safety checks.
    Supports safe_mode (require WHERE for DML) and read_only mode.

    v0.0.3: _DANGEROUS now covers DROP SCHEMA / DROP VIEW / DROP FUNCTION /
            DROP PROCEDURE — aligned with NLP.SafetyChecker v0.0.3.
    """

    _DANGEROUS = re.compile(
        r"^\s*(DROP\s+TABLE|DROP\s+DATABASE|DROP\s+SCHEMA|DROP\s+VIEW"
        r"|DROP\s+FUNCTION|DROP\s+PROCEDURE|TRUNCATE|DELETE\s+FROM"
        r"|ALTER\s+TABLE|DROP\s+INDEX|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )
    _DML = re.compile(
        r"^\s*(INSERT|UPDATE|DELETE|REPLACE|TRUNCATE|DROP|CREATE|ALTER|RENAME|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )
    _NEEDS_WHERE = re.compile(r"^\s*(DELETE|UPDATE)\b", re.IGNORECASE)
    _HAS_WHERE   = re.compile(r"\bWHERE\b", re.IGNORECASE)

    def __init__(self, safe_mode: bool = False, read_only: bool = False):
        self.safe_mode = safe_mode
        self.read_only = read_only

    def check(self, sql: str) -> tuple[bool, bool, str]:
        """
        Returns (allowed, needs_backup, reason).
        allowed:      True → proceed
        needs_backup: True → take backup before executing
        reason:       human-readable explanation if blocked
        """
        if self.read_only and self._DML.match(sql):
            token = sql.strip().split()[0].upper()
            return False, False, f"Read-only mode: {token} blocked"

        if self.safe_mode:
            if self._NEEDS_WHERE.match(sql) and not self._HAS_WHERE.search(sql):
                return False, False, "Safe-mode: DELETE/UPDATE without WHERE is blocked"

        needs_backup = bool(self._DANGEROUS.match(sql))
        return True, needs_backup, ""


# ─────────────────────────────────────────────────────────────
#  Core engine
# ─────────────────────────────────────────────────────────────

class TerSQLCore:
    """
    Main orchestration engine.
    Ties together NLP, plugins, safety, backups, and rendering.
    """

    BACKUP_DIR = os.path.expanduser("~/.tersql/backups")

    def __init__(
        self,
        db:              BaseDB,
        nlp:             NLPEngine,
        output_mode:     str  = "table",
        table_format:    str  = "grid",
        safe_mode:       bool = False,
        read_only:       bool = False,
        auto_backup:     bool = True,
        history_size:    int  = 500,
        explain_mode:    bool = False,
        timer_on:        bool = True,
        confidence_warn: float = 0.65,
    ):
        self.db           = db
        self.nlp          = nlp
        self.renderer     = OutputRenderer(output_mode, table_format)
        self.safety       = SafetyGate(safe_mode, read_only)
        self.auto_backup  = auto_backup
        self.explain_mode = explain_mode
        self.timer_on     = timer_on
        self.conf_warn    = confidence_warn

        self._history:   deque[QueryRecord] = deque(maxlen=history_size)
        self._session_n: int   = 0
        self._last_result: Optional[QueryResult] = None
        self._schema_cache: dict = {}

        self._bookmarks: dict = {}
        self._bookmarks_file = os.path.expanduser("~/.tersql_bookmarks.json")
        self._load_bookmarks()

    # ── Bookmarks ─────────────────────────────────────────────

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
            self._print_warn(f"Could not save bookmarks: {e}")

    def add_bookmark(self, name: str, sql: str):
        self._bookmarks[name] = sql
        self._save_bookmarks()
        self._print_info(f"Bookmark '{name}' saved.")

    def list_bookmarks(self):
        if not self._bookmarks:
            print("  (no bookmarks saved)")
            return
        rows = [(k, v[:80]) for k, v in self._bookmarks.items()]
        print(tabulate(rows, headers=["Name", "SQL"], tablefmt="simple"))

    def run_bookmark(self, name: str):
        sql = self._bookmarks.get(name)
        if not sql:
            self._print_error(f"Bookmark '{name}' not found. Use .bookmarks to list.")
            return None
        self._print_info(f"Running bookmark: {sql}")
        return self.run(sql if sql.endswith(";") else sql + ";")

    def del_bookmark(self, name: str):
        if name in self._bookmarks:
            del self._bookmarks[name]
            self._save_bookmarks()
            self._print_info(f"Bookmark '{name}' deleted.")
        else:
            self._print_error(f"Bookmark '{name}' not found.")

    # ── Schema sync ───────────────────────────────────────────

    def sync_schema(self):
        """
        Fetch live schema and push to NLP engine.
        v0.0.3: FK enrichment is skipped gracefully for non-SQL backends
                (e.g. MongoDB) that don't implement get_foreign_keys().
        """
        try:
            schema = self.db.get_schema()
            for table in list(schema.keys()):
                try:
                    fks = self.db.get_foreign_keys(table)
                    for fk in fks:
                        ref_col = f"{fk['ref_table']}.{fk['ref_column']}"
                        if ref_col not in schema.get(fk["ref_table"], []):
                            schema.setdefault(fk["ref_table"], []).append(ref_col)
                except (AttributeError, NotImplementedError):
                    # Backend does not support FK introspection (e.g. MongoDB)
                    break
                except Exception:
                    pass
            self._schema_cache = schema
            self.nlp.update_schema(schema)
            logger.info("Schema synced: %d tables", len(schema))
        except Exception as e:
            logger.warning("Schema sync failed: %s", e)

    # ── Schema helpers (v0.0.3) ───────────────────────────────

    def table_info(self, table: str) -> dict:
        """
        Return enriched table metadata via plugin's table_info().
        Falls back to an empty dict if the plugin doesn't support it.
        """
        try:
            return self.db.table_info(table)
        except (AttributeError, NotImplementedError):
            return {}

    def primary_keys(self, table: str) -> list[str]:
        """Return primary key column names for a table."""
        try:
            return self.db.get_primary_keys(table)
        except (AttributeError, NotImplementedError):
            return []

    def plugins_info(self) -> list:
        """Return PluginMeta for every registered plugin (v0.0.3)."""
        return PluginRegistry.list_meta()

    # ── Main entry point ──────────────────────────────────────

    def run(self, user_input: str) -> Optional[QueryResult]:
        """
        Full pipeline:
          1. Auto-correct obvious typos
          2. NLP translate
          3. Safety check
          4. Optional backup
          5. Execute (with optional EXPLAIN prefix)
          6. Render
          7. Record history
        """
        raw   = user_input.strip()
        t_run = time.perf_counter()

        # ── Auto-corrections ─────────────────────────────────
        corrected = raw
        for pattern, fix in SQL_FIXES.items():
            if fix is None:
                continue
            m = re.match(pattern, raw, re.IGNORECASE)
            if m:
                corrected = re.sub(pattern, fix, raw, flags=re.IGNORECASE)
                if corrected.lower() != raw.lower():
                    self._print_info(f"Auto-fix → {corrected}")
                break

        # ── Translate ────────────────────────────────────────
        parse: ParseResult = self.nlp.translate(corrected)

        if not parse.sql:
            self._print_warn("Could not generate SQL from input.")
            for w in parse.warnings:
                self._print_warn(w)
            return None

        sql = parse.sql.strip()

        # Confidence warning
        if parse.source == "rule" and parse.confidence < self.conf_warn:
            self._print_warn(
                f"Low confidence ({parse.confidence:.0%}) — verify the generated SQL before proceeding."
            )

        # Print generated SQL (always, for transparency)
        self._print_sql(sql)
        for w in parse.warnings:
            self._print_warn(w)

        # ── Safety check ─────────────────────────────────────
        allowed, needs_backup, reason = self.safety.check(sql)
        if not allowed:
            self._print_error(f"Blocked: {reason}")
            self._record(raw, sql, parse, 0.0, 0, ok=False, error=reason)
            return None

        if parse.is_dangerous:
            confirm = input(f"\n  ⚠  This is a destructive operation. Type YES to continue: ").strip()
            if confirm != "YES":
                self._print_info("Cancelled.")
                return None

        # ── Auto-backup ───────────────────────────────────────
        if (needs_backup or parse.is_dangerous) and self.auto_backup:
            if not self._do_backup(sql):
                self._record(raw, sql, parse, 0.0, 0, ok=False, error="Aborted: backup failed")
                return None

        # ── EXPLAIN mode ─────────────────────────────────────
        if self.explain_mode and re.match(r"^\s*SELECT\b", sql, re.IGNORECASE):
            if self.db.meta.dialect not in ("mongodb",):
                explain_result = self.db.execute(f"EXPLAIN {sql}")
                self._print_info("EXPLAIN output:")
                print(self.renderer.render(explain_result))
                print()
            else:
                self._print_info("EXPLAIN not supported for this backend — skipped.")

        # ── Execute ───────────────────────────────────────────
        result = self.db.execute(sql)
        elapsed = (time.perf_counter() - t_run) * 1000

        # Handle USE <db> schema refresh
        if re.match(r"^\s*USE\b", sql, re.IGNORECASE):
            self.sync_schema()

        # ── Render ────────────────────────────────────────────
        if result.warnings:
            for w in result.warnings:
                self._print_warn(w)

        if result.rows:
            print(self.renderer.render(result))
            row_label = f"{result.row_count} row(s) returned"
            if result.truncated and result.total_count >= 0:
                row_label += f" (truncated from {result.total_count})"
        else:
            if sql.upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE")):
                row_label = f"Query OK  {result.affected_rows} row(s) affected"
            else:
                row_label = "Query OK"

        timer_str = f"  ({elapsed:.1f} ms)" if self.timer_on else ""
        print(f"\n  {row_label}{timer_str}")

        self._last_result = result
        self._record(raw, sql, parse, elapsed, result.row_count, ok=True)

        return result

    # ── Export ────────────────────────────────────────────────

    def export(self, path: str, fmt: str = "csv"):
        """Export last result to file."""
        if not self._last_result or not self._last_result.rows:
            self._print_warn("No result to export. Run a SELECT query first.")
            return
        try:
            if fmt == "json":
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(self._last_result.as_dicts(), f, indent=2, default=str)
            else:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(self._last_result.columns)
                    w.writerows(self._last_result.rows)
            n = self._last_result.row_count
            self._print_info(f"Exported {n} row(s) → {path} ({fmt.upper()})")
        except Exception as e:
            self._print_error(f"Export failed: {e}")

    # ── History ───────────────────────────────────────────────

    def history(self, n: int = 20) -> list[QueryRecord]:
        return list(self._history)[-n:]

    def print_history(self, n: int = 20):
        records = self.history(n)
        if not records:
            print("  (no history)")
            return
        rows = [
            (r.n, r.ts[11:19], r.intent, r.source,
             f"{r.elapsed:.0f}ms", "✓" if r.ok else "✗",
             r.input[:60])
            for r in records
        ]
        print(tabulate(rows,
                       headers=["#", "Time", "Intent", "Source", "Elapsed", "OK", "Input"],
                       tablefmt="simple"))

    # ── Explain ───────────────────────────────────────────────

    def explain(self, user_input: str):
        """Translate and EXPLAIN without executing."""
        parse = self.nlp.translate(user_input)
        if not parse.sql:
            self._print_warn("Could not generate SQL")
            return
        self._print_sql(parse.sql)
        if re.match(r"^\s*SELECT\b", parse.sql, re.IGNORECASE):
            if self.db.meta.dialect not in ("mongodb",):
                result = self.db.execute(f"EXPLAIN {parse.sql}")
                print(self.renderer.render(result))
            else:
                self._print_info("EXPLAIN not supported for MongoDB.")

    # ── Health / status ───────────────────────────────────────

    def status(self):
        """
        Print connection and session status.
        v0.0.3: shows HealthStatus.warnings and full PluginStats summary.
        """
        h = self.db.health()
        m = self.db.meta
        print(f"\n{'─'*55}")
        print(f"  Plugin          : {m.name} v{m.version}")
        print(f"  Description     : {m.description or '—'}")
        print(f"  Connected       : {'✓' if h.connected else '✗'}")
        print(f"  Server version  : {h.server_version or 'unknown'}")
        print(f"  Current DB      : {h.current_db or '(none)'}")
        print(f"  Latency         : {h.latency_ms:.1f} ms")
        print(f"  Mode            : output={self.renderer.mode}  fmt={self.renderer.fmt}")
        print(f"  Safe mode       : {'on' if self.safety.safe_mode else 'off'}")
        print(f"  Read-only       : {'on' if self.safety.read_only else 'off'}")
        print(f"  Auto-backup     : {'on' if self.auto_backup else 'off'}")
        print(f"  Explain mode    : {'on' if self.explain_mode else 'off'}")
        print(f"  Queries run     : {self._session_n}")

        # v0.0.3: Plugin stats
        if hasattr(self.db, "_stats"):
            st = self.db.stats.as_dict()
            print(f"  {'─'*47}")
            print(f"  PluginStats     :")
            for k, v in st.items():
                print(f"    {k:<20}: {v}")

        # v0.0.3: Extra fields from health (e.g. cache_size, pool_size)
        if h.extra:
            print(f"  {'─'*47}")
            for k, v in h.extra.items():
                print(f"  {k:<16}: {v}")

        # v0.0.3: Surface health warnings
        if h.warnings:
            print(f"  {'─'*47}")
            for w in h.warnings:
                print(f"  ⚠  {w}")

        print(f"{'─'*55}\n")

    # ── Internals ─────────────────────────────────────────────

    def _do_backup(self, triggering_sql: str) -> bool:
        """
        Trigger a backup before a destructive operation.
        v0.0.3: catches NotImplementedError for backends that don't support
                backup (e.g. MongoDB) and gives the user an option to continue.
        """
        try:
            self._print_info(f"Auto-backup triggered by: {triggering_sql[:60]}")
            path = self.db.backup(self.BACKUP_DIR)
            self._print_info(f"Backup saved → {path}")
            return True
        except NotImplementedError as e:
            self._print_warn(f"Backup not supported by this backend: {e}")
            confirm = input("  Continue without backup? (yes/no): ").strip().lower()
            return confirm == "yes"
        except Exception as e:
            self._print_warn(f"Backup failed: {e}")
            confirm = input("  Continue without backup? (yes/no): ").strip().lower()
            if confirm != "yes":
                self._print_info("Cancelled.")
                return False
            return True

    def _record(self, raw, sql, parse, elapsed, rows, ok, error=""):
        self._session_n += 1
        self._history.append(QueryRecord(
            n=self._session_n,
            ts=datetime.now().isoformat(timespec="seconds"),
            input=raw,
            sql=sql,
            intent=parse.intent,
            source=parse.source,
            elapsed=elapsed,
            rows=rows,
            ok=ok,
            error=error,
        ))

    @staticmethod
    def _print_sql(sql: str):
        print(f"\n  SQL → {sql}\n")

    @staticmethod
    def _print_info(msg: str):
        print(f"  ℹ  {msg}")

    @staticmethod
    def _print_warn(msg: str):
        print(f"  ⚠  {msg}")

    @staticmethod
    def _print_error(msg: str):
        print(f"  ✗  {msg}")