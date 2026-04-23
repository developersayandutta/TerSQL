"""
plugins/mysql.py — TerSQL MySQL Plugin v0.0.3

Changes in v0.0.3:
  - Per-instance cache (via QueryCacheMixin._init_cache fix)
  - get_primary_keys() implemented
  - table_info() returns richer metadata including engine & charset
  - execute() passes stats via timed_query decorator
  - PluginMeta updated with description/author
  - backup(): redacted password from mysqldump command in logs
  - Minor: pool_name collision guard (suffixed with pid)
"""

from __future__ import annotations

import os
import re
import shutil
import logging
import subprocess
import time
from typing import Optional, Iterator

try:
    import mysql.connector
    from mysql.connector import Error, pooling
    from mysql.connector.cursor import MySQLCursorBuffered
except ImportError as e:
    raise ImportError("mysql-connector-python is required: pip install mysql-connector-python") from e

from plugins.base import (
    BaseDB, QueryResult, HealthStatus, PluginMeta,
    PluginRegistry, timed_query, QueryCacheMixin,
)

logger = logging.getLogger("tersql.mysql")


@PluginRegistry.register("mysql")
class MySQLPlugin(QueryCacheMixin, BaseDB):
    """
    MySQL / MariaDB plugin for TerSQL.
    Supports connection pooling, streaming, FK introspection, primary key lookup.
    """

    @property
    def meta(self) -> PluginMeta:
        return PluginMeta(
            name="MySQL",
            version="0.0.3",
            db_type="mysql",
            dialect="mysql",
            supports_transactions=True,
            supports_streaming=True,
            supports_backup=bool(shutil.which("mysqldump")),
            author="TerSQL",
            description="MySQL / MariaDB plugin with pooling, streaming and FK introspection",
        )

    def __init__(self):
        super().__init__()         # initialise PluginStats
        self._init_cache()         # per-instance cache (v0.0.3 fix)
        self._pool:       Optional[pooling.MySQLConnectionPool] = None
        self._conn:       Optional[mysql.connector.MySQLConnection] = None
        self._cursor:     Optional[MySQLCursorBuffered] = None
        self._config:     dict = {}
        self._current_db: str  = ""
        self._in_tx:      bool = False

    # ── Connection lifecycle ──────────────────────────────────

    def connect(
        self,
        host:      str  = "localhost",
        port:      int  = 3306,
        user:      str  = "root",
        password:  str  = "",
        database:  str  = "",
        ssl_ca:    Optional[str] = None,
        ssl_cert:  Optional[str] = None,
        ssl_key:   Optional[str] = None,
        timeout:   int  = 10,
        charset:   str  = "utf8mb4",
        pool_size: int  = 3,
        use_pool:  bool = True,
    ) -> None:
        self._config = {
            "host":               host,
            "port":               port,
            "user":               user,
            "password":           password,
            "connection_timeout": timeout,
            "charset":            charset,
            "autocommit":         True,
            "raise_on_warnings":  False,
        }
        if database:
            self._config["database"] = database
            self._current_db         = database
        if ssl_ca:
            self._config["ssl_ca"]   = ssl_ca
        if ssl_cert:
            self._config["ssl_cert"] = ssl_cert
        if ssl_key:
            self._config["ssl_key"]  = ssl_key

        try:
            if use_pool and pool_size > 1:
                # Unique pool name per process to avoid collision on reconnect
                pool_name = f"tersql_pool_{os.getpid()}"
                self._pool = pooling.MySQLConnectionPool(
                    pool_name=pool_name,
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **self._config,
                )
                self._conn = self._pool.get_connection()
            else:
                self._conn = mysql.connector.connect(**self._config)

            self._cursor = self._conn.cursor(buffered=True, dictionary=False)
            ver = self._conn.get_server_info()
            logger.info("Connected to MySQL %s at %s:%s (db=%s)", ver, host, port, database or "none")

        except Error as e:
            logger.error("MySQL connect failed: %s", e)
            raise ConnectionError(f"MySQL connection failed: {e}") from e

    def disconnect(self) -> None:
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._conn   = None
        self._cursor = None
        logger.info("MySQL disconnected")

    def reconnect(self) -> None:
        self.disconnect()
        time.sleep(0.5)
        safe_keys = {"host", "port", "user", "password", "database", "charset",
                     "ssl_ca", "ssl_cert", "ssl_key"}
        cfg = {k: v for k, v in self._config.items() if k in safe_keys}
        self.connect(**cfg)
        if self._current_db:
            self._safe_exec(f"USE `{self._current_db}`")

    def _ensure_connected(self):
        try:
            self._conn.ping(reconnect=True, attempts=3, delay=1)
            if not self._cursor or not self._cursor.connection:
                self._cursor = self._conn.cursor(buffered=True, dictionary=False)
        except Exception:
            logger.warning("Lost connection, reconnecting...")
            self.reconnect()

    # ── Query execution ───────────────────────────────────────

    @timed_query
    def execute(self, query: str, params: Optional[tuple] = None) -> QueryResult:
        self._ensure_connected()

        # Track USE <db> changes
        m = re.match(r"USE\s+[`\"]?(\w+)[`\"]?", query.strip(), re.IGNORECASE)
        if m:
            self._current_db = m.group(1)
            self._cache_invalidate()

        try:
            self._cursor.execute(query, params or ())

            if self._cursor.with_rows:
                rows     = self._cursor.fetchall()
                columns  = [desc[0] for desc in self._cursor.description]
                warnings = self._fetch_warnings()
                return QueryResult(
                    rows=list(rows),
                    columns=columns,
                    affected_rows=len(rows),
                    warnings=warnings,
                )
            else:
                if not self._in_tx:
                    self._conn.commit()
                warnings = self._fetch_warnings()
                return QueryResult(
                    rows=[],
                    columns=[],
                    affected_rows=self._cursor.rowcount,
                    warnings=warnings,
                )

        except Error as e:
            logger.error("MySQL query error: %s | query=%s", e, query[:200])
            try:
                self._cursor = self._conn.cursor(buffered=True)
            except Exception:
                pass
            return QueryResult(
                rows=[], columns=[],
                affected_rows=0,
                warnings=[str(e)],
            )

    def stream(self, query: str, params: Optional[tuple] = None,
               chunk_size: int = 500) -> Iterator[list[dict]]:
        """True streaming using an unbuffered server-side cursor."""
        self._ensure_connected()
        stream_cursor = None
        try:
            stream_cursor = self._conn.cursor(buffered=False, dictionary=True)
            stream_cursor.execute(query, params or ())
            columns = [desc[0] for desc in stream_cursor.description] if stream_cursor.description else []
            chunk   = []
            for row in stream_cursor:
                chunk.append(dict(zip(columns, row)) if not isinstance(row, dict) else row)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
        except Error as e:
            logger.error("Stream error: %s", e)
            raise
        finally:
            if stream_cursor:
                stream_cursor.close()

    def _safe_exec(self, query: str) -> None:
        try:
            self._cursor.execute(query)
        except Exception:
            pass

    def _fetch_warnings(self) -> list[str]:
        wcount = getattr(self._cursor, "warning_count", 0) or 0
        if not wcount:
            return []
        try:
            c = self._conn.cursor(buffered=True)
            c.execute("SHOW WARNINGS")
            rows = c.fetchall()
            c.close()
            return [f"[{r[0]}] {r[2]}" for r in rows]
        except Exception:
            return []

    # ── Transactions ──────────────────────────────────────────

    def begin(self) -> None:
        self._ensure_connected()
        self._conn.autocommit = False
        self._in_tx = True
        logger.debug("Transaction started")

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()
            self._conn.autocommit = True
        self._in_tx = False
        logger.debug("Transaction committed")

    def rollback(self) -> None:
        if self._conn:
            self._conn.rollback()
            self._conn.autocommit = True
        self._in_tx = False
        logger.debug("Transaction rolled back")

    # ── Schema introspection ──────────────────────────────────

    def get_tables(self) -> list[str]:
        result = self.execute("SHOW TABLES")
        return [row[0] for row in result.rows]

    def get_schema(self) -> dict[str, list[str]]:
        schema = {}
        for table in self.get_tables():
            result = self.execute(f"DESCRIBE `{table}`")
            schema[table] = [row[0] for row in result.rows]
        return schema

    def get_column_types(self, table: str) -> dict[str, str]:
        result = self.execute(f"DESCRIBE `{table}`")
        # DESCRIBE: Field, Type, Null, Key, Default, Extra
        return {row[0]: row[1] for row in result.rows}

    def get_primary_keys(self, table: str) -> list[str]:
        """NEW in v0.0.3: Return PK column names from DESCRIBE output."""
        result = self.execute(f"DESCRIBE `{table}`")
        # DESCRIBE columns: Field, Type, Null, Key, Default, Extra
        return [row[0] for row in result.rows if row[3] == "PRI"]

    def get_foreign_keys(self, table: str) -> list[dict]:
        if not self._current_db:
            return []
        q = """
            SELECT
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE kcu
            JOIN information_schema.TABLE_CONSTRAINTS tc
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA    = kcu.TABLE_SCHEMA
             AND tc.TABLE_NAME      = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND kcu.TABLE_SCHEMA   = %s
              AND kcu.TABLE_NAME     = %s
        """
        result = self.execute(q, (self._current_db, table))
        return [
            {"column": r[0], "ref_table": r[1], "ref_column": r[2]}
            for r in result.rows
        ]

    def get_indexes(self, table: str) -> list[dict]:
        result = self.execute(f"SHOW INDEXES FROM `{table}`")
        cols   = result.columns
        return [dict(zip(cols, row)) for row in result.rows]

    def table_info(self, table: str) -> dict:
        """v0.0.3: Enriched table_info with engine and charset from information_schema."""
        base = super().table_info(table)
        if self._current_db:
            q = """
                SELECT ENGINE, TABLE_COLLATION, TABLE_ROWS, DATA_LENGTH
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """
            r = self.execute(q, (self._current_db, table))
            if r.rows:
                row = r.rows[0]
                base["engine"]    = row[0]
                base["collation"] = row[1]
                base["est_rows"]  = row[2]
                base["data_size"] = row[3]
        return base

    # ── Health ────────────────────────────────────────────────

    def health(self) -> HealthStatus:
        if not self._conn:
            return HealthStatus(connected=False, latency_ms=-1)
        try:
            t0 = time.perf_counter()
            self._conn.ping(reconnect=False)
            latency = (time.perf_counter() - t0) * 1000
            ver = self._conn.get_server_info()
            return HealthStatus(
                connected=True,
                latency_ms=round(latency, 2),
                server_version=ver,
                current_db=self._current_db,
                extra={
                    "pool_size":   self._pool.pool_size if self._pool else 1,
                    "cache_size":  self.cache_size(),        # NEW
                    "queries_run": self._stats.total_queries, # NEW
                },
            )
        except Exception as e:
            return HealthStatus(connected=False, latency_ms=-1, extra={"error": str(e)})

    # ── Backup ────────────────────────────────────────────────

    def backup(self, output_path: str) -> str:
        if not shutil.which("mysqldump"):
            raise RuntimeError("mysqldump not found in PATH")
        if not self._current_db:
            raise ValueError("No database selected for backup")

        cfg     = self._config
        ts      = time.strftime("%Y%m%d_%H%M%S")
        db_name = self._current_db
        fname   = os.path.join(output_path, f"{db_name}_backup_{ts}.sql")
        os.makedirs(output_path, exist_ok=True)

        # v0.0.3: pass password via env var to avoid shell history / log exposure
        env = os.environ.copy()
        env["MYSQL_PWD"] = cfg.get("password", "")

        cmd = [
            "mysqldump",
            f"--host={cfg['host']}",
            f"--port={cfg['port']}",
            f"--user={cfg['user']}",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--events",
            "--set-gtid-purged=OFF",
            db_name,
        ]

        try:
            with open(fname, "w", encoding="utf-8") as out_f:
                proc = subprocess.run(
                    cmd,
                    stdout=out_f,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=300,
                    env=env,
                )
            if proc.returncode != 0:
                err = proc.stderr.strip()
                if "warning" in err.lower() and os.path.getsize(fname) > 0:
                    logger.warning("mysqldump warning: %s", err)
                else:
                    raise RuntimeError(f"mysqldump failed: {err}")

            size_kb = os.path.getsize(fname) // 1024
            logger.info("Backup created: %s (%d KB)", fname, size_kb)
            return fname

        except subprocess.TimeoutExpired:
            raise RuntimeError("mysqldump timed out after 300s")

    def restore(self, backup_path: str) -> bool:
        if not shutil.which("mysql"):
            raise RuntimeError("mysql CLI not found in PATH")
        if not os.path.isfile(backup_path):
            raise FileNotFoundError(backup_path)

        cfg = self._config
        env = os.environ.copy()
        env["MYSQL_PWD"] = cfg.get("password", "")

        cmd = [
            "mysql",
            f"--host={cfg['host']}",
            f"--port={cfg['port']}",
            f"--user={cfg['user']}",
            self._current_db,
        ]

        with open(backup_path, "r", encoding="utf-8") as f:
            proc = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE,
                                  text=True, timeout=600, env=env)

        if proc.returncode != 0:
            raise RuntimeError(f"Restore failed: {proc.stderr.strip()}")

        logger.info("Restored %s → %s", backup_path, self._current_db)
        return True

    # ── Identifier quoting ────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"