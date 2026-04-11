"""
plugins/postgre.py — TerSQL PostgreSQL Plugin
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
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import connection as PgConnection
    from psycopg2.extensions import cursor as PgCursor
    from psycopg2.pool import ThreadedConnectionPool
except ImportError as e:
    raise ImportError("psycopg2-binary is required: pip install psycopg2-binary") from e

from plugins.base import (
    BaseDB, QueryResult, HealthStatus, PluginMeta,
    PluginRegistry, timed_query, QueryCacheMixin,
)

logger = logging.getLogger("tersql.postgre")


@PluginRegistry.register("postgresql")
class PostgreSQLPlugin(QueryCacheMixin, BaseDB):
    """
    PostgreSQL plugin for TerSQL.
    """

    @property
    def meta(self) -> PluginMeta:
        return PluginMeta(
            name="PostgreSQL",
            version="2.0.0",
            db_type="postgresql",
            dialect="postgresql",
            supports_transactions=True,
            supports_streaming=True,
            supports_backup=bool(shutil.which("pg_dump")),
        )

    def __init__(self):
        self._pool:       Optional[ThreadedConnectionPool] = None
        self._conn:       Optional[PgConnection] = None
        self._cursor:     Optional[PgCursor] = None
        self._config:     dict = {}
        self._current_db: str  = ""
        self._in_tx:      bool = False

    # ── Connection lifecycle ──────────────────────────────────

    def connect(
        self,
        host:     str  = "localhost",
        port:     int  = 5432,
        user:     str  = "postgres",
        password: str  = "",
        database: str  = "",
        ssl_ca:   Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key:  Optional[str] = None,
        timeout:  int  = 10,
        charset:  str  = "utf8",
        pool_size: int = 3,
        use_pool:  bool = True,
    ) -> None:
        self._config = {
            "host":               host,
            "port":               port,
            "user":               user,
            "password":           password,
            "database":           database or "postgres",
            "connect_timeout":    timeout,
            "client_encoding":    charset,
        }
        self._current_db = self._config["database"]
        
        try:
            if use_pool and pool_size > 1:
                self._pool = ThreadedConnectionPool(
                    1, pool_size,
                    **self._config
                )
                self._conn = self._pool.getconn()
            else:
                self._conn = psycopg2.connect(**self._config)

            self._conn.autocommit = True
            self._cursor = self._conn.cursor()
            ver = self._conn.info.server_version
            logger.info("Connected to PostgreSQL %s at %s:%s (db=%s)", ver, host, port, self._current_db)

        except psycopg2.Error as e:
            logger.error("PostgreSQL connect failed: %s", e)
            raise ConnectionError(f"PostgreSQL connection failed: {e}") from e

    def disconnect(self) -> None:
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn:
                if self._pool:
                    self._pool.putconn(self._conn)
                else:
                    self._conn.close()
        except Exception:
            pass
        self._conn   = None
        self._cursor = None
        logger.info("PostgreSQL disconnected")

    def reconnect(self) -> None:
        self.disconnect()
        time.sleep(0.5)
        self.connect(**{k: v for k, v in self._config.items() if k not in ("connect_timeout", "client_encoding")})

    def _ensure_connected(self):
        try:
            with self._conn.cursor() as c:
                c.execute("SELECT 1")
        except Exception:
            logger.warning("Lost connection, reconnecting...")
            self.reconnect()

    # ── Query execution ───────────────────────────────────────

    @timed_query
    def execute(self, query: str, params: Optional[tuple] = None) -> QueryResult:
        self._ensure_connected()

        try:
            self._cursor.execute(query, params or ())
            
            affected = self._cursor.rowcount
            if self._cursor.description:
                rows = self._cursor.fetchall()
                columns = [desc[0] for desc in self._cursor.description]
                return QueryResult(
                    rows=rows,
                    columns=columns,
                    affected_rows=affected,
                    warnings=self._fetch_notices(),
                )
            else:
                if not self._in_tx:
                    self._conn.commit()
                return QueryResult(
                    rows=[],
                    columns=[],
                    affected_rows=affected,
                    warnings=self._fetch_notices(),
                )

        except psycopg2.Error as e:
            logger.error("PostgreSQL query error: %s | query=%s", e, query[:200])
            self._conn.rollback() # Important in PG to recover from failed transaction block
            return QueryResult(
                rows=[], columns=[],
                affected_rows=0,
                warnings=[str(e)],
            )

    def _fetch_notices(self) -> list[str]:
        if self._conn and self._conn.notices:
            notices = [n.strip() for n in self._conn.notices]
            del self._conn.notices[:]
            return notices
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
        q = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """
        result = self.execute(q)
        return [row[0] for row in result.rows]

    def get_schema(self) -> dict[str, list[str]]:
        schema = {}
        q = """
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public';
        """
        result = self.execute(q)
        for t, c in result.rows:
            schema.setdefault(t, []).append(c)
        return schema

    def get_column_types(self, table: str) -> dict[str, str]:
        q = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """
        result = self.execute(q, (table,))
        return {row[0]: row[1] for row in result.rows}

    # ── Health ────────────────────────────────────────────────

    def health(self) -> HealthStatus:
        if not self._conn or self._conn.closed:
            return HealthStatus(connected=False, latency_ms=-1)
        try:
            t0 = time.perf_counter()
            with self._conn.cursor() as c:
                c.execute("SELECT 1")
            latency = (time.perf_counter() - t0) * 1000
            # server_version returns int like 140001 for 14.1
            ver = str(self._conn.info.server_version)
            return HealthStatus(
                connected=True,
                latency_ms=round(latency, 2),
                server_version=ver,
                current_db=self._current_db,
                extra={},
            )
        except Exception as e:
            return HealthStatus(connected=False, latency_ms=-1, extra={"error": str(e)})

    # ── Backup ────────────────────────────────────────────────

    def backup(self, output_path: str) -> str:
        if not shutil.which("pg_dump"):
            raise RuntimeError("pg_dump not found in PATH")

        cfg = self._config
        ts  = time.strftime("%Y%m%d_%H%M%S")
        fname = os.path.join(output_path, f"{self._current_db}_backup_{ts}.sql")
        os.makedirs(output_path, exist_ok=True)

        cmd = [
            "pg_dump",
            "-h", str(cfg["host"]),
            "-p", str(cfg["port"]),
            "-U", str(cfg["user"]),
            "-F", "p",  # plain-text SQL script
            "-f", fname,
            self._current_db
        ]

        env = os.environ.copy()
        if cfg["password"]:
            env["PGPASSWORD"] = cfg["password"]

        try:
            proc = subprocess.run(cmd, env=env, stderr=subprocess.PIPE, text=True, timeout=300)
            if proc.returncode != 0:
                raise RuntimeError(f"pg_dump failed: {proc.stderr.strip()}")
            return fname
        except subprocess.TimeoutExpired:
            raise RuntimeError("pg_dump timed out after 300s")

    def restore(self, backup_path: str) -> bool:
        if not shutil.which("psql"):
            raise RuntimeError("psql CLI not found in PATH")
        if not os.path.isfile(backup_path):
            raise FileNotFoundError(backup_path)

        cfg = self._config
        cmd = [
            "psql",
            "-h", str(cfg["host"]),
            "-p", str(cfg["port"]),
            "-U", str(cfg["user"]),
            "-d", self._current_db,
            "-f", backup_path
        ]
        
        env = os.environ.copy()
        if cfg["password"]:
            env["PGPASSWORD"] = cfg["password"]

        proc = subprocess.run(cmd, env=env, stderr=subprocess.PIPE, text=True, timeout=600)
        if proc.returncode != 0:
            raise RuntimeError(f"Restore failed: {proc.stderr.strip()}")
        return True

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'
