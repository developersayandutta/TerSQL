"""
plugins/mongodb.py — TerSQL MongoDB Plugin v0.0.3

Changes in v0.0.3:
  - Per-instance stats via BaseDB.__init__() super call
  - get_primary_keys() returns ["_id"] (MongoDB standard)
  - table_info() enriched with collection stats (count, size, avgObjSize)
  - execute(): stats wired via timed_query
  - PluginMeta updated with description/author
  - health(): exposes queries_run
  - New: list_databases() helper
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError as e:
    raise ImportError("pymongo is required: pip install pymongo") from e

from plugins.base import (
    BaseDB, QueryResult, HealthStatus, PluginMeta,
    PluginRegistry, timed_query,
)

logger = logging.getLogger("tersql.mongodb")


@PluginRegistry.register("mongodb")
class MongoDBPlugin(BaseDB):
    """
    MongoDB plugin for TerSQL.
    Expects NLP engine to generate JSON commands for db.runCommand().
    """

    @property
    def meta(self) -> PluginMeta:
        return PluginMeta(
            name="MongoDB",
            version="0.0.3",
            db_type="mongodb",
            dialect="mongodb",
            supports_transactions=True,
            supports_streaming=False,
            supports_backup=False,
            author="TerSQL",
            description="MongoDB plugin using runCommand JSON interface",
        )

    def __init__(self):
        super().__init__()   # initialise PluginStats
        self._client:     Optional[MongoClient] = None
        self._db         = None
        self._config:     dict = {}
        self._current_db: str  = ""

    # ── Connection lifecycle ──────────────────────────────────

    def connect(
        self,
        host:     str = "localhost",
        port:     int = 27017,
        user:     str = "",
        password: str = "",
        database: str = "",
        **kwargs
    ) -> None:
        self._config = {"host": host, "port": port}
        if user and password:
            self._config["username"] = user
            self._config["password"] = password

        self._current_db = database or "test"

        try:
            self._client = MongoClient(**self._config, serverSelectionTimeoutMS=5000)
            self._client.admin.command("ping")
            self._db = self._client[self._current_db]
            ver = self._client.server_info().get("version", "unknown")
            logger.info("Connected to MongoDB %s at %s:%s (db=%s)", ver, host, port, self._current_db)

        except PyMongoError as e:
            logger.error("MongoDB connect failed: %s", e)
            raise ConnectionError(f"MongoDB connection failed: {e}") from e

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
        self._client = None
        self._db     = None
        logger.info("MongoDB disconnected")

    def reconnect(self) -> None:
        self.disconnect()
        time.sleep(0.5)
        self.connect(**self._config, database=self._current_db)

    def _ensure_connected(self):
        try:
            self._client.admin.command("ping")
        except Exception:
            logger.warning("Lost connection, reconnecting...")
            self.reconnect()

    # ── Query execution ───────────────────────────────────────

    @timed_query
    def execute(self, query: str, params: Optional[tuple] = None) -> QueryResult:
        """
        Execute MongoDB command given as a JSON string.
        Also handles the pseudo-command: USE <dbname>
        """
        self._ensure_connected()

        if query.strip().upper().startswith("USE "):
            parts = query.strip().split()
            if len(parts) >= 2:
                self._current_db = parts[1].strip("`'\";")
                self._db = self._client[self._current_db]
                return QueryResult(rows=[], columns=[], affected_rows=0)

        try:
            cmd = json.loads(query)
            res = self._db.command(cmd)

            if "cursor" in res and "firstBatch" in res["cursor"]:
                docs = res["cursor"]["firstBatch"]
            elif "n" in res:
                return QueryResult(rows=[], columns=[], affected_rows=res.get("n", 0))
            else:
                docs = [res]

            if not docs:
                return QueryResult(rows=[], columns=[])

            columns = []
            for doc in docs:
                for k in doc.keys():
                    if k not in columns:
                        columns.append(k)

            rows = []
            for doc in docs:
                row = []
                for col in columns:
                    val = doc.get(col)
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val, default=str)
                    row.append(val)
                rows.append(tuple(row))

            return QueryResult(rows=rows, columns=columns, affected_rows=len(rows))

        except json.JSONDecodeError:
            err = "MongoDB plugin expects query to be a valid JSON command document."
            return QueryResult(rows=[], columns=[], warnings=[err])
        except PyMongoError as e:
            logger.error("MongoDB error: %s | query=%s", e, query[:200])
            return QueryResult(rows=[], columns=[], warnings=[str(e)])

    # ── Schema introspection ──────────────────────────────────

    def get_tables(self) -> list[str]:
        """Returns collection names (analogous to tables)."""
        if not self._db:
            return []
        return sorted(self._db.list_collection_names())

    def get_schema(self) -> dict[str, list[str]]:
        """Sample first document of each collection to infer field names."""
        schema = {}
        for coll_name in self.get_tables():
            coll = self._db[coll_name]
            doc  = coll.find_one()
            schema[coll_name] = list(doc.keys()) if doc else []
        return schema

    def get_primary_keys(self, table: str) -> list[str]:
        """NEW in v0.0.3: MongoDB always uses _id as the primary key."""
        return ["_id"]

    def table_info(self, table: str) -> dict:
        """v0.0.3: Enriched with collection stats from collStats command."""
        base = super().table_info(table)
        try:
            stats = self._db.command("collStats", table)
            base["count"]       = stats.get("count", 0)
            base["size_bytes"]  = stats.get("size", 0)
            base["avg_obj_size"] = stats.get("avgObjSize", 0)
            base["storage_size"] = stats.get("storageSize", 0)
            base["num_indexes"]  = stats.get("nindexes", 0)
        except Exception:
            pass
        return base

    def list_databases(self) -> list[str]:
        """NEW in v0.0.3: List all databases on the server."""
        if not self._client:
            return []
        try:
            return sorted(self._client.list_database_names())
        except PyMongoError as e:
            logger.warning("list_databases failed: %s", e)
            return []

    # ── Health ────────────────────────────────────────────────

    def health(self) -> HealthStatus:
        if not self._client:
            return HealthStatus(connected=False, latency_ms=-1)
        try:
            t0 = time.perf_counter()
            self._client.admin.command("ping")
            latency = (time.perf_counter() - t0) * 1000
            ver = self._client.server_info().get("version", "unknown")
            return HealthStatus(
                connected=True,
                latency_ms=round(latency, 2),
                server_version=ver,
                current_db=self._current_db,
                extra={"queries_run": self._stats.total_queries},
            )
        except Exception as e:
            return HealthStatus(connected=False, latency_ms=-1, extra={"error": str(e)})

    # ── Backup ────────────────────────────────────────────────

    def backup(self, output_path: str) -> str:
        raise NotImplementedError(
            "MongoDB backup via mongodump is not yet implemented. "
            "Use mongodump CLI directly: mongodump --db <db> --out <path>"
        )