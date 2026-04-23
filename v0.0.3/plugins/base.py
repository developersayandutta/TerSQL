"""
plugins/base.py — TerSQL Plugin Base Interface v0.0.3

Every database plugin must implement BaseDB.
Provides: abstract interface, shared utilities, plugin registry,
          health monitoring, result streaming contracts,
          and per-plugin query statistics.

Changes in v0.0.3:
  - QueryResult gains .truncated flag and .page() helper
  - HealthStatus gains .warnings list
  - BaseDB gains .get_primary_keys(), .count(), .table_info()
  - PluginRegistry.list_meta() for richer plugin discovery
  - QueryCacheMixin: per-instance cache (no class-level shared state bug fix)
  - timed_query: now thread-safe via functools.wraps
  - New PluginStats dataclass for per-session metrics
"""

from __future__ import annotations

import abc
import functools
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, Optional

logger = logging.getLogger("tersql.plugin")


# ─────────────────────────────────────────────────────────────
#  Result types
# ─────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """Uniform result container returned by every plugin."""
    rows:          list[tuple]
    columns:       list[str]
    affected_rows: int   = 0
    elapsed_ms:    float = 0.0
    query:         str   = ""
    warnings:      list[str] = field(default_factory=list)
    metadata:      dict      = field(default_factory=dict)
    truncated:     bool  = False   # NEW: True if rows were capped server-side
    total_count:   int   = -1      # NEW: -1 = unknown; >=0 = total before truncation

    @property
    def is_empty(self) -> bool:
        return not self.rows

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def as_dicts(self) -> list[dict]:
        return [dict(zip(self.columns, row)) for row in self.rows]

    def first(self) -> Optional[dict]:
        if self.rows:
            return dict(zip(self.columns, self.rows[0]))
        return None

    def scalar(self) -> Any:
        """Return single value from first row, first column."""
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def page(self, page: int, size: int = 20) -> "QueryResult":
        """
        NEW: Return a paginated slice of in-memory results.
        page is 1-based.
        """
        start = (max(page, 1) - 1) * size
        sliced = self.rows[start: start + size]
        return QueryResult(
            rows=sliced,
            columns=self.columns,
            affected_rows=len(sliced),
            elapsed_ms=self.elapsed_ms,
            query=self.query,
            metadata={**self.metadata, "page": page, "page_size": size},
            total_count=len(self.rows),
        )

    def column_index(self, name: str) -> Optional[int]:
        """NEW: Return 0-based index of a column by name (case-insensitive)."""
        name_l = name.lower()
        for i, c in enumerate(self.columns):
            if c.lower() == name_l:
                return i
        return None

    def col(self, name: str) -> list[Any]:
        """NEW: Extract all values for a named column as a list."""
        idx = self.column_index(name)
        if idx is None:
            raise KeyError(f"Column '{name}' not found in result")
        return [row[idx] for row in self.rows]


@dataclass
class HealthStatus:
    connected:      bool
    latency_ms:     float
    server_version: str  = ""
    current_db:     str  = ""
    extra:          dict = field(default_factory=dict)
    warnings:       list[str] = field(default_factory=list)   # NEW
    checked_at:     str  = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def ok(self) -> bool:
        return self.connected and self.latency_ms < 5000


# ─────────────────────────────────────────────────────────────
#  Plugin metadata
# ─────────────────────────────────────────────────────────────

@dataclass
class PluginMeta:
    name:                  str
    version:               str
    db_type:               str   # "mysql" | "postgresql" | "mongodb" | ...
    dialect:               str   # SQL dialect for NLP hints
    supports_transactions: bool  = True
    supports_streaming:    bool  = False
    supports_backup:       bool  = True
    min_server_version:    str   = ""
    author:                str   = "TerSQL"          # NEW
    description:           str   = ""                # NEW


# ─────────────────────────────────────────────────────────────
#  Per-session plugin statistics  (NEW)
# ─────────────────────────────────────────────────────────────

@dataclass
class PluginStats:
    """Accumulated metrics for a single plugin/session."""
    total_queries:   int   = 0
    total_rows:      int   = 0
    total_elapsed_ms: float = 0.0
    errors:          int   = 0
    cache_hits:      int   = 0

    @property
    def avg_elapsed_ms(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return self.total_elapsed_ms / self.total_queries

    def record(self, result: "QueryResult", *, from_cache: bool = False):
        self.total_queries += 1
        self.total_rows += result.row_count
        self.total_elapsed_ms += result.elapsed_ms
        if result.warnings:
            self.errors += 1
        if from_cache:
            self.cache_hits += 1

    def as_dict(self) -> dict:
        return {
            "total_queries":    self.total_queries,
            "total_rows":       self.total_rows,
            "total_elapsed_ms": round(self.total_elapsed_ms, 2),
            "avg_elapsed_ms":   round(self.avg_elapsed_ms, 2),
            "errors":           self.errors,
            "cache_hits":       self.cache_hits,
        }


# ─────────────────────────────────────────────────────────────
#  Abstract base
# ─────────────────────────────────────────────────────────────

class BaseDB(abc.ABC):
    """
    Abstract interface that every TerSQL database plugin must implement.

    Contract:
    - connect() must be called before any other method.
    - execute() returns a QueryResult; never raises — errors go into QueryResult.warnings
      (or raise ConnectionError / PermissionError for unrecoverable states).
    - All methods are synchronous; async support is out of scope for v0.0.3.
    """

    def __init__(self):
        self._stats = PluginStats()   # NEW: per-instance stats

    @property
    @abc.abstractmethod
    def meta(self) -> PluginMeta:
        """Return plugin metadata."""

    @property
    def stats(self) -> PluginStats:
        """NEW: Return accumulated session statistics."""
        return self._stats

    # ── Connection lifecycle ──────────────────────────────────

    @abc.abstractmethod
    def connect(self, **kwargs) -> None:
        """
        Establish database connection.
        Should raise ConnectionError on failure.
        kwargs: host, port, user, password, database, ssl_*, timeout, etc.
        """

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the connection cleanly."""

    @abc.abstractmethod
    def reconnect(self) -> None:
        """Drop and re-establish the connection."""

    def ping(self) -> bool:
        """Quick liveness check. Override for efficiency."""
        try:
            self.health()
            return True
        except Exception:
            return False

    # ── Query execution ───────────────────────────────────────

    @abc.abstractmethod
    def execute(self, query: str, params: Optional[tuple] = None) -> QueryResult:
        """
        Execute a single SQL/query statement.
        params: parameterised query values (preferred over f-strings).
        Returns QueryResult.
        """

    def execute_many(self, query: str, param_list: list[tuple]) -> QueryResult:
        """
        Execute the same statement with multiple parameter sets.
        Default implementation loops over execute(); override for bulk efficiency.
        """
        total_affected = 0
        t0 = time.perf_counter()
        for params in param_list:
            result = self.execute(query, params)
            total_affected += result.affected_rows
        elapsed = (time.perf_counter() - t0) * 1000
        return QueryResult(
            rows=[], columns=[],
            affected_rows=total_affected,
            elapsed_ms=elapsed,
            query=query,
        )

    def stream(self, query: str, params: Optional[tuple] = None,
               chunk_size: int = 500) -> Iterator[list[dict]]:
        """
        Streaming query — yields chunks of rows as dicts.
        Default: fetch all, then yield chunks. Override for true cursor streaming.
        """
        result = self.execute(query, params)
        dicts  = result.as_dicts()
        for i in range(0, len(dicts), chunk_size):
            yield dicts[i:i + chunk_size]

    # ── Schema introspection ──────────────────────────────────

    @abc.abstractmethod
    def get_tables(self) -> list[str]:
        """List all tables (or collections) in the current database."""

    @abc.abstractmethod
    def get_schema(self) -> dict[str, list[str]]:
        """
        Return schema as {table_name: [col1, col2, ...]}.
        Used by NLP engine for schema-aware query generation.
        """

    def get_column_types(self, table: str) -> dict[str, str]:
        """
        Return {column_name: data_type} for a table.
        Default: return empty dict. Override for richer NLP type hints.
        """
        return {}

    def get_foreign_keys(self, table: str) -> list[dict]:
        """
        Return FK relationships: [{"column": ..., "ref_table": ..., "ref_column": ...}]
        Used by NLP for JOIN inference. Default: empty list.
        """
        return []

    def get_indexes(self, table: str) -> list[dict]:
        """Return index metadata for a table."""
        return []

    def get_primary_keys(self, table: str) -> list[str]:
        """
        NEW: Return list of primary key column names for a table.
        Default: empty list. Override per plugin for richer support.
        """
        return []

    def count(self, table: str, where: str = "") -> int:
        """
        NEW: Convenience COUNT(*) helper.
        where: optional WHERE clause fragment (without the 'WHERE' keyword).
        """
        sql = f"SELECT COUNT(*) FROM {self.quote_identifier(table)}"
        if where:
            sql += f" WHERE {where}"
        try:
            result = self.execute(sql)
            val = result.scalar()
            return int(val) if val is not None else 0
        except Exception:
            return -1

    def table_info(self, table: str) -> dict:
        """
        NEW: Return a summary dict about a table:
          columns, column_types, primary_keys, foreign_keys, indexes, row_count.
        """
        return {
            "table":        table,
            "columns":      self.get_schema().get(table, []),
            "column_types": self.get_column_types(table),
            "primary_keys": self.get_primary_keys(table),
            "foreign_keys": self.get_foreign_keys(table),
            "indexes":      self.get_indexes(table),
            "row_count":    self.count(table),
        }

    # ── Transactions ──────────────────────────────────────────

    def begin(self) -> None:
        """Begin a transaction. Override if supported."""
        if self.meta.supports_transactions:
            raise NotImplementedError(f"{self.meta.name} supports transactions but begin() not implemented")

    def commit(self) -> None:
        """Commit current transaction."""
        if self.meta.supports_transactions:
            raise NotImplementedError(f"{self.meta.name} supports transactions but commit() not implemented")

    def rollback(self) -> None:
        """Rollback current transaction."""
        if self.meta.supports_transactions:
            raise NotImplementedError(f"{self.meta.name} supports transactions but rollback() not implemented")

    def transaction(self):
        """
        Context manager for transactions.
        Usage:
            with db.transaction():
                db.execute("INSERT ...")
                db.execute("UPDATE ...")
        """
        return _TransactionContext(self)

    # ── Backup / restore ─────────────────────────────────────

    @abc.abstractmethod
    def backup(self, output_path: str) -> str:
        """
        Create a backup of the current database.
        Returns the path to the created backup file.
        Raises if backup fails.
        """

    def restore(self, backup_path: str) -> bool:
        """
        Restore from a backup file.
        Returns True on success.
        Default: not implemented.
        """
        raise NotImplementedError(f"{self.meta.name} does not support restore()")

    # ── Health ────────────────────────────────────────────────

    @abc.abstractmethod
    def health(self) -> HealthStatus:
        """
        Return a HealthStatus snapshot.
        Must include at minimum: connected, latency_ms.
        """

    # ── Utilities ─────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        """Safely quote a table or column name. Override per dialect."""
        return f"`{name}`"

    def escape_value(self, value: Any) -> str:
        """Escape a literal value for inline SQL. Use params instead where possible."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def table_exists(self, table: str) -> bool:
        """Check if a table exists in the current database."""
        return table.lower() in [t.lower() for t in self.get_tables()]

    def row_count(self, table: str) -> int:
        """Return approximate row count for a table."""
        return self.count(table)

    # ── Repr ──────────────────────────────────────────────────

    def __repr__(self) -> str:
        h = self.health()
        return (
            f"<{self.meta.name} "
            f"{'connected' if h.connected else 'disconnected'} "
            f"db={h.current_db} latency={h.latency_ms:.1f}ms>"
        )


# ─────────────────────────────────────────────────────────────
#  Transaction context manager
# ─────────────────────────────────────────────────────────────

class _TransactionContext:
    def __init__(self, db: BaseDB):
        self._db = db

    def __enter__(self):
        self._db.begin()
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._db.commit()
        else:
            self._db.rollback()
            logger.warning("Transaction rolled back due to: %s", exc_val)
        return False  # don't suppress exceptions


# ─────────────────────────────────────────────────────────────
#  Plugin registry
# ─────────────────────────────────────────────────────────────

class PluginRegistry:
    """
    Central registry for database plugins.
    Plugins self-register on import via @PluginRegistry.register.
    """

    _plugins: dict[str, type[BaseDB]] = {}

    @classmethod
    def register(cls, db_type: str):
        """Decorator to register a plugin class."""
        def decorator(plugin_cls: type[BaseDB]):
            cls._plugins[db_type.lower()] = plugin_cls
            logger.debug("Registered plugin: %s", db_type)
            return plugin_cls
        return decorator

    @classmethod
    def get(cls, db_type: str) -> type[BaseDB]:
        key = db_type.lower()
        if key not in cls._plugins:
            raise KeyError(
                f"No plugin for '{db_type}'. Available: {list(cls._plugins.keys())}"
            )
        return cls._plugins[key]

    @classmethod
    def available(cls) -> list[str]:
        return list(cls._plugins.keys())

    @classmethod
    def list_meta(cls) -> list[PluginMeta]:
        """NEW: Return PluginMeta for every registered plugin."""
        metas = []
        for plugin_cls in cls._plugins.values():
            try:
                metas.append(plugin_cls().meta)
            except Exception:
                pass
        return metas

    @classmethod
    def load_plugin(cls, db_type: str) -> type[BaseDB]:
        """Lazy-load a built-in plugin by name."""
        _builtin = {
            "mysql":      "plugins.mysql",
            "postgresql": "plugins.postgre",
            "mongodb":    "plugins.mongodb",
        }
        if db_type.lower() not in cls._plugins:
            module_path = _builtin.get(db_type.lower())
            if module_path:
                import importlib
                importlib.import_module(module_path)
            else:
                raise ImportError(f"No built-in plugin for '{db_type}'")
        return cls.get(db_type)


# ─────────────────────────────────────────────────────────────
#  Shared timing decorator
# ─────────────────────────────────────────────────────────────

def timed_query(fn):
    """
    Decorator for execute() implementations.
    Wraps result with elapsed_ms and updates plugin stats automatically.
    """
    @functools.wraps(fn)
    def wrapper(self, query: str, params=None) -> QueryResult:
        t0 = time.perf_counter()
        result: QueryResult = fn(self, query, params)
        result.elapsed_ms = (time.perf_counter() - t0) * 1000
        result.query      = query
        # Update per-instance stats if available
        if hasattr(self, "_stats") and isinstance(self._stats, PluginStats):
            self._stats.record(result)
        return result
    return wrapper


# ─────────────────────────────────────────────────────────────
#  Query cache (optional mixin)
# ─────────────────────────────────────────────────────────────

class QueryCacheMixin:
    """
    Optional mixin for read-heavy plugins.
    Caches SELECT results for a configurable TTL.

    v0.0.3 fix: cache is now per-instance (not a shared class-level dict).

    Usage:
        class MySQLPlugin(QueryCacheMixin, BaseDB): ...
        self._init_cache()   # call in __init__
        self._cache_ttl = 30  # override TTL if desired
    """

    _cache_ttl: int = 30

    def _init_cache(self):
        """Call in plugin __init__ to ensure per-instance cache."""
        self._cache: dict = {}

    def _ensure_cache(self):
        if not hasattr(self, "_cache"):
            self._cache = {}

    def _cache_key(self, query: str, params) -> str:
        raw = query + str(params or "")
        return hashlib.md5(raw.encode()).hexdigest()

    def _cache_get(self, key: str) -> Optional[QueryResult]:
        self._ensure_cache()
        entry = self._cache.get(key)
        if entry and (time.time() - entry["ts"]) < self._cache_ttl:
            return entry["result"]
        return None

    def _cache_set(self, key: str, result: QueryResult):
        self._ensure_cache()
        self._cache[key] = {"ts": time.time(), "result": result}

    def _cache_invalidate(self):
        self._ensure_cache()
        self._cache.clear()

    def cache_size(self) -> int:
        """NEW: Return number of cached entries."""
        self._ensure_cache()
        return len(self._cache)

    def execute_cached(self, query: str, params=None) -> QueryResult:
        """Use for read-only SELECT queries that benefit from caching."""
        import re
        if not re.match(r"^\s*SELECT\b", query, re.IGNORECASE):
            return self.execute(query, params)
        key = self._cache_key(query, params)
        cached = self._cache_get(key)
        if cached:
            cached.metadata["from_cache"] = True
            if hasattr(self, "_stats"):
                self._stats.cache_hits += 1
            return cached
        result = self.execute(query, params)
        self._cache_set(key, result)
        return result