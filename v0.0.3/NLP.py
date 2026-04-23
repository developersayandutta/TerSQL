"""
NLP.py — TerSQL Hybrid NLP Engine v0.0.3

Changes in v0.0.3:
  - NLPEngine.translate() returns ParseResult faster via early SQL passthrough
  - SchemaResolver: fuzzy edit-distance fallback for column resolution
  - ConditionParser: BETWEEN support, IN (...) list support
  - NLPEngine: new translate_batch() for multi-statement input
  - New _normalise: expand more contractions, strip common filler words
  - SQLBuilder: _build_subquery helper for nested SELECT patterns
  - SafetyChecker: also flags DROP SCHEMA / DROP VIEW
  - NLPEngine: dialect-aware SQL tweaks (e.g. ILIKE for postgresql)
"""

from __future__ import annotations

import re
import json
import logging
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

logger = logging.getLogger("tersql.nlp")


# ─────────────────────────────────────────────────────────────
#  Data classes
# ─────────────────────────────────────────────────────────────

@dataclass
class ParseResult:
    """Result returned by the NLP engine."""
    sql:          str
    intent:       str
    confidence:   float          # 0.0 – 1.0
    slots:        dict = field(default_factory=dict)
    source:       str  = "rule"  # "rule" | "passthrough"
    warnings:     list[str] = field(default_factory=list)
    is_dangerous: bool = False
    raw_input:    str  = ""

    def __bool__(self):
        return bool(self.sql)


# ─────────────────────────────────────────────────────────────
#  Condition parser
# ─────────────────────────────────────────────────────────────

class ConditionParser:
    """
    Converts natural language condition fragments to SQL WHERE clauses.

    v0.0.3 additions:
      - BETWEEN … AND … support
      - IN (a, b, c) list support
      - NOT LIKE / NOT IN expansions
    """

    OP_MAP = {
        # equality
        r"\bis\b(?!\s+not\b)":        "=",
        r"\bequals?\b":               "=",
        r"\bequal to\b":              "=",
        r"==":                        "=",
        # inequality
        r"\bis not\b":                "!=",
        r"\bisn'?t\b":                "!=",
        r"\bnot equal(?: to)?\b":     "!=",
        r"<>":                        "!=",
        # comparison
        r"\bgreater than\b":          ">",
        r"\bmore than\b":             ">",
        r"\bover\b(?=\s+\d)":         ">",
        r"\babove\b(?=\s+\d)":        ">",
        r"\bexceeds?\b":              ">",
        r"\bless than\b":             "<",
        r"\bunder\b(?=\s+\d)":        "<",
        r"\bbelow\b(?=\s+\d)":        "<",
        r"\bfewer than\b":            "<",
        r"\bat least\b":              ">=",
        r"\bminimum of\b":            ">=",
        r"\bat most\b":               "<=",
        r"\bmaximum of\b":            "<=",
        # NULL
        r"\bis null\b":               "IS NULL",
        r"\bis empty\b":              "IS NULL",
        r"\bhas no\b":                "IS NULL",
        r"\bis not null\b":           "IS NOT NULL",
        r"\bhas a\b":                 "IS NOT NULL",
        r"\bexists\b":                "IS NOT NULL",
        # LIKE
        r"\bcontains?\b":             "LIKE",
        r"\bincludes?\b":             "LIKE",
        r"\bdoes not contain\b":      "NOT LIKE",
        r"\bdoesn'?t contain\b":      "NOT LIKE",
        # IN
        r"\bin\b(?=\s*\()":           "IN",
        r"\bnot in\b(?=\s*\()":       "NOT IN",
    }

    _NUMERIC_RE  = re.compile(r"^-?\d+(\.\d+)?$")
    _DATE_RE     = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _BOOL_TRUE   = {"true", "yes", "1", "active", "enabled", "on"}
    _BOOL_FALSE  = {"false", "no", "0", "inactive", "disabled", "off"}

    # NEW: BETWEEN pattern
    _BETWEEN_RE  = re.compile(
        r"(.+?)\s+(?:is\s+)?between\s+(.+?)\s+and\s+(.+)", re.IGNORECASE
    )
    # NEW: IN list pattern: col in a, b, c  (no parens)
    _IN_LIST_RE  = re.compile(
        r"(.+?)\s+(?:is\s+)?(?:one of|in)\s+(.+)", re.IGNORECASE
    )

    def parse(self, fragment: str) -> str:
        fragment = fragment.strip()

        # Already looks like SQL condition
        if re.search(r"[=<>!]=?|IS\s+(?:NOT\s+)?NULL|LIKE|BETWEEN|IN\s*\(", fragment, re.IGNORECASE):
            return self._quote_values(fragment)

        # BETWEEN
        m = self._BETWEEN_RE.match(fragment)
        if m:
            col  = m.group(1).strip().replace(" ", "_")
            low  = self._coerce_value(m.group(2).strip())
            high = self._coerce_value(m.group(3).strip())
            return f"{col} BETWEEN {low} AND {high}"

        # IN list (without parens)
        m = self._IN_LIST_RE.match(fragment)
        if m:
            col  = m.group(1).strip().replace(" ", "_")
            vals = [self._coerce_value(v.strip()) for v in m.group(2).split(",")]
            return f"{col} IN ({', '.join(vals)})"

        # Standard operator map
        frag = fragment
        found_op = None
        sql_op   = None

        for pattern, op in self.OP_MAP.items():
            m = re.search(pattern, frag, re.IGNORECASE)
            if m:
                found_op = m.group(0)
                sql_op   = op
                frag     = frag[:m.start()].strip() + "|||" + frag[m.end():].strip()
                break

        if found_op:
            parts = frag.split("|||", 1)
            col   = parts[0].strip().replace(" ", "_")
            val   = parts[1].strip() if len(parts) > 1 else ""

            if sql_op in ("IS NULL", "IS NOT NULL"):
                return f"{col} {sql_op}"

            if sql_op in ("LIKE", "NOT LIKE"):
                val = val.strip("'\"")
                return f"{col} {sql_op} '%{val}%'"

            val = self._coerce_value(val)
            return f"{col} {sql_op} {val}"

        # AND / OR compound
        for conj in (" and ", " or "):
            sql_conj = conj.strip().upper()
            if conj in frag.lower():
                parts = re.split(conj, frag, flags=re.IGNORECASE, maxsplit=1)
                if len(parts) == 2:
                    left  = self.parse(parts[0])
                    right = self.parse(parts[1])
                    return f"({left} {sql_conj} {right})"

        return fragment

    def _coerce_value(self, val: str) -> str:
        val = val.strip().strip("'\"")
        if self._NUMERIC_RE.match(val):
            return val
        if self._DATE_RE.match(val):
            return f"'{val}'"
        if val.lower() in self._BOOL_TRUE:
            return "1"
        if val.lower() in self._BOOL_FALSE:
            return "0"
        return f"'{val}'"

    def _quote_values(self, condition: str) -> str:
        def _repl(m):
            op, val = m.group(1), m.group(2).strip()
            if val.startswith("'") or val.startswith('"') or self._NUMERIC_RE.match(val):
                return m.group(0)
            return f"{op} '{val}'"
        return re.sub(r"(=|!=|<>|<=?|>=?)\s*([A-Za-z_]\w*)", _repl, condition)


# ─────────────────────────────────────────────────────────────
#  Schema resolver
# ─────────────────────────────────────────────────────────────

class SchemaResolver:
    """
    Resolves ambiguous table/column names against the live schema.

    v0.0.3: Added simple edit-distance fallback for minor typos.
    """

    def __init__(self, schema: dict):
        self.schema   = {k.lower(): [c.lower() for c in v] for k, v in schema.items()}
        self._aliases = self._build_aliases()

    def _build_aliases(self) -> dict:
        aliases = {}
        for tbl in self.schema:
            if tbl.endswith("s"):
                aliases[tbl[:-1]] = tbl
            else:
                aliases[tbl + "s"] = tbl
            parts = tbl.split("_")
            if len(parts) > 1:
                abbrev = "".join(p[0] for p in parts)
                aliases[abbrev] = tbl
        return aliases

    @staticmethod
    def _edit_distance(a: str, b: str) -> int:
        """Simple Levenshtein distance (max len 30 to stay cheap)."""
        if len(a) > 30 or len(b) > 30:
            return 99
        dp = list(range(len(b) + 1))
        for i, ca in enumerate(a):
            ndp = [i + 1]
            for j, cb in enumerate(b):
                ndp.append(min(dp[j] + (0 if ca == cb else 1),
                               dp[j + 1] + 1, ndp[j] + 1))
            dp = ndp
        return dp[-1]

    def resolve_table(self, name: str) -> Optional[str]:
        n = name.lower().strip()
        if n in self.schema:
            return n
        if n in self._aliases:
            return self._aliases[n]
        # Prefix match
        candidates = [t for t in self.schema if t.startswith(n) or n.startswith(t)]
        if len(candidates) == 1:
            return candidates[0]
        # NEW: fuzzy edit-distance (tolerance = 2)
        scored = [(self._edit_distance(n, t), t) for t in self.schema]
        scored.sort()
        if scored and scored[0][0] <= 2:
            return scored[0][1]
        return None

    def resolve_column(self, table: str, col_fragment: str) -> Optional[str]:
        tbl = self.resolve_table(table)
        if not tbl or tbl not in self.schema:
            return None
        cols = self.schema[tbl]
        cf   = col_fragment.lower().strip()
        if cf in cols:
            return cf
        # Prefix match
        candidates = [c for c in cols if c.startswith(cf)]
        if len(candidates) == 1:
            return candidates[0]
        # NEW: fuzzy match
        scored = [(self._edit_distance(cf, c), c) for c in cols]
        scored.sort()
        if scored and scored[0][0] <= 2:
            return scored[0][1]
        return None

    def all_columns(self) -> list[str]:
        seen = []
        for cols in self.schema.values():
            for c in cols:
                if c not in seen:
                    seen.append(c)
        return seen


# ─────────────────────────────────────────────────────────────
#  Intent matcher
# ─────────────────────────────────────────────────────────────

class IntentMatcher:
    """
    Loads intents from Intent.json and matches normalised user input.
    """

    def __init__(self, intent_path: str):
        with open(intent_path, encoding="utf-8") as f:
            data = json.load(f)
        self._intents: list[dict] = data if isinstance(data, list) else data.get("intents", [])
        self._compiled = self._compile()

    def _compile(self) -> list[tuple]:
        compiled = []
        for intent in self._intents:
            patterns = []
            for pat in intent.get("patterns", []):
                try:
                    patterns.append(re.compile(pat, re.IGNORECASE))
                except re.error as e:
                    logger.warning("Bad intent pattern '%s': %s", pat, e)
            compiled.append((intent, patterns))
        return compiled

    def match(self, text: str) -> tuple[Optional[dict], dict, float]:
        """Returns (intent_dict, slots, confidence)."""
        best_intent    = None
        best_slots     = {}
        best_confidence = 0.0

        for intent, patterns in self._compiled:
            for pattern in patterns:
                m = pattern.search(text)
                if m:
                    slots = {k: v for k, v in m.groupdict().items() if v is not None}
                    # Base confidence from pattern specificity (named groups → higher)
                    conf = 0.7 + 0.05 * len(slots)
                    conf = min(conf, 0.98)
                    if conf > best_confidence:
                        best_confidence = conf
                        best_intent     = intent
                        best_slots      = slots

        return best_intent, best_slots, best_confidence


# ─────────────────────────────────────────────────────────────
#  SQL Builder
# ─────────────────────────────────────────────────────────────

class SQLBuilder:
    """
    Converts matched intent + slots → SQL string.
    v0.0.3: _build_subquery, dialect-aware LIKE (ILIKE for postgresql).
    """

    _DISPATCH = {
        "select":          "_build_select",
        "select_where":    "_build_select_where",
        "select_join":     "_build_join",
        "select_group":    "_build_group",
        "select_order":    "_build_order",
        "select_limit":    "_build_limit",
        "select_agg":      "_build_aggregate",
        "count":           "_build_count",
        "insert":          "_build_insert",
        "update":          "_build_update",
        "delete":          "_build_delete",
        "describe_table":  "_build_describe_table",
        "show_tables":     "_build_show_tables",
        "show_databases":  "_build_show_databases",
        "drop_table":      "_build_drop_table",
        "distinct":        "_build_distinct",
        "like_search":     "_build_like_search",
        "date_filter":     "_build_date_filter",
        "having_filter":   "_build_having_filter",
    }

    def __init__(self, cond_parser: ConditionParser,
                 resolver: Optional[SchemaResolver] = None,
                 dialect: str = "mysql"):
        self._cond    = cond_parser
        self._res     = resolver
        self._dialect = dialect

    def build(self, intent: dict, slots: dict) -> tuple[str, list[str]]:
        warnings: list[str] = []
        name = intent.get("name", "")

        # Resolve table/column names against schema
        if self._res and "table" in slots:
            resolved = self._res.resolve_table(slots["table"])
            if resolved and resolved != slots["table"]:
                warnings.append(f"Resolved table '{slots['table']}' → '{resolved}'")
                slots["table"] = resolved

        # Resolve condition columns
        if self._res and "condition" in slots and slots.get("table"):
            slots["condition"] = self._resolve_condition_columns(
                slots["condition"], slots["table"]
            )

        handler = self._DISPATCH.get(name)
        if handler and hasattr(self, handler):
            return getattr(self, handler)(intent, slots, warnings)
        return self._generic_build(intent, slots, warnings)

    def _resolve_condition_columns(self, condition: str, table: str) -> str:
        """Try to resolve bare column names in a condition string."""
        if not self._res:
            return condition

        def _try_resolve(word: str) -> str:
            resolved = self._res.resolve_column(table, word)
            return resolved if resolved else word

        # Simple word-by-word replacement for bare identifiers
        return re.sub(
            r"\b([a-z_]\w*)\b(?!\s*[=(])",
            lambda m: _try_resolve(m.group(1)),
            condition,
            flags=re.IGNORECASE,
        )

    def _like_op(self) -> str:
        """NEW: Return ILIKE for postgresql, LIKE otherwise."""
        return "ILIKE" if self._dialect == "postgresql" else "LIKE"

    # ── Builders ─────────────────────────────────────────────

    def _build_select(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        sql  = f"SELECT {cols} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        return sql, warnings

    def _build_select_where(self, intent, slots, warnings):
        cols      = slots.get("columns", "*")
        condition = self._cond.parse(slots.get("condition", ""))
        sql       = f"SELECT {cols} FROM {slots['table']}"
        if condition:
            sql += f" WHERE {condition}"
        return sql, warnings

    def _build_join(self, intent, slots, warnings):
        t1   = slots.get("table", "")
        t2   = slots.get("table2", "")
        on   = slots.get("on_clause", "")
        kind = slots.get("join_type", "INNER").upper()
        if kind not in ("INNER", "LEFT", "RIGHT", "FULL", "CROSS"):
            kind = "INNER"
        sql = f"SELECT * FROM {t1} {kind} JOIN {t2} ON {on}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        return sql, warnings

    def _build_group(self, intent, slots, warnings):
        cols     = slots.get("columns", "*")
        group_by = slots.get("group_col", slots.get("columns", ""))
        sql      = f"SELECT {group_by}, {cols} FROM {slots['table']} GROUP BY {group_by}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        return sql, warnings

    def _build_order(self, intent, slots, warnings):
        cols  = slots.get("columns", "*")
        order = slots.get("order_col", cols)
        dir_  = slots.get("direction", "ASC").upper()
        if dir_ not in ("ASC", "DESC"):
            dir_ = "ASC"
        sql = f"SELECT {cols} FROM {slots['table']} ORDER BY {order} {dir_}"
        if slots.get("limit"):
            sql += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_limit(self, intent, slots, warnings):
        cols  = slots.get("columns", "*")
        limit = slots.get("limit", 10)
        sql   = f"SELECT {cols} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        sql += f" LIMIT {limit}"
        return sql, warnings

    def _build_aggregate(self, intent, slots, warnings):
        agg  = slots.get("agg_func", "COUNT").upper()
        col  = slots.get("agg_col", "*")
        tbl  = slots.get("table", "")
        gcol = slots.get("group_col", "")
        sql  = f"SELECT {agg}({col}) AS {agg.lower()}_{col} FROM {tbl}"
        if gcol:
            sql = f"SELECT {gcol}, {agg}({col}) AS {agg.lower()}_{col} FROM {tbl} GROUP BY {gcol}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        return sql, warnings

    def _build_count(self, intent, slots, warnings):
        sql = f"SELECT COUNT(*) AS total FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {self._cond.parse(slots['condition'])}"
        return sql, warnings

    def _build_insert(self, intent, slots, warnings):
        tbl  = slots.get("table", "")
        cols = slots.get("columns", "")
        vals = slots.get("values", "")
        if cols:
            return f"INSERT INTO {tbl} ({cols}) VALUES ({vals})", warnings
        return f"INSERT INTO {tbl} VALUES ({vals})", warnings

    def _build_update(self, intent, slots, warnings):
        set_clause = slots.get("set_clause", "")
        condition  = slots.get("condition", "")
        if not condition:
            warnings.append("UPDATE without WHERE clause is dangerous")
        return f"UPDATE {slots['table']} SET {set_clause} WHERE {condition}", warnings

    def _build_delete(self, intent, slots, warnings):
        condition = slots.get("condition", "")
        if not condition:
            warnings.append("DELETE without WHERE will remove ALL rows")
        sql = f"DELETE FROM {slots['table']}"
        if condition:
            sql += f" WHERE {condition}"
        return sql, warnings

    def _build_describe_table(self, intent, slots, warnings):
        return f"DESCRIBE {slots['table']}", warnings

    def _build_show_tables(self, intent, slots, warnings):
        return "SHOW TABLES", warnings

    def _build_show_databases(self, intent, slots, warnings):
        return "SHOW DATABASES", warnings

    def _build_drop_table(self, intent, slots, warnings):
        return f"DROP TABLE {slots['table']}", warnings

    def _build_distinct(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        sql  = f"SELECT DISTINCT {cols} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_like_search(self, intent, slots, warnings):
        tbl  = slots["table"]
        col  = slots["column"]
        val  = slots["value"].strip("'\"")
        like = self._like_op()
        return f"SELECT * FROM {tbl} WHERE {col} {like} '%{val}%'", warnings

    def _build_date_filter(self, intent, slots, warnings):
        tbl = slots["table"]
        if slots.get("days"):
            return (
                f"SELECT * FROM {tbl} WHERE created_at >= "
                f"DATE_SUB(NOW(), INTERVAL {slots['days']} DAY)",
                warnings,
            )
        if slots.get("date_from") and slots.get("date_to"):
            return (
                f"SELECT * FROM {tbl} WHERE created_at BETWEEN "
                f"'{slots['date_from']}' AND '{slots['date_to']}'",
                warnings,
            )
        if slots.get("date_from"):
            return (
                f"SELECT * FROM {tbl} WHERE created_at >= '{slots['date_from']}'",
                warnings,
            )
        return f"SELECT * FROM {tbl}", warnings

    def _build_having_filter(self, intent, slots, warnings):
        gcol = slots["group_col"]
        tbl  = slots["table"]
        hval = slots.get("having_value", "0")
        sql  = (
            f"SELECT {gcol}, COUNT(*) AS count FROM {tbl} "
            f"GROUP BY {gcol} HAVING count > {hval}"
        )
        return sql, warnings

    def _build_subquery(self, outer_table: str, outer_col: str,
                        inner_sql: str) -> str:
        """NEW in v0.0.3: Helper to wrap a subquery in a WHERE IN clause."""
        return f"SELECT * FROM {outer_table} WHERE {outer_col} IN ({inner_sql})"

    def _generic_build(self, intent, slots, warnings):
        tmpl = intent.get("sql_template", "")
        if not tmpl:
            return "", ["No SQL template found for intent: " + intent["name"]]
        try:
            sql = tmpl.format(**slots)
            return sql, warnings
        except KeyError as e:
            warnings.append(f"Missing slot {e} for template")
            return "", warnings


# ─────────────────────────────────────────────────────────────
#  Safety detector
# ─────────────────────────────────────────────────────────────

class SafetyChecker:
    """
    Detects dangerous SQL patterns.
    v0.0.3: added DROP SCHEMA / DROP VIEW / DROP FUNCTION.
    """

    DANGEROUS_KEYWORDS = {
        r"\bDROP\s+TABLE\b",
        r"\bDROP\s+DATABASE\b",
        r"\bDROP\s+SCHEMA\b",      # NEW
        r"\bDROP\s+VIEW\b",        # NEW
        r"\bDROP\s+FUNCTION\b",    # NEW
        r"\bDROP\s+PROCEDURE\b",   # NEW
        r"\bTRUNCATE\b",
        r"\bDELETE\s+FROM\b",
        r"\bALTER\s+TABLE\b",
        r"\bDROP\s+INDEX\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
    }

    UNGUARDED_PATTERNS = {
        r"\bDELETE\s+FROM\s+\w+\s*;?\s*$",
    }

    def check(self, sql: str) -> tuple[bool, str]:
        upped = sql.upper()

        for pat in self.DANGEROUS_KEYWORDS:
            if re.search(pat, upped):
                return True, f"Contains dangerous keyword: {pat}"

        for pat in self.UNGUARDED_PATTERNS:
            if re.search(pat, upped, re.DOTALL):
                return True, "DELETE statement without WHERE clause"

        if (re.search(r"\bUPDATE\s+\w+\s+SET\b", upped, re.DOTALL)
                and not re.search(r"\bWHERE\b", upped, re.DOTALL)):
            return True, "UPDATE statement without WHERE clause"

        return False, ""


# ─────────────────────────────────────────────────────────────
#  Main NLP engine
# ─────────────────────────────────────────────────────────────

class NLPEngine:
    """
    Facade that orchestrates:
      1. Pass-through for raw SQL
      2. Rule-based intent matching + slot-filling

    v0.0.3: translate_batch(), dialect-aware builder, richer normalise.
    """

    _SQL_START = re.compile(
        r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|SHOW|DESCRIBE|USE|EXPLAIN|WITH|CALL)\b",
        re.IGNORECASE,
    )

    # Filler words stripped from NL input before matching
    _FILLER = re.compile(
        r"\b(please|kindly|can you|could you|i want to|i need to|"
        r"tell me|show me|give me|fetch|retrieve|get|find)\b",
        re.IGNORECASE,
    )

    def __init__(
        self,
        intent_path:          Optional[str]  = None,
        schema:               Optional[dict] = None,
        dialect:              str            = "mysql",
        confidence_threshold: float          = 0.55,
    ):
        base  = Path(__file__).parent
        ipath = intent_path or str(base / "Intent.json")

        self._matcher   = IntentMatcher(ipath)
        self._cond_p    = ConditionParser()
        self._safety    = SafetyChecker()
        self._dialect   = dialect
        self._threshold = confidence_threshold
        self._schema    = schema or {}
        self._resolver  = SchemaResolver(self._schema) if self._schema else None
        self._builder   = SQLBuilder(self._cond_p, self._resolver, dialect)

    def update_schema(self, schema: dict):
        """Hot-reload schema after a USE <db> change."""
        self._schema   = schema
        self._resolver = SchemaResolver(schema)
        self._builder  = SQLBuilder(self._cond_p, self._resolver, self._dialect)

    def translate(self, text: str) -> ParseResult:
        """Main entry point: text → ParseResult."""
        raw = text.strip()

        # ── 1. Pass-through for raw SQL ────────────────────────
        if self._SQL_START.match(raw):
            dangerous, reason = self._safety.check(raw)
            return ParseResult(
                sql=raw,
                intent="passthrough",
                confidence=1.0,
                source="passthrough",
                is_dangerous=dangerous,
                warnings=[reason] if dangerous else [],
                raw_input=raw,
            )

        normalised = self._normalise(raw)

        # ── 2. Rule-based matching ─────────────────────────────
        intent, slots, confidence = self._matcher.match(normalised)

        if intent and confidence >= self._threshold:
            sql, warnings = self._builder.build(intent, slots)
            if sql:
                dangerous, danger_reason = self._safety.check(sql)
                if dangerous:
                    warnings.append(danger_reason)
                return ParseResult(
                    sql=sql,
                    intent=intent["name"],
                    confidence=confidence,
                    slots=slots,
                    source="rule",
                    is_dangerous=dangerous,
                    warnings=warnings,
                    raw_input=raw,
                )

        return ParseResult(
            sql="",
            intent="unknown",
            confidence=0.0,
            source="none",
            warnings=["Could not parse query. Try rephrasing or use raw SQL."],
            raw_input=raw,
        )

    def translate_batch(self, texts: list[str]) -> list[ParseResult]:
        """NEW in v0.0.3: Translate multiple NL queries at once."""
        return [self.translate(t) for t in texts]

    # ── Helpers ──

    def _normalise(self, text: str) -> str:
        """Lowercase, collapse whitespace, strip filler words and trailing punctuation."""
        text = text.lower().strip().rstrip("?.")
        text = re.sub(r"\s+", " ", text)
        # Expand contractions
        text = re.sub(r"\bdon't\b",  "do not",     text)
        text = re.sub(r"\bcan't\b",  "cannot",     text)
        text = re.sub(r"\bisn't\b",  "is not",     text)
        text = re.sub(r"\bwon't\b",  "will not",   text)
        text = re.sub(r"\bdidn't\b", "did not",    text)
        text = re.sub(r"\bhasn't\b", "has not",    text)
        text = re.sub(r"\baren't\b", "are not",    text)
        # Strip filler words (NEW)
        text = self._FILLER.sub("", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text