"""
NLP.py — TerSQL Hybrid NLP Engine v0.0.2
Converts natural language → SQL using:
  1. Rule-based slot-filling (Intent.json patterns)
  2. Schema-aware column/table resolution
  3. Groq API fallback with structured prompting
"""

from __future__ import annotations

import re
import json
import os
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
    sql: str
    intent: str
    confidence: float          # 0.0 – 1.0
    slots: dict = field(default_factory=dict)
    source: str = "rule"       # "rule" | "groq" | "passthrough"
    warnings: list[str] = field(default_factory=list)
    is_dangerous: bool = False
    raw_input: str = ""

    def __bool__(self):
        return bool(self.sql)


# ─────────────────────────────────────────────────────────────
#  Condition parser
# ─────────────────────────────────────────────────────────────

class ConditionParser:
    """
    Converts natural language condition fragments to SQL WHERE clauses.
    Examples:
      "age > 30"           → "age > 30"
      "name is alice"      → "name = 'alice'"
      "status equals active" → "status = 'active'"
      "salary more than 50000" → "salary > 50000"
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
        # IN
        r"\bin\b(?=\s*\()":           "IN",
        r"\bnot in\b(?=\s*\()":       "NOT IN",
    }

    _NUMERIC_RE  = re.compile(r"^-?\d+(\.\d+)?$")
    _DATE_RE     = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _BOOL_TRUE   = {"true", "yes", "1", "active", "enabled", "on"}
    _BOOL_FALSE  = {"false", "no", "0", "inactive", "disabled", "off"}

    def parse(self, fragment: str) -> str:
        """
        Try to convert a natural language condition fragment to SQL.
        Falls back to returning the original fragment unchanged if already SQL-like.
        """
        fragment = fragment.strip()

        # Already looks like SQL condition — check for raw operators
        if re.search(r"[=<>!]=?|IS\s+(?:NOT\s+)?NULL|LIKE|BETWEEN|IN\s*\(", fragment, re.IGNORECASE):
            return self._quote_values(fragment)

        # Try to parse natural language
        frag = fragment
        found_op = None
        sql_op = None

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

            if sql_op == "LIKE":
                val = val.strip("'\"")
                return f"{col} LIKE '%{val}%'"

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

        # Can't parse — return as-is and let Groq handle it
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
        """Quote bare string values in already-SQL-like conditions."""
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
    Supports fuzzy matching for typos and aliases.
    """

    def __init__(self, schema: dict):
        # schema = {"table_name": ["col1", "col2", ...], ...}
        self.schema   = {k.lower(): [c.lower() for c in v] for k, v in schema.items()}
        self._aliases = self._build_aliases()

    def _build_aliases(self) -> dict:
        aliases = {}
        for tbl in self.schema:
            # singular/plural
            if tbl.endswith("s"):
                aliases[tbl[:-1]] = tbl
            else:
                aliases[tbl + "s"] = tbl
            # common abbreviations
            parts = tbl.split("_")
            if len(parts) > 1:
                abbrev = "".join(p[0] for p in parts)
                aliases[abbrev] = tbl
        return aliases

    def resolve_table(self, name: str) -> Optional[str]:
        n = name.lower().strip()
        if n in self.schema:
            return n
        if n in self._aliases:
            return self._aliases[n]
        # Fuzzy: starts with
        candidates = [t for t in self.schema if t.startswith(n) or n.startswith(t)]
        if len(candidates) == 1:
            return candidates[0]
        return None

    def resolve_column(self, table: str, col_fragment: str) -> Optional[str]:
        tbl = self.resolve_table(table)
        if not tbl or tbl not in self.schema:
            return col_fragment
        cols = self.schema[tbl]
        c    = col_fragment.lower().strip()
        if c in cols:
            return c
        candidates = [col for col in cols if col.startswith(c) or c in col]
        if len(candidates) == 1:
            return candidates[0]
        return col_fragment

    def infer_join_condition(self, table1: str, table2: str) -> Optional[str]:
        """Infer ON clause using FK naming conventions."""
        t1 = self.resolve_table(table1)
        t2 = self.resolve_table(table2)
        if not t1 or not t2:
            return None

        cols1 = self.schema.get(t1, [])
        cols2 = self.schema.get(t2, [])

        # t2 has a column like t1_id
        fk_name = f"{t1.rstrip('s')}_id"
        if fk_name in cols2:
            return f"{t1}.id = {t2}.{fk_name}"

        # t1 has a column like t2_id
        fk_name2 = f"{t2.rstrip('s')}_id"
        if fk_name2 in cols1:
            return f"{t1}.{fk_name2} = {t2}.id"

        # Both have 'id' — try common patterns
        if "id" in cols1 and "id" in cols2:
            return f"{t1}.id = {t2}.{t1.rstrip('s')}_id"

        return None

    def columns_for(self, table: str) -> list[str]:
        tbl = self.resolve_table(table)
        return self.schema.get(tbl, []) if tbl else []


# ─────────────────────────────────────────────────────────────
#  Intent matcher
# ─────────────────────────────────────────────────────────────

class IntentMatcher:
    """Matches input against Intent.json patterns and extracts slots."""

    def __init__(self, intent_path: str):
        with open(intent_path, encoding="utf-8") as f:
            self._data = json.load(f)
        self._intents    = sorted(self._data["intents"], key=lambda x: -x.get("priority", 0))
        self._cond_ops   = self._data.get("condition_operators", {})
        self._schema_hints = self._data.get("schema_hints", {})

    def match(self, text: str) -> tuple[Optional[dict], dict, float]:
        """
        Returns (intent_dict, slots, confidence).
        confidence is 0.0–1.0 based on match quality.
        """
        t = text.strip()

        for intent in self._intents:
            for pattern in intent.get("patterns", []):
                try:
                    m = re.match(pattern, t, re.IGNORECASE)
                    if m:
                        slots = dict(intent.get("slot_defaults", {}))
                        slots.update({k: v for k, v in m.groupdict().items() if v is not None})
                        slots = self._clean_slots(slots)
                        confidence = self._score_match(m, t, pattern)
                        return intent, slots, confidence
                except re.error:
                    logger.debug("Regex error in pattern: %s", pattern)

        return None, {}, 0.0

    def _clean_slots(self, slots: dict) -> dict:
        cleaned = {}
        for k, v in slots.items():
            if isinstance(v, str):
                v = v.strip().strip("'\"")
                # Normalise boolean-like columns
                if k == "columns":
                    v = self._normalise_columns(v)
            cleaned[k] = v
        return cleaned

    def _normalise_columns(self, cols_str: str) -> str:
        """Standardise column list: remove duplicate spaces, split on comma/and."""
        cols_str = re.sub(r"\band\b", ",", cols_str, flags=re.IGNORECASE)
        cols     = [c.strip() for c in cols_str.split(",") if c.strip()]
        return ", ".join(cols)

    def _score_match(self, match, text: str, pattern: str) -> float:
        """
        Rough confidence score:
        - Full string match (pos 0, covers whole string) → 0.9+
        - Partial match → 0.6–0.8
        """
        if match.start() == 0 and match.end() == len(text):
            return min(0.95, 0.75 + 0.05 * len(match.groupdict()))
        coverage = (match.end() - match.start()) / len(text)
        return round(0.5 + 0.3 * coverage, 2)


# ─────────────────────────────────────────────────────────────
#  SQL builder
# ─────────────────────────────────────────────────────────────

class SQLBuilder:
    """Builds final SQL from matched intent + slots."""

    def __init__(self, condition_parser: ConditionParser, schema_resolver: Optional[SchemaResolver]):
        self._cp = condition_parser
        self._sr = schema_resolver

    def build(self, intent: dict, slots: dict) -> tuple[str, list[str]]:
        """Returns (sql, warnings)."""
        warnings = []
        name = intent["name"]

        # Resolve table/column names against live schema
        if self._sr:
            slots, w = self._resolve_slots(intent, slots)
            warnings.extend(w)

        # Parse condition fragment if present
        if "condition" in slots and slots["condition"]:
            slots["condition"] = self._cp.parse(slots["condition"])

        # Dispatch to builder
        builder = getattr(self, f"_build_{name}", None)
        if builder:
            return builder(intent, slots, warnings)

        # Generic template substitution
        return self._generic_build(intent, slots, warnings)

    def _resolve_slots(self, intent: dict, slots: dict) -> tuple[dict, list[str]]:
        warnings = []
        if "table" in slots and slots["table"]:
            resolved = self._sr.resolve_table(slots["table"])
            if resolved and resolved != slots["table"].lower():
                warnings.append(f"Resolved table '{slots['table']}' → '{resolved}'")
            slots["table"] = resolved or slots["table"]

        if "join_table" in slots and slots["join_table"]:
            resolved = self._sr.resolve_table(slots["join_table"])
            slots["join_table"] = resolved or slots["join_table"]

        # Auto-infer join condition if missing
        if intent["name"] in ("join_inner", "join_left") and not slots.get("join_on") and self._sr:
            inferred = self._sr.infer_join_condition(slots.get("table", ""), slots.get("join_table", ""))
            if inferred:
                slots["join_on"] = inferred
                warnings.append(f"Inferred JOIN condition: {inferred}")
            else:
                warnings.append("Could not infer JOIN condition — please specify ON clause")

        return slots, warnings

    # ── Specific builders ──

    def _build_select_all(self, intent, slots, warnings):
        sql = f"SELECT * FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        if slots.get("limit"):
            sql += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_select_columns(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        sql  = f"SELECT {cols} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_select_with_filter(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        sql  = f"SELECT {cols} FROM {slots['table']} WHERE {slots['condition']}"
        if slots.get("limit"):
            sql += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_count_rows(self, intent, slots, warnings):
        col = slots.get("count_col", "*")
        sql = f"SELECT COUNT({col}) AS total FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_aggregate_sum(self, intent, slots, warnings):
        col = slots["column"]
        sql = f"SELECT SUM({col}) AS total_{col} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_aggregate_avg(self, intent, slots, warnings):
        col = slots["column"]
        sql = f"SELECT AVG({col}) AS avg_{col} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_aggregate_max_min(self, intent, slots, warnings):
        col = slots["column"]
        # Detect max vs min from the original pattern trigger
        trigger = slots.get("_trigger", "").lower()
        fn = "MIN" if any(w in trigger for w in ("lowest", "minimum", "min", "smallest")) else "MAX"
        label = fn.lower()
        sql = f"SELECT {fn}({col}) AS {label}_{col} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_group_by(self, intent, slots, warnings):
        gcol = slots["group_col"]
        tbl  = slots["table"]
        if slots.get("column"):
            col = slots["column"]
            sql = f"SELECT {gcol}, COUNT(*) AS count, SUM({col}) AS total_{col} FROM {tbl} GROUP BY {gcol}"
        else:
            sql = f"SELECT {gcol}, COUNT(*) AS count FROM {tbl} GROUP BY {gcol}"
        if slots.get("having_value"):
            sql += f" HAVING count > {slots['having_value']}"
        if slots.get("order_col"):
            sql += f" ORDER BY {slots['order_col']} {slots.get('order_dir', 'DESC')}"
        return sql, warnings

    def _build_join_inner(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        t1   = slots["table"]
        t2   = slots["join_table"]
        on   = slots.get("join_on", f"{t1}.id = {t2}.{t1}_id")
        sql  = f"SELECT {cols} FROM {t1} INNER JOIN {t2} ON {on}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        if slots.get("limit"):
            sql += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_join_left(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        t1   = slots["table"]
        t2   = slots["join_table"]
        on   = slots.get("join_on", f"{t1}.id = {t2}.{t1}_id")
        sql  = f"SELECT {cols} FROM {t1} LEFT JOIN {t2} ON {on}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        return sql, warnings

    def _build_order_by(self, intent, slots, warnings):
        cols  = slots.get("columns", "*")
        tbl   = slots["table"]
        ocol  = slots["order_col"]
        odir  = slots.get("order_dir", "ASC").upper()
        if odir.startswith("DESC") or odir == "D":
            odir = "DESC"
        else:
            odir = "ASC"
        sql   = f"SELECT {cols} FROM {tbl} ORDER BY {ocol} {odir}"
        if slots.get("limit"):
            sql += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_limit(self, intent, slots, warnings):
        cols = slots.get("columns", "*")
        sql  = f"SELECT {cols} FROM {slots['table']}"
        if slots.get("condition"):
            sql += f" WHERE {slots['condition']}"
        sql  += f" LIMIT {slots['limit']}"
        return sql, warnings

    def _build_insert(self, intent, slots, warnings):
        """Parse 'col=val, col2=val2' or 'val1, val2' format."""
        raw  = slots.get("values", "")
        tbl  = slots["table"]

        # col=val format
        pairs = re.findall(r"(\w+)\s*=\s*([^,]+)", raw)
        if pairs:
            cols = ", ".join(p[0] for p in pairs)
            vals = ", ".join(ConditionParser()._coerce_value(p[1]) for p in pairs)
            return f"INSERT INTO {tbl} ({cols}) VALUES ({vals})", warnings

        # Bare values
        val_list = [v.strip() for v in raw.split(",")]
        vals = ", ".join(ConditionParser()._coerce_value(v) for v in val_list)
        warnings.append("Column names not specified — using positional VALUES")
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
        tbl = slots["table"]
        col = slots["column"]
        val = slots["value"].strip("'\"")
        return f"SELECT * FROM {tbl} WHERE {col} LIKE '%{val}%'", warnings

    def _build_date_filter(self, intent, slots, warnings):
        tbl = slots["table"]
        if slots.get("days"):
            return f"SELECT * FROM {tbl} WHERE created_at >= DATE_SUB(NOW(), INTERVAL {slots['days']} DAY)", warnings
        if slots.get("date_from") and slots.get("date_to"):
            return f"SELECT * FROM {tbl} WHERE created_at BETWEEN '{slots['date_from']}' AND '{slots['date_to']}'", warnings
        if slots.get("date_from"):
            return f"SELECT * FROM {tbl} WHERE created_at >= '{slots['date_from']}'", warnings
        return f"SELECT * FROM {tbl}", warnings

    def _build_having_filter(self, intent, slots, warnings):
        gcol = slots["group_col"]
        tbl  = slots["table"]
        hval = slots.get("having_value", "0")
        sql  = f"SELECT {gcol}, COUNT(*) AS count FROM {tbl} GROUP BY {gcol} HAVING count > {hval}"
        return sql, warnings

    def _generic_build(self, intent, slots, warnings):
        """Fallback: substitute slots into the first sql_template."""
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
    """Detects dangerous SQL patterns."""

    DANGEROUS_KEYWORDS = {
        r"\bDROP\s+TABLE\b",
        r"\bDROP\s+DATABASE\b",
        r"\bTRUNCATE\b",
        r"\bDELETE\s+FROM\b",
        r"\bALTER\s+TABLE\b",
        r"\bDROP\s+INDEX\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
    }

    UNGUARDED_PATTERNS = {
        # DELETE/UPDATE without WHERE
        r"\bDELETE\s+FROM\s+\w+\s*;?\s*$",
    }

    def check(self, sql: str) -> tuple[bool, str]:
        """Returns (is_dangerous, reason)."""
        upped = sql.upper()

        for pat in self.DANGEROUS_KEYWORDS:
            if re.search(pat, upped):
                return True, f"Contains dangerous keyword: {pat}"

        for pat in self.UNGUARDED_PATTERNS:
            if re.search(pat, upped, re.DOTALL):
                return True, "DELETE statement without WHERE clause"
                
        if re.search(r"\bUPDATE\s+\w+\s+SET\b", upped, re.DOTALL) and not re.search(r"\bWHERE\b", upped, re.DOTALL):
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
      3. Groq fallback
    """

    # SQL passthrough detection
    _SQL_START = re.compile(
        r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|SHOW|DESCRIBE|USE|EXPLAIN|WITH|CALL)\b",
        re.IGNORECASE,
    )

    def __init__(
        self,
        intent_path: Optional[str] = None,
        schema: Optional[dict] = None,
        dialect: str = "mysql",
        confidence_threshold: float = 0.55,
    ):
        base      = Path(__file__).parent
        ipath     = intent_path or str(base / "Intent.json")

        self._matcher   = IntentMatcher(ipath)
        self._cond_p    = ConditionParser()
        self._safety    = SafetyChecker()
        self._dialect   = dialect
        self._threshold = confidence_threshold
        self._schema    = schema or {}
        self._resolver  = SchemaResolver(self._schema) if self._schema else None
        self._builder   = SQLBuilder(self._cond_p, self._resolver)

    def update_schema(self, schema: dict):
        """Hot-reload schema after a USE <db> change."""
        self._schema   = schema
        self._resolver = SchemaResolver(schema)
        self._builder  = SQLBuilder(self._cond_p, self._resolver)

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

        # Normalise NL input
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

    # ── Helpers ──

    def _normalise(self, text: str) -> str:
        """Lowercase, collapse whitespace, remove trailing punctuation."""
        text = text.lower().strip().rstrip("?.")
        text = re.sub(r"\s+", " ", text)
        # Expand contractions
        text = re.sub(r"\bdon't\b", "do not", text)
        text = re.sub(r"\bcan't\b", "cannot", text)
        text = re.sub(r"\bisn't\b", "is not", text)
        text = re.sub(r"\bwon't\b", "will not", text)
        return text