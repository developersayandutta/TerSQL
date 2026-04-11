import sys
import os
import pytest
import re

# Add TerSQL root to sys.path so we can import internal modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from NLP import SafetyChecker
from Core import SQL_FIXES

# ─────────────────────────────────────────────────────────────
#  Tests for NLP.py SafetyChecker
# ─────────────────────────────────────────────────────────────

class TestSafetyChecker:
    @pytest.fixture
    def checker(self):
        return SafetyChecker()

    def test_dangerous_keywords(self, checker):
        dangerous_queries = [
            "DROP TABLE users;",
            "TRUNCATE TABLE logs;",
            "drop database test;",
        ]
        for q in dangerous_queries:
            is_dangerous, reason = checker.check(q)
            assert is_dangerous is True
            assert "dangerous keyword" in reason

    def test_missing_where_clause(self, checker):
        is_dangerous, reason = checker.check("UPDATE users SET name = 'admin';")
        assert is_dangerous is True
        assert "UPDATE statement without WHERE clause" in reason

    def test_delete_is_always_dangerous(self, checker):
        # DELETE FROM is mapped explicitly in DANGEROUS_KEYWORDS to trigger terminal confirmation
        is_dangerous, reason = checker.check("DELETE FROM users WHERE id = 1;")
        assert is_dangerous is True
        assert r"\bDELETE\s+FROM\b" in reason
        
        is_dangerous, _ = checker.check("DELETE FROM users;")
        assert is_dangerous is True

    def test_safe_queries(self, checker):
        safe_queries = [
            "SELECT * FROM users;",
            "UPDATE users SET name = 'admin' WHERE id = 1;",
            "INSERT INTO users (name) VALUES ('test');",
            "EXPLAIN SELECT * FROM orders;"
        ]
        for q in safe_queries:
            is_dangerous, reason = checker.check(q)
            assert is_dangerous is False
            assert reason == ""

# ─────────────────────────────────────────────────────────────
#  Tests for Core.py SQL_FIXES Auto-Corrections
# ─────────────────────────────────────────────────────────────

def auto_fix(sql: str) -> str:
    """Helper mirroring Core._safe_execute auto-fix runner."""
    for pattern, fix in SQL_FIXES.items():
        if re.match(pattern, sql, re.IGNORECASE):
            return re.sub(pattern, fix, sql, flags=re.IGNORECASE)
    return sql

class TestAutoCorrections:
    def test_show_database(self):
        assert auto_fix("show database;") == "SHOW DATABASES;"
        assert auto_fix("show dbs") == "SHOW DATABASES;"
        assert auto_fix("show database") == "SHOW DATABASES;"

    def test_show_table(self):
        assert auto_fix("show table;") == "SHOW TABLES;"
        assert auto_fix("show table") == "SHOW TABLES;"

    def test_describe(self):
        assert auto_fix("desc users;") == "DESCRIBE users;"
        assert auto_fix("desc my_tables") == "DESCRIBE my_tables;"

    def test_select_all(self):
        assert auto_fix("select * users;") == "SELECT * FROM users;"
        assert auto_fix("select from logs") == "SELECT * FROM logs;"
        # With trailing semicolon
        assert auto_fix("select * tokens;") == "SELECT * FROM tokens;"
        assert auto_fix("select from env;") == "SELECT * FROM env;"

    def test_use_db(self):
        assert auto_fix("use production;") == "USE production;"
        assert auto_fix("use admin") == "USE admin;"

    def test_no_correction(self):
        # Native queries that shouldn't match autocomplete syntax rules
        assert auto_fix("SELECT id FROM users;") == "SELECT id FROM users;"
        assert auto_fix("SHOW GRANTS;") == "SHOW GRANTS;"
        assert auto_fix("DESCRIBE logs;") == "DESCRIBE logs;"

if __name__ == "__main__":
    pytest.main([__file__])