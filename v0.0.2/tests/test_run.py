import sys
import os
import pytest

# Add TerSQL root to sys.path so we can import internal modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Core import TerSQLCore, SafetyGate
from NLP import NLPEngine
from plugins.base import BaseDB, QueryResult

# ─────────────────────────────────────────────────────────────
#  Mock Objects
# ─────────────────────────────────────────────────────────────

class MockDB(BaseDB):
    def connect(self, **kwargs):
        pass

    def disconnect(self):
        pass

    def reconnect(self):
        return True

    def meta(self):
        return {"version": "mock"}

    def get_tables(self):
        return ["users"]

    def execute(self, sql: str) -> QueryResult:
        # Mock execution returning an empty set for simple tracking
        return QueryResult(columns=["mock_col"], rows=[["mock_val"]])
        
    def get_schema(self) -> dict[str, list[str]]:
        return {"users": ["id", "name"]}
        
    def health(self) -> tuple[bool, float]:
        return True, 0.1

    def backup(self, directory: str) -> str:
        return "/mock/backup/path.sql"

# ─────────────────────────────────────────────────────────────
#  SafetyGate Tests
# ─────────────────────────────────────────────────────────────

class TestSafetyGate:
    def test_safe_mode_blocks_unbounded_deletes(self):
        gate = SafetyGate(safe_mode=True, read_only=False)
        allowed, needs_backup, reason = gate.check("DELETE FROM users;")
        assert allowed is False
        assert "without WHERE is blocked" in reason

    def test_safe_mode_allows_bounded_deletes(self):
        gate = SafetyGate(safe_mode=True, read_only=False)
        allowed, needs_backup, reason = gate.check("DELETE FROM users WHERE id=1;")
        assert allowed is True
        assert needs_backup is True

    def test_read_only_blocks_writes(self):
        gate = SafetyGate(safe_mode=False, read_only=True)
        # Check standard DML drops
        allowed, _, reason = gate.check("INSERT INTO users (name) VALUES ('Test');")
        assert allowed is False
        assert "Read-only mode" in reason

        allowed, _, reason = gate.check("DROP TABLE users;")
        assert allowed is False

    def test_read_only_allows_selects(self):
        gate = SafetyGate(safe_mode=False, read_only=True)
        allowed, needs_backup, reason = gate.check("SELECT * FROM users;")
        assert allowed is True
        assert needs_backup is False

# ─────────────────────────────────────────────────────────────
#  TerSQLCore Pipeline Tests
# ─────────────────────────────────────────────────────────────

class TestTerSQLCore:
    @pytest.fixture
    def core(self):
        db = MockDB()
        nlp = NLPEngine()
        return TerSQLCore(db=db, nlp=nlp)

    def test_history_logging(self, core):
        # Initial core setup
        result = core.run("SELECT * FROM users;")
        assert result is not None
        assert core._session_n == 1
        assert len(core._history) == 1
        
        record = core._history[0]
        assert record.sql == "SELECT * FROM users;"
        assert record.ok is True

    def test_explain_mode_prefix(self, core):
        core.explain_mode = True
        
        # When explain mode is active, the engine should prepend EXPLAIN to SELECTs
        # We can't trivially check the DB trace without mocking deeper, but we can verify it doesn't crash 
        # and correctly logs the base SQL to history.
        result = core.run("SELECT * FROM users;")
        assert result is not None
        assert len(core._history) == 1

        record = core._history[0]
        assert record.sql == "SELECT * FROM users;"

    def test_sync_schema_updates_nlp(self, core):
        # sync_schema should pull get_schema from MockDB and feed the NLP resolver
        core.sync_schema()
        assert "users" in core.nlp._schema
        assert "id" in core.nlp._schema["users"]

if __name__ == "__main__":
    pytest.main([__file__])
    