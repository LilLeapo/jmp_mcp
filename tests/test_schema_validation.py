import pytest

from jmp_readonly_mcp.schemas import (
    TABLE_PREVIEW_SCHEMA,
    TABLE_SCHEMA_SCHEMA,
    TABLES_LIST_SCHEMA,
    validate_payload,
)
from jmp_readonly_mcp.errors import MCPError, ErrorCode


def test_tables_list_defaults():
    payload = validate_payload(TABLES_LIST_SCHEMA, {"path": "/tmp"})
    assert payload["extensions"] == [".csv", ".jmp"]


def test_table_schema_defaults():
    payload = validate_payload(TABLE_SCHEMA_SCHEMA, {"tableId": "file:/tmp/a.csv"})
    assert payload["maxColumns"] == 2000


def test_table_preview_defaults():
    payload = validate_payload(TABLE_PREVIEW_SCHEMA, {"tableId": "file:/tmp/a.csv"})
    assert payload["rows"] == 200
    assert payload["method"] == "random"
    assert payload["seed"] == 42


def test_invalid_payload():
    with pytest.raises(MCPError) as exc:
        validate_payload(TABLES_LIST_SCHEMA, {"extensions": [".csv"]})
    assert exc.value.code == ErrorCode.INVALID_ARGUMENT
