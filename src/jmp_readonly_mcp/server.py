from __future__ import annotations

import json
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from .errors import MCPError, ErrorCode, error_payload
from .reader import (
    table_preview as read_table_preview,
    table_schema as read_table_schema,
    tables_list as read_tables_list,
)
from .schemas import (
    TABLE_PREVIEW_SCHEMA,
    TABLE_SCHEMA_SCHEMA,
    TABLES_LIST_SCHEMA,
    validate_payload,
)

mcp = FastMCP("jmp-readonly-mcp")


def _json_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False),
            }
        ]
    }


def _error_response(error: MCPError) -> Dict[str, Any]:
    return {
        "isError": True,
        "content": [
            {
                "type": "text",
                "text": json.dumps(error_payload(error), ensure_ascii=False),
            }
        ],
    }


@mcp.tool()
def tables_list(path: str, extensions: list[str] | None = None) -> Dict[str, Any]:
    try:
        payload: Dict[str, Any] = {"path": path}
        if extensions is not None:
            payload["extensions"] = extensions
        payload = validate_payload(TABLES_LIST_SCHEMA, payload)
        result = read_tables_list(payload["path"], payload["extensions"])
        return _json_response(result)
    except MCPError as err:
        return _error_response(err)
    except Exception as err:  # pragma: no cover
        return _error_response(
            MCPError(ErrorCode.INTERNAL, "Unexpected server error", {"hint": str(err)})
        )


@mcp.tool()
def table_schema(tableId: str, maxColumns: int | None = None) -> Dict[str, Any]:
    try:
        payload: Dict[str, Any] = {"tableId": tableId}
        if maxColumns is not None:
            payload["maxColumns"] = maxColumns
        payload = validate_payload(TABLE_SCHEMA_SCHEMA, payload)
        result = read_table_schema(payload["tableId"], payload["maxColumns"])
        return _json_response(result)
    except MCPError as err:
        return _error_response(err)
    except Exception as err:  # pragma: no cover
        return _error_response(
            MCPError(ErrorCode.INTERNAL, "Unexpected server error", {"hint": str(err)})
        )


@mcp.tool()
def table_preview(
    tableId: str,
    rows: int | None = None,
    method: str | None = None,
    seed: int | None = None,
) -> Dict[str, Any]:
    try:
        payload: Dict[str, Any] = {"tableId": tableId}
        if rows is not None:
            payload["rows"] = rows
        if method is not None:
            payload["method"] = method
        if seed is not None:
            payload["seed"] = seed
        payload = validate_payload(TABLE_PREVIEW_SCHEMA, payload)
        result = read_table_preview(
            payload["tableId"],
            payload["rows"],
            payload["method"],
            payload["seed"],
        )
        return _json_response(result)
    except MCPError as err:
        return _error_response(err)
    except Exception as err:  # pragma: no cover
        return _error_response(
            MCPError(ErrorCode.INTERNAL, "Unexpected server error", {"hint": str(err)})
        )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
