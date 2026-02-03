from __future__ import annotations

from typing import Any, Dict

from jsonschema import Draft7Validator

from .errors import MCPError, ErrorCode

TABLES_LIST_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {"type": "string", "minLength": 1},
        "extensions": {
            "type": "array",
            "items": {"type": "string", "enum": [".csv", ".jmp"]},
            "default": [".csv", ".jmp"],
        },
    },
    "required": ["path"],
    "additionalProperties": False,
}

TABLE_SCHEMA_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "tableId": {"type": "string", "minLength": 1},
        "maxColumns": {"type": "integer", "minimum": 1, "maximum": 2000, "default": 2000},
    },
    "required": ["tableId"],
    "additionalProperties": False,
}

TABLE_PREVIEW_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "tableId": {"type": "string", "minLength": 1},
        "rows": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200},
        "method": {"type": "string", "enum": ["head", "random"], "default": "random"},
        "seed": {
            "type": "integer",
            "minimum": 0,
            "maximum": 2147483647,
            "default": 42,
        },
    },
    "required": ["tableId"],
    "additionalProperties": False,
}


def apply_defaults(schema: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(data)
    for key, props in schema.get("properties", {}).items():
        if key not in result and "default" in props:
            result[key] = props["default"]
    return result


def validate_payload(schema: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
    if errors:
        err = errors[0]
        location = "/".join([str(part) for part in err.path])
        raise MCPError(
            ErrorCode.INVALID_ARGUMENT,
            "Input validation failed",
            {"path": location, "message": err.message},
        )
    return apply_defaults(schema, data)
