from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


class ErrorCode:
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    NOT_FOUND = "NOT_FOUND"
    SECURITY_VIOLATION = "SECURITY_VIOLATION"
    JMP_EXEC_FAILED = "JMP_EXEC_FAILED"
    JMP_TIMEOUT = "JMP_TIMEOUT"
    READ_FAILED = "READ_FAILED"
    INTERNAL = "INTERNAL"


@dataclass
class MCPError(Exception):
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


def error_payload(error: MCPError) -> Dict[str, Any]:
    details: Dict[str, Any] = error.details or {}
    return {
        "error": {
            "code": error.code,
            "message": error.message,
            "details": details,
        }
    }


def tail_text(text: str, max_bytes: int = 8192) -> str:
    if not text:
        return ""
    encoded = text.encode("utf-8", errors="ignore")
    if len(encoded) <= max_bytes:
        return text
    tail = encoded[-max_bytes:]
    return tail.decode("utf-8", errors="ignore")
