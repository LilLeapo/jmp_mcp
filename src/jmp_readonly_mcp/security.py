from __future__ import annotations

import os
from typing import Iterable, List

from .errors import MCPError, ErrorCode


def _split_roots(value: str) -> List[str]:
    if not value:
        return []
    parts: List[str] = []
    for token in value.replace(";", ",").split(","):
        token = token.strip()
        if token:
            parts.append(token)
    return parts


def data_roots() -> List[str]:
    roots = _split_roots(os.environ.get("DATA_ROOTS", ""))
    normalized: List[str] = []
    for root in roots:
        normalized.append(os.path.realpath(os.path.abspath(root)))
    return normalized


def ensure_allowed_path(path: str, roots: Iterable[str]) -> str:
    real_path = os.path.realpath(os.path.abspath(path))
    real_path_cmp = os.path.normcase(real_path)
    if not roots:
        raise MCPError(
            ErrorCode.SECURITY_VIOLATION,
            "DATA_ROOTS is not configured",
            {"path": real_path},
        )
    for root in roots:
        root_real = os.path.realpath(os.path.abspath(root))
        root_cmp = os.path.normcase(root_real)
        if real_path_cmp == root_cmp or real_path_cmp.startswith(root_cmp + os.sep):
            return real_path
    raise MCPError(
        ErrorCode.SECURITY_VIOLATION,
        "Path is outside allowed DATA_ROOTS",
        {"path": real_path},
    )
