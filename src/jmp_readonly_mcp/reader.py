from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from pandas.api import types as pd_types

from .errors import MCPError, ErrorCode
from .runner import run_jmp
from .security import data_roots, ensure_allowed_path

NUNIQUE_ROW_THRESHOLD = 200_000
NUNIQUE_COL_THRESHOLD = 2_000


def parse_table_id(table_id: str) -> str:
    if not table_id.startswith("file:"):
        raise MCPError(ErrorCode.INVALID_ARGUMENT, "tableId must start with file:")
    path = table_id[len("file:") :]
    if not path:
        raise MCPError(ErrorCode.INVALID_ARGUMENT, "tableId path is empty")
    return path


def _normalize_path(path: str) -> str:
    roots = data_roots()
    return ensure_allowed_path(path, roots)


def _map_dtype(series: pd.Series) -> str:
    if pd_types.is_bool_dtype(series):
        return "boolean"
    if pd_types.is_numeric_dtype(series):
        return "numeric"
    if pd_types.is_datetime64_any_dtype(series):
        return "date"
    if pd_types.is_string_dtype(series) or pd_types.is_object_dtype(series):
        return "character"
    return "unknown"


def _csv_schema(file_path: str, max_columns: int) -> Dict[str, Any]:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:  # pragma: no cover - depends on pandas internals
        raise MCPError(
            ErrorCode.READ_FAILED,
            "Failed to read CSV for schema",
            {"path": file_path, "hint": str(exc)},
        ) from exc
    rows = int(df.shape[0])
    cols = int(df.shape[1])

    compute_nunique = not (rows > NUNIQUE_ROW_THRESHOLD or cols > NUNIQUE_COL_THRESHOLD)

    columns: List[Dict[str, Any]] = []
    for name in df.columns[:max_columns]:
        series = df[name]
        missing = int(series.isna().sum())
        missing_rate = missing / rows if rows else 0.0
        n_unique = int(series.nunique(dropna=True)) if compute_nunique else None
        columns.append(
            {
                "name": str(name),
                "type": _map_dtype(series),
                "missingRate": float(missing_rate),
                "nUnique": n_unique,
            }
        )

    return {
        "rows": rows,
        "cols": cols,
        "columns": columns,
        "limits": {"nUniqueMayBeNull": not compute_nunique},
    }


def _csv_preview(file_path: str, rows: int, method: str, seed: int) -> Dict[str, Any]:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:  # pragma: no cover - depends on pandas internals
        raise MCPError(
            ErrorCode.READ_FAILED,
            "Failed to read CSV for preview",
            {"path": file_path, "hint": str(exc)},
        ) from exc
    total_rows = int(df.shape[0])
    rows_take = min(rows, total_rows)

    if method == "head":
        preview_df = df.head(rows_take)
    else:
        if rows_take == 0:
            preview_df = df.head(0)
        else:
            preview_df = df.sample(n=rows_take, replace=False, random_state=seed)

    json_text = preview_df.to_json(orient="records", date_format="iso")
    data = json.loads(json_text)

    return {
        "rowsRequested": rows,
        "rowsReturned": len(data),
        "data": data,
        "truncated": rows > total_rows,
    }


def table_schema(table_id: str, max_columns: int) -> Dict[str, Any]:
    path = parse_table_id(table_id)
    file_path = _normalize_path(path)

    if not os.path.exists(file_path):
        raise MCPError(ErrorCode.NOT_FOUND, "File not found", {"path": file_path})

    ext = Path(file_path).suffix.lower()
    name = Path(file_path).stem

    if ext == ".csv":
        output = _csv_schema(file_path, max_columns)
    elif ext == ".jmp":
        output = run_jmp("schema", file_path, {"maxColumns": max_columns})
        if "columns" in output and isinstance(output["columns"], list):
            output["columns"] = output["columns"][:max_columns]
        output.setdefault("limits", {"nUniqueMayBeNull": True})
    else:
        raise MCPError(ErrorCode.INVALID_ARGUMENT, "Unsupported file extension")

    return {"tableId": f"file:{file_path}", "name": name, **output}


def table_preview(table_id: str, rows: int, method: str, seed: int) -> Dict[str, Any]:
    path = parse_table_id(table_id)
    file_path = _normalize_path(path)
    max_rows_env = max(int(os.environ.get("MAX_PREVIEW_ROWS", "1000")), 1)
    if rows > max_rows_env:
        rows = max_rows_env

    if not os.path.exists(file_path):
        raise MCPError(ErrorCode.NOT_FOUND, "File not found", {"path": file_path})

    ext = Path(file_path).suffix.lower()
    name = Path(file_path).stem

    if ext == ".csv":
        output = _csv_preview(file_path, rows, method, seed)
    elif ext == ".jmp":
        output = run_jmp("preview", file_path, {"rows": rows, "method": method, "seed": seed})
        if "truncated" in output:
            output["truncated"] = bool(output["truncated"])
    else:
        raise MCPError(ErrorCode.INVALID_ARGUMENT, "Unsupported file extension")

    return {
        "tableId": f"file:{file_path}",
        "name": name,
        "method": method,
        "seed": seed,
        **output,
    }


def tables_list(path: str, extensions: List[str]) -> Dict[str, Any]:
    dir_path = _normalize_path(path)
    if not os.path.isdir(dir_path):
        raise MCPError(ErrorCode.NOT_FOUND, "Path is not a directory", {"path": dir_path})

    normalized_exts = [ext.lower() for ext in extensions]

    tables: List[Dict[str, Any]] = []
    try:
        entries = os.listdir(dir_path)
    except OSError as exc:
        raise MCPError(
            ErrorCode.READ_FAILED,
            "Failed to list directory",
            {"path": dir_path, "hint": str(exc)},
        ) from exc

    for entry in entries:
        full_path = os.path.join(dir_path, entry)
        if not os.path.isfile(full_path):
            continue
        ext = Path(full_path).suffix.lower()
        if ext not in normalized_exts:
            continue
        tables.append(
            {
                "tableId": f"file:{os.path.abspath(full_path)}",
                "name": Path(full_path).stem,
                "format": ext.lstrip("."),
                "path": os.path.abspath(full_path),
                "sizeBytes": os.path.getsize(full_path),
            }
        )

    return {"tables": tables}
