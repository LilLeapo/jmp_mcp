# JMP Read-only MCP Server

This project provides a local, read-only MCP server that can read **JMP (.jmp)** and **CSV** tables. The server exposes three tools: `tables_list`, `table_schema`, and `table_preview`.

## Requirements

- Python 3.10+
- JMP 18 installed (for `.jmp` support)

## Configuration

Environment variables:

- `JMP_EXE_PATH`: Absolute path to `jmp.exe` (required for `.jmp`).
- `TEMP_ROOT`: Optional temp directory for run artifacts (default: system temp + `jmp_readonly_mcp`).
- `JMP_TIMEOUT_SEC`: Timeout for `jmp.exe` runs (default: `60`).
- `DATA_ROOTS`: Allowed data roots (comma or semicolon separated). Required.
- `MAX_PREVIEW_ROWS`: Optional additional cap for preview (schema already enforces max 1000).

## Install

```bash
pip install -e .
```

### Using uv

```bash
uv venv
uv pip install -e .
```

## Run

```bash
jmp-readonly-mcp
```

## Tool Overview

- `tables_list(path, extensions)`
- `table_schema(tableId, maxColumns=2000)`
- `table_preview(tableId, rows=200, method=head|random, seed=42)`

All tool responses return a JSON string in `content[0].text`. Errors use a unified error envelope.

## Tests

```bash
pytest
```

## Notes

- `.jmp` files are read by generating a temporary JSL script and invoking `jmp.exe`.
- `.csv` files are read directly via pandas.
