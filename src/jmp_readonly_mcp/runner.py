from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from .errors import MCPError, ErrorCode, tail_text


def _escape_jsl_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "\\\"")


def _temp_root() -> Path:
    root = os.environ.get("TEMP_ROOT")
    if not root:
        root = os.path.join(tempfile.gettempdir(), "jmp_readonly_mcp")
    return Path(root)


def _render_jsl(
    template_text: str, input_path: Path, input_jsl_path: Path, output_path: Path
) -> str:
    return (
        template_text.replace("{{INPUT_PATH}}", _escape_jsl_string(str(input_path)))
        .replace("{{INPUT_JSL_PATH}}", _escape_jsl_string(str(input_jsl_path)))
        .replace("{{OUTPUT_PATH}}", _escape_jsl_string(str(output_path)))
    )


def _render_input_jsl(action: str, file_path: str, params: Dict[str, Any]) -> str:
    lines = [
        "Names Default To Here(1);",
        f'action = "{_escape_jsl_string(action)}";',
        f'filePath = "{_escape_jsl_string(file_path)}";',
        "params = Associative Array();",
    ]
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str):
            literal = f"\"{_escape_jsl_string(value)}\""
        elif isinstance(value, bool):
            literal = "1" if value else "0"
        else:
            literal = str(value)
        lines.append(f'params["{_escape_jsl_string(str(key))}"] = {literal};')
    return "\n".join(lines) + "\n"


def _execute_jmp(
    exe_path: str,
    job_path: Path,
    timeout_sec: int,
    stdout_path: Path,
    stderr_path: Path,
) -> subprocess.CompletedProcess:
    with stdout_path.open("w", encoding="utf-8") as stdout_fh, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_fh:
        return subprocess.run(
            [exe_path, str(job_path)],
            stdout=stdout_fh,
            stderr=stderr_fh,
            timeout=timeout_sec,
            check=False,
        )


def run_jmp(action: str, file_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    exe_path = os.environ.get("JMP_EXE_PATH")
    if not exe_path:
        raise MCPError(ErrorCode.JMP_EXEC_FAILED, "JMP_EXE_PATH is not configured")

    timeout_sec = int(os.environ.get("JMP_TIMEOUT_SEC", "60"))
    run_id = str(uuid.uuid4())
    run_dir = _temp_root() / run_id
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    input_path = run_dir / "input.json"
    input_jsl_path = run_dir / "input.jsl"
    output_path = run_dir / "output.json"
    job_path = run_dir / "job.jsl"
    stdout_path = logs_dir / "stdout.txt"
    stderr_path = logs_dir / "stderr.txt"

    payload = {"action": action, "filePath": file_path, "params": params}
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    input_jsl_path.write_text(_render_input_jsl(action, file_path, params), encoding="utf-8")

    template_path = Path(__file__).resolve().parents[2] / "templates" / "runner_readonly.jsl"
    template_text = template_path.read_text(encoding="utf-8")
    job_path.write_text(
        _render_jsl(template_text, input_path, input_jsl_path, output_path),
        encoding="utf-8",
    )

    try:
        result = _execute_jmp(exe_path, job_path, timeout_sec, stdout_path, stderr_path)
    except subprocess.TimeoutExpired as exc:
        stderr_tail = tail_text(stderr_path.read_text(encoding="utf-8", errors="ignore"))
        raise MCPError(
            ErrorCode.JMP_TIMEOUT,
            "JMP execution timed out",
            {"runId": run_id, "stderrTail": stderr_tail, "hint": str(exc)},
        ) from exc

    if result.returncode != 0:
        stderr_tail = tail_text(stderr_path.read_text(encoding="utf-8", errors="ignore"))
        raise MCPError(
            ErrorCode.JMP_EXEC_FAILED,
            "JMP execution failed",
            {"runId": run_id, "exitCode": result.returncode, "stderrTail": stderr_tail},
        )

    if not output_path.exists():
        raise MCPError(
            ErrorCode.JMP_EXEC_FAILED,
            "JMP did not produce output.json",
            {"runId": run_id, "exitCode": result.returncode},
        )

    try:
        output = json.loads(output_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MCPError(
            ErrorCode.JMP_EXEC_FAILED,
            "output.json is not valid JSON",
            {"runId": run_id, "hint": str(exc)},
        ) from exc

    if isinstance(output, dict) and "error" in output:
        err = output.get("error") or {}
        raise MCPError(
            err.get("code", ErrorCode.JMP_EXEC_FAILED),
            err.get("message", "JSL reported an error"),
            {"runId": run_id, **(err.get("details") or {})},
        )

    return output
