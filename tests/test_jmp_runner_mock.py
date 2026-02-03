import json
import subprocess

from jmp_readonly_mcp import runner


def test_run_jmp_schema_mock(tmp_path, monkeypatch):
    monkeypatch.setenv("JMP_EXE_PATH", "jmp.exe")
    monkeypatch.setenv("TEMP_ROOT", str(tmp_path))

    def fake_execute(exe_path, job_path, timeout_sec, stdout_path, stderr_path):
        output_path = job_path.parent / "output.json"
        output = {
            "rows": 1,
            "cols": 1,
            "columns": [
                {"name": "a", "type": "numeric", "missingRate": 0.0, "nUnique": 1}
            ],
            "limits": {"nUniqueMayBeNull": False},
        }
        output_path.write_text(json.dumps(output), encoding="utf-8")
        return subprocess.CompletedProcess([exe_path, str(job_path)], 0)

    monkeypatch.setattr(runner, "_execute_jmp", fake_execute)

    result = runner.run_jmp("schema", "C:/data/demo.jmp", {"maxColumns": 2000})
    assert result["rows"] == 1
