import os

from jmp_readonly_mcp.reader import table_preview, table_schema, tables_list


def test_csv_schema_and_preview(tmp_path, monkeypatch):
    csv_path = tmp_path / "demo.csv"
    csv_path.write_text("col1,col2\n1,a\n2,\n, c\n", encoding="utf-8")

    monkeypatch.setenv("DATA_ROOTS", str(tmp_path))

    listed = tables_list(str(tmp_path), [".csv"])
    assert listed["tables"][0]["path"] == str(csv_path)

    schema = table_schema(f"file:{csv_path}", 2000)
    assert schema["rows"] == 3
    assert schema["cols"] == 2
    assert schema["columns"][0]["name"] == "col1"

    preview = table_preview(f"file:{csv_path}", 5, "head", 42)
    assert preview["rowsReturned"] == 3
    assert preview["truncated"] is True
