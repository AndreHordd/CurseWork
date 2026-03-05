from __future__ import annotations

import io
import json

import pytest

from core.exceptions import FileParseError, FileValidationError
from core.services.file_readers import read_file


class TestReadCSV:
    def test_reads_valid_csv(self, valid_csv):
        result = read_file(valid_csv, "csv")
        assert len(result.df) == 3
        assert list(result.df.columns) == ["id", "name", "value", "active"]

    def test_custom_delimiter(self):
        data = b"id;name;value\n1;Alice;10\n2;Bob;20\n"
        buf = io.BytesIO(data)
        buf.name = "semi.csv"
        result = read_file(buf, "csv", {"delimiter": ";"})
        assert list(result.df.columns) == ["id", "name", "value"]
        assert len(result.df) == 2

    def test_fallback_encoding(self):
        data = "id,name\n1,café\n".encode("latin-1")
        buf = io.BytesIO(data)
        buf.name = "latin.csv"
        result = read_file(buf, "csv", {"encoding": "utf-8"})
        assert len(result.df) == 1
        assert any("Fallback" in w for w in result.warnings)

    def test_no_header(self):
        data = b"1,Alice,10\n2,Bob,20\n"
        buf = io.BytesIO(data)
        buf.name = "noheader.csv"
        result = read_file(buf, "csv", {"has_header": False})
        assert len(result.df) == 2
        assert len(result.df.columns) == 3


class TestReadXLSX:
    def test_reads_valid_xlsx(self, valid_xlsx):
        result = read_file(valid_xlsx, "xlsx")
        assert len(result.df) == 2
        assert "id" in result.df.columns


class TestReadJSON:
    def test_reads_valid_json(self, valid_json):
        result = read_file(valid_json, "json")
        assert len(result.df) == 2
        assert "id" in result.df.columns

    def test_rejects_non_tabular_json(self):
        buf = io.BytesIO(json.dumps("just a string").encode())
        buf.name = "bad.json"
        with pytest.raises(FileParseError):
            read_file(buf, "json")

    def test_rejects_array_of_non_objects(self):
        buf = io.BytesIO(json.dumps([1, 2, 3]).encode())
        buf.name = "ints.json"
        with pytest.raises(FileParseError):
            read_file(buf, "json")


class TestUnsupportedFormat:
    def test_rejects_unknown_format(self):
        buf = io.BytesIO(b"data")
        buf.name = "file.txt"
        with pytest.raises(FileValidationError):
            read_file(buf, "txt")


class TestColumnNormalization:
    def test_renames_duplicate_columns(self, csv_with_duplicate_columns):
        result = read_file(csv_with_duplicate_columns, "csv")
        cols = list(result.df.columns)
        assert len(cols) == len(set(cols))
        assert any("Renamed" in w for w in result.warnings)

    def test_drops_fully_empty_columns(self):
        data = b"a,b,c\n1,,3\n2,,4\n"
        buf = io.BytesIO(data)
        buf.name = "empty_col.csv"
        result = read_file(buf, "csv")
        assert "b" not in result.df.columns
        assert any("empty columns" in w for w in result.warnings)
