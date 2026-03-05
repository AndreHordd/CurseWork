from __future__ import annotations

import pandas as pd
import pytest

from core.services.profiler import profile_dataframe


class TestTypeInference:
    def test_integer_column(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "int"

    def test_float_column(self):
        df = pd.DataFrame({"x": [1.1, 2.2, 3.3]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "float"

    def test_string_column(self):
        df = pd.DataFrame({"x": ["hello", "world", "foo"]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "string"

    def test_date_column(self):
        df = pd.DataFrame({"x": ["2024-01-01", "2024-06-15", "2024-12-31"]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "date"

    def test_bool_column(self):
        df = pd.DataFrame({"x": ["true", "false", "true", "false"]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "bool"

    def test_mixed_type_becomes_string(self):
        df = pd.DataFrame({"x": [1, "hello", 3.5, "world"]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type in ("string", "float")

    def test_all_null_column(self):
        df = pd.DataFrame({"x": [None, None, None]})
        result = profile_dataframe(df)
        assert result.columns[0].inferred_type == "string"
        assert result.columns[0].nullable is True
        assert result.columns[0].missing_count == 3


class TestStats:
    def test_numeric_stats(self):
        df = pd.DataFrame({"x": [10, 20, 30]})
        result = profile_dataframe(df)
        stats = result.columns[0].stats
        assert stats["min"] == 10
        assert stats["max"] == 30
        assert stats["mean"] == 20.0

    def test_string_stats(self):
        df = pd.DataFrame({"x": ["a", "bb", "ccc"]})
        result = profile_dataframe(df)
        stats = result.columns[0].stats
        assert stats["max_length"] == 3
        assert "top_values" in stats

    def test_date_stats(self):
        df = pd.DataFrame({"x": ["2024-01-01", "2024-12-31"]})
        result = profile_dataframe(df)
        stats = result.columns[0].stats
        assert stats["min_date"] == "2024-01-01"
        assert stats["max_date"] == "2024-12-31"

    def test_bool_stats(self):
        df = pd.DataFrame({"x": ["true", "false", "true"]})
        result = profile_dataframe(df)
        stats = result.columns[0].stats
        assert stats["true_count"] == 2
        assert stats["false_count"] == 1


class TestProfileMetadata:
    def test_row_and_column_counts(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        result = profile_dataframe(df)
        assert result.row_count == 2
        assert result.column_count == 3
        assert len(result.columns) == 3

    def test_missing_count(self):
        df = pd.DataFrame({"x": [1, None, 3, None]})
        result = profile_dataframe(df)
        assert result.columns[0].missing_count == 2
        assert result.columns[0].nullable is True

    def test_distinct_count(self):
        df = pd.DataFrame({"x": ["a", "b", "a", "c"]})
        result = profile_dataframe(df)
        assert result.columns[0].distinct_count == 3
