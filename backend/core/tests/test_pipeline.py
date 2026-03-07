from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import OperationConfigError
from core.services.pipeline.operation_registry import (
    OperationResult,
    get_operation,
    list_operations,
)


class TestOperationRegistry:
    def test_list_operations(self):
        ops = list_operations()
        assert "fill_missing" in ops
        assert "drop_duplicates" in ops
        assert "cast_type" in ops
        assert "filter_rows" in ops
        assert "aggregate" in ops
        assert len(ops) >= 20

    def test_get_known_operation(self):
        fn = get_operation("fill_missing")
        assert callable(fn)

    def test_get_unknown_operation(self):
        with pytest.raises(OperationConfigError, match="Unknown operation"):
            get_operation("does_not_exist")


class TestOperationsViaRegistry:
    def test_fill_missing(self):
        df = pd.DataFrame({"a": [1, None, 3]})
        fn = get_operation("fill_missing")
        result: OperationResult = fn(df, {"strategy": "constant", "fill_value": 0})
        assert result.rows_after == 3
        assert result.df["a"].isna().sum() == 0

    def test_drop_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 2]})
        fn = get_operation("drop_duplicates")
        result = fn(df, {"keep": "first"})
        assert result.rows_after == 2

    def test_cast_type(self):
        df = pd.DataFrame({"d": ["2026-01-01", "2026-02-01"]})
        fn = get_operation("cast_type")
        result = fn(df, {"column": "d", "target_type": "date"})
        assert pd.api.types.is_datetime64_any_dtype(result.df["d"])

    def test_filter_rows(self):
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        fn = get_operation("filter_rows")
        result = fn(df, {"conditions": [{"column": "x", "operator": "gt", "value": 3}]})
        assert result.rows_after == 2

    def test_add_arithmetic_column(self):
        df = pd.DataFrame({"a": [10, 20], "b": [2, 3]})
        fn = get_operation("add_arithmetic_column")
        result = fn(df, {"new_name": "c", "expression": "a * b"})
        assert "c" in result.columns_added
        assert result.df["c"].tolist() == [20, 60]

    def test_aggregate(self):
        df = pd.DataFrame({"g": ["a", "a", "b"], "v": [10, 20, 30]})
        fn = get_operation("aggregate")
        result = fn(df, {"group_by": ["g"], "agg_config": {"v": ["sum"]}})
        assert result.rows_after == 2

    def test_rename_columns(self):
        df = pd.DataFrame({"old": [1]})
        fn = get_operation("rename_columns")
        result = fn(df, {"mapping": {"old": "new"}})
        assert "new" in result.df.columns

    def test_select_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        fn = get_operation("select_columns")
        result = fn(df, {"columns": ["a", "c"]})
        assert list(result.df.columns) == ["a", "c"]

    def test_sort_rows(self):
        df = pd.DataFrame({"x": [3, 1, 2]})
        fn = get_operation("sort_rows")
        result = fn(df, {"by": ["x"]})
        assert result.df["x"].tolist() == [1, 2, 3]

    def test_multi_step_pipeline(self):
        df = pd.DataFrame({
            "a": [1, None, 2, 2, 3],
            "b": ["x", "y", "z", "z", "w"],
        })

        fn1 = get_operation("fill_missing")
        r1 = fn1(df, {"columns": ["a"], "strategy": "constant", "fill_value": 0})

        fn2 = get_operation("drop_duplicates")
        r2 = fn2(r1.df, {"keep": "first"})

        assert r2.rows_after == 4
        assert r2.df["a"].isna().sum() == 0
