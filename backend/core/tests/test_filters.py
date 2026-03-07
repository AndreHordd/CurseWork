from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, EmptyResultError, OperationConfigError
from core.services.transforms.filters import filter_rows


@pytest.fixture()
def df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Carol", "Dave"],
        "age": [25, 30, 35, 40],
        "city": ["NYC", "LA", "NYC", "Boston"],
        "date": ["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"],
    })


class TestOperators:
    def test_eq(self, df):
        result = filter_rows(df, conditions=[{"column": "city", "operator": "eq", "value": "NYC"}])
        assert len(result) == 2

    def test_neq(self, df):
        result = filter_rows(df, conditions=[{"column": "city", "operator": "neq", "value": "NYC"}])
        assert len(result) == 2

    def test_gt(self, df):
        result = filter_rows(df, conditions=[{"column": "age", "operator": "gt", "value": 30}])
        assert len(result) == 2

    def test_gte(self, df):
        result = filter_rows(df, conditions=[{"column": "age", "operator": "gte", "value": 30}])
        assert len(result) == 3

    def test_lt(self, df):
        result = filter_rows(df, conditions=[{"column": "age", "operator": "lt", "value": 35}])
        assert len(result) == 2

    def test_lte(self, df):
        result = filter_rows(df, conditions=[{"column": "age", "operator": "lte", "value": 35}])
        assert len(result) == 3

    def test_contains(self, df):
        result = filter_rows(df, conditions=[{"column": "name", "operator": "contains", "value": "o"}])
        assert len(result) == 2

    def test_not_contains(self, df):
        result = filter_rows(df, conditions=[{"column": "name", "operator": "not_contains", "value": "o"}])
        assert len(result) == 2

    def test_in_list(self, df):
        result = filter_rows(df, conditions=[{"column": "city", "operator": "in_list", "value": ["NYC", "LA"]}])
        assert len(result) == 3

    def test_not_in_list(self, df):
        result = filter_rows(df, conditions=[{"column": "city", "operator": "not_in_list", "value": ["NYC"]}])
        assert len(result) == 2

    def test_is_null(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        result = filter_rows(df, conditions=[{"column": "x", "operator": "is_null", "value": None}])
        assert len(result) == 1

    def test_is_not_null(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        result = filter_rows(df, conditions=[{"column": "x", "operator": "is_not_null", "value": None}])
        assert len(result) == 2

    def test_date_range(self, df):
        result = filter_rows(df, conditions=[{
            "column": "date",
            "operator": "date_range",
            "value": {"start": "2026-01-15", "end": "2026-03-15"},
        }])
        assert len(result) == 2


class TestLogic:
    def test_and_logic(self, df):
        result = filter_rows(df, conditions=[
            {"column": "city", "operator": "eq", "value": "NYC"},
            {"column": "age", "operator": "gt", "value": 30},
        ], logic="and")
        assert len(result) == 1

    def test_or_logic(self, df):
        result = filter_rows(df, conditions=[
            {"column": "city", "operator": "eq", "value": "Boston"},
            {"column": "name", "operator": "eq", "value": "Alice"},
        ], logic="or")
        assert len(result) == 2


class TestErrors:
    def test_empty_conditions(self, df):
        with pytest.raises(OperationConfigError, match="At least one"):
            filter_rows(df, conditions=[])

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            filter_rows(df, conditions=[{"column": "missing", "operator": "eq", "value": 1}])

    def test_empty_result(self, df):
        with pytest.raises(EmptyResultError):
            filter_rows(df, conditions=[{"column": "age", "operator": "gt", "value": 1000}])

    def test_unknown_operator(self, df):
        with pytest.raises(OperationConfigError, match="Unknown operator"):
            filter_rows(df, conditions=[{"column": "age", "operator": "like", "value": 1}])
