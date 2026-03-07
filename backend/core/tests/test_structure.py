from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.transforms.structure import (
    rename_columns,
    reorder_columns,
    select_columns,
    sort_rows,
)


@pytest.fixture()
def df():
    return pd.DataFrame({
        "name": ["Carol", "Alice", "Bob"],
        "age": [35, 25, 30],
        "city": ["NYC", "LA", "Boston"],
    })


class TestRenameColumns:
    def test_rename(self, df):
        result = rename_columns(df, mapping={"name": "full_name", "age": "years"})
        assert "full_name" in result.columns
        assert "years" in result.columns
        assert "name" not in result.columns

    def test_empty_mapping(self, df):
        with pytest.raises(OperationConfigError, match="empty"):
            rename_columns(df, mapping={})

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            rename_columns(df, mapping={"missing": "new"})


class TestSelectColumns:
    def test_select(self, df):
        result = select_columns(df, columns=["name", "city"])
        assert list(result.columns) == ["name", "city"]
        assert len(result) == 3

    def test_empty_columns(self, df):
        with pytest.raises(OperationConfigError, match="empty"):
            select_columns(df, columns=[])

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            select_columns(df, columns=["missing"])


class TestSortRows:
    def test_ascending(self, df):
        result = sort_rows(df, by=["age"])
        assert result["age"].tolist() == [25, 30, 35]

    def test_descending(self, df):
        result = sort_rows(df, by=["age"], ascending=False)
        assert result["age"].tolist() == [35, 30, 25]

    def test_empty_by(self, df):
        with pytest.raises(OperationConfigError, match="empty"):
            sort_rows(df, by=[])


class TestReorderColumns:
    def test_reorder(self, df):
        result = reorder_columns(df, order=["city", "name"])
        assert list(result.columns) == ["city", "name", "age"]

    def test_empty_order(self, df):
        with pytest.raises(OperationConfigError, match="empty"):
            reorder_columns(df, order=[])

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            reorder_columns(df, order=["missing"])
