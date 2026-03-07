from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.cleaning.missing_values import fill_missing


@pytest.fixture()
def df_with_nulls():
    return pd.DataFrame({
        "a": [1, None, 3, None, 5],
        "b": ["x", None, "z", None, "w"],
        "c": [10.0, 20.0, None, 40.0, 50.0],
    })


class TestDropStrategy:
    def test_drops_rows_with_any_null(self, df_with_nulls):
        result = fill_missing(df_with_nulls, strategy="drop")
        assert len(result) == 2
        assert result["a"].isna().sum() == 0

    def test_drops_only_specified_columns(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=["c"], strategy="drop")
        assert len(result) == 4


class TestConstantStrategy:
    def test_fill_with_constant(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=["a"], strategy="constant", fill_value=0)
        assert result["a"].isna().sum() == 0
        assert result["a"].tolist() == [1, 0, 3, 0, 5]

    def test_fill_string_with_constant(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=["b"], strategy="constant", fill_value="MISSING")
        assert "MISSING" in result["b"].tolist()


class TestMeanStrategy:
    def test_fill_with_mean(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=["a"], strategy="mean")
        assert result["a"].isna().sum() == 0
        assert result["a"].iloc[1] == 3.0  # mean of 1,3,5

    def test_errors_on_non_numeric(self, df_with_nulls):
        with pytest.raises(OperationConfigError, match="no numeric values"):
            fill_missing(
                pd.DataFrame({"x": [None, None]}),
                columns=["x"],
                strategy="mean",
            )


class TestMedianStrategy:
    def test_fill_with_median(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=["c"], strategy="median")
        assert result["c"].isna().sum() == 0


class TestModeStrategy:
    def test_fill_with_mode(self):
        df = pd.DataFrame({"x": ["a", "a", "b", None, "a"]})
        result = fill_missing(df, columns=["x"], strategy="mode")
        assert result["x"].iloc[3] == "a"


class TestKeepStrategy:
    def test_keep_does_nothing(self, df_with_nulls):
        result = fill_missing(df_with_nulls, strategy="keep")
        assert result["a"].isna().sum() == 2


class TestErrors:
    def test_unknown_strategy(self, df_with_nulls):
        with pytest.raises(OperationConfigError, match="Unknown strategy"):
            fill_missing(df_with_nulls, strategy="unknown")

    def test_column_not_found(self, df_with_nulls):
        with pytest.raises(ColumnNotFoundError):
            fill_missing(df_with_nulls, columns=["nonexistent"], strategy="drop")

    def test_all_columns_when_none(self, df_with_nulls):
        result = fill_missing(df_with_nulls, columns=None, strategy="drop")
        assert result.isna().sum().sum() == 0
