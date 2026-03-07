from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.transforms.derived_columns import (
    add_arithmetic_column,
    add_concat_column,
    add_conditional_column,
    add_ratio_column,
)


@pytest.fixture()
def df():
    return pd.DataFrame({
        "price": [100, 200, 300],
        "quantity": [2, 3, 0],
        "name": ["Alice", "Bob", "Carol"],
        "city": ["NYC", "LA", "NYC"],
    })


class TestArithmeticColumn:
    def test_simple_expression(self, df):
        result = add_arithmetic_column(df, new_name="total", expression="price * quantity")
        assert result["total"].tolist() == [200, 600, 0]

    def test_complex_expression(self, df):
        result = add_arithmetic_column(df, new_name="discounted", expression="price * 0.9")
        assert result["discounted"].iloc[0] == pytest.approx(90.0)

    def test_invalid_expression(self, df):
        with pytest.raises(OperationConfigError, match="disallowed characters"):
            add_arithmetic_column(df, new_name="bad", expression="__import__('os')")

    def test_expression_error(self, df):
        with pytest.raises(OperationConfigError, match="Cannot evaluate"):
            add_arithmetic_column(df, new_name="bad", expression="nonexistent_col + 1")


class TestRatioColumn:
    def test_ratio(self, df):
        result = add_ratio_column(df, new_name="ratio", numerator="price", denominator="quantity")
        assert result["ratio"].iloc[0] == pytest.approx(50.0)
        assert pd.isna(result["ratio"].iloc[2])  # division by zero -> NaN

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            add_ratio_column(df, new_name="r", numerator="missing", denominator="quantity")


class TestConditionalColumn:
    def test_basic_condition(self, df):
        result = add_conditional_column(
            df,
            new_name="category",
            conditions=[
                {"column": "price", "operator": "gte", "value": 200, "result": "expensive"},
                {"column": "price", "operator": "lt", "value": 200, "result": "cheap"},
            ],
            default="unknown",
        )
        assert result["category"].iloc[0] == "cheap"
        assert result["category"].iloc[1] == "expensive"

    def test_empty_conditions(self, df):
        with pytest.raises(OperationConfigError, match="At least one"):
            add_conditional_column(df, new_name="x", conditions=[], default="none")


class TestConcatColumn:
    def test_concat(self, df):
        result = add_concat_column(df, new_name="full", columns=["name", "city"], separator=" - ")
        assert result["full"].iloc[0] == "Alice - NYC"

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            add_concat_column(df, new_name="x", columns=["name", "missing"])
