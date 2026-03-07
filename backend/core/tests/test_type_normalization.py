from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError, TypeConversionError
from core.services.cleaning.type_normalization import (
    cast_type,
    convert_empty_to_null,
    normalize_decimal_separators,
    trim_spaces,
)


class TestCastType:
    def test_cast_to_int(self):
        df = pd.DataFrame({"x": ["1", "2", "3"]})
        result = cast_type(df, column="x", target_type="int")
        assert result["x"].dtype.name == "Int64"

    def test_cast_to_float(self):
        df = pd.DataFrame({"x": ["1.5", "2.5"]})
        result = cast_type(df, column="x", target_type="float")
        assert result["x"].dtype.name == "Float64"

    def test_cast_to_date(self):
        df = pd.DataFrame({"d": ["2026-01-01", "2026-02-01"]})
        result = cast_type(df, column="d", target_type="date")
        assert pd.api.types.is_datetime64_any_dtype(result["d"])

    def test_cast_to_bool(self):
        df = pd.DataFrame({"b": ["true", "false", "yes", "no"]})
        result = cast_type(df, column="b", target_type="bool")
        assert result["b"].iloc[0] == True  # noqa: E712
        assert result["b"].iloc[1] == False  # noqa: E712

    def test_cast_to_string(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = cast_type(df, column="x", target_type="string")
        assert pd.api.types.is_string_dtype(result["x"])

    def test_invalid_type(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(OperationConfigError, match="Unsupported target type"):
            cast_type(df, column="x", target_type="complex")

    def test_column_not_found(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ColumnNotFoundError):
            cast_type(df, column="z", target_type="int")

    def test_failed_conversion(self):
        df = pd.DataFrame({"x": ["abc", "def"]})
        with pytest.raises(TypeConversionError):
            cast_type(df, column="x", target_type="int")


class TestTrimSpaces:
    def test_trims_spaces(self):
        df = pd.DataFrame({"name": ["  Alice  ", "Bob  ", "  Carol"]})
        result = trim_spaces(df)
        assert result["name"].tolist() == ["Alice", "Bob", "Carol"]

    def test_skips_numeric(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = trim_spaces(df)
        assert result["x"].tolist() == [1, 2, 3]


class TestNormalizeDecimalSeparators:
    def test_comma_to_dot(self):
        df = pd.DataFrame({"price": ["1,50", "2,99"]})
        result = normalize_decimal_separators(df, columns=["price"])
        assert result["price"].tolist() == ["1.50", "2.99"]


class TestConvertEmptyToNull:
    def test_converts_markers(self):
        df = pd.DataFrame({"x": ["hello", "N/A", "", "world", "UNKNOWN"]})
        result = convert_empty_to_null(df)
        assert result["x"].isna().sum() == 3

    def test_custom_values(self):
        df = pd.DataFrame({"x": ["hello", "MISSING", "world"]})
        result = convert_empty_to_null(df, values=["MISSING"])
        assert result["x"].isna().sum() == 1
