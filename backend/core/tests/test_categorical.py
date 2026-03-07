from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.cleaning.categorical import (
    normalize_case,
    replace_values,
    trim_and_collapse,
)


@pytest.fixture()
def df_text():
    return pd.DataFrame({
        "name": ["  Alice  ", "  BOB  ", "carol", None],
        "city": ["  New   York ", " los angeles ", "CHICAGO", "boston"],
    })


class TestNormalizeCase:
    def test_lower(self, df_text):
        result = normalize_case(df_text, columns=["city"], case_type="lower")
        assert result["city"].iloc[2] == "chicago"

    def test_upper(self, df_text):
        result = normalize_case(df_text, columns=["city"], case_type="upper")
        assert result["city"].iloc[3] == "BOSTON"

    def test_title(self, df_text):
        result = normalize_case(df_text, columns=["city"], case_type="title")
        assert result["city"].iloc[1] == " Los Angeles "

    def test_preserves_null(self, df_text):
        result = normalize_case(df_text, columns=["name"], case_type="lower")
        assert pd.isna(result["name"].iloc[3])

    def test_invalid_case_type(self, df_text):
        with pytest.raises(OperationConfigError, match="Unknown case_type"):
            normalize_case(df_text, columns=["name"], case_type="camel")

    def test_column_not_found(self, df_text):
        with pytest.raises(ColumnNotFoundError):
            normalize_case(df_text, columns=["missing"], case_type="lower")


class TestTrimAndCollapse:
    def test_trims_and_collapses(self, df_text):
        result = trim_and_collapse(df_text, columns=["city"])
        assert result["city"].iloc[0] == "New York"
        assert result["city"].iloc[1] == "los angeles"


class TestReplaceValues:
    def test_replaces(self):
        df = pd.DataFrame({"status": ["active", "inactive", "active", "pending"]})
        result = replace_values(df, column="status", mapping={"inactive": "disabled"})
        assert "disabled" in result["status"].tolist()
        assert "inactive" not in result["status"].tolist()

    def test_empty_mapping(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(OperationConfigError, match="empty"):
            replace_values(df, column="x", mapping={})

    def test_column_not_found(self):
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ColumnNotFoundError):
            replace_values(df, column="z", mapping={"a": "b"})
