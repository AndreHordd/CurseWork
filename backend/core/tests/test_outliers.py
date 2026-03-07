from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.cleaning.outliers import detect_outliers


@pytest.fixture()
def df_with_outlier():
    return pd.DataFrame({
        "value": [10, 12, 11, 13, 12, 100, 11, 10, 12, 13],
    })


class TestIQRMethod:
    def test_mark(self, df_with_outlier):
        result = detect_outliers(df_with_outlier, column="value", method="iqr", action="mark")
        assert "value_is_outlier" in result.columns
        assert result["value_is_outlier"].sum() >= 1

    def test_remove(self, df_with_outlier):
        result = detect_outliers(df_with_outlier, column="value", method="iqr", action="remove")
        assert len(result) < len(df_with_outlier)
        assert 100 not in result["value"].tolist()

    def test_cap(self, df_with_outlier):
        result = detect_outliers(df_with_outlier, column="value", method="iqr", action="cap")
        assert len(result) == len(df_with_outlier)
        assert result["value"].max() < 100


class TestZScoreMethod:
    def test_mark(self, df_with_outlier):
        result = detect_outliers(
            df_with_outlier, column="value", method="zscore", action="mark", threshold=2.0,
        )
        assert "value_is_outlier" in result.columns

    def test_remove(self, df_with_outlier):
        result = detect_outliers(
            df_with_outlier, column="value", method="zscore", action="remove", threshold=2.0,
        )
        assert len(result) <= len(df_with_outlier)


class TestErrors:
    def test_column_not_found(self, df_with_outlier):
        with pytest.raises(ColumnNotFoundError):
            detect_outliers(df_with_outlier, column="missing", method="iqr", action="mark")

    def test_invalid_method(self, df_with_outlier):
        with pytest.raises(OperationConfigError, match="Unknown method"):
            detect_outliers(df_with_outlier, column="value", method="invalid", action="mark")

    def test_invalid_action(self, df_with_outlier):
        with pytest.raises(OperationConfigError, match="Unknown action"):
            detect_outliers(df_with_outlier, column="value", method="iqr", action="invalid")
