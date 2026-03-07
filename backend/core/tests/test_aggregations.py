from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import AggregationConfigError, ColumnNotFoundError
from core.services.transforms.aggregations import aggregate


@pytest.fixture()
def df():
    return pd.DataFrame({
        "city": ["NYC", "LA", "NYC", "LA", "NYC"],
        "revenue": [100, 200, 150, 250, 300],
        "count": [1, 2, 1, 3, 2],
    })


class TestAggregate:
    def test_single_func(self, df):
        result = aggregate(df, group_by=["city"], agg_config={"revenue": ["sum"]})
        assert len(result) == 2
        assert "revenue_sum" in result.columns

    def test_multiple_funcs(self, df):
        result = aggregate(
            df, group_by=["city"],
            agg_config={"revenue": ["sum", "avg", "min", "max"]},
        )
        assert "revenue_sum" in result.columns
        assert "revenue_mean" in result.columns
        assert "revenue_min" in result.columns
        assert "revenue_max" in result.columns

    def test_multiple_columns(self, df):
        result = aggregate(
            df, group_by=["city"],
            agg_config={"revenue": ["sum"], "count": ["sum"]},
        )
        assert "revenue_sum" in result.columns
        assert "count_sum" in result.columns

    def test_count_func(self, df):
        result = aggregate(df, group_by=["city"], agg_config={"revenue": ["count"]})
        nyc_row = result[result["city"] == "NYC"]
        assert nyc_row["revenue_count"].iloc[0] == 3

    def test_median_func(self, df):
        result = aggregate(df, group_by=["city"], agg_config={"revenue": ["median"]})
        assert "revenue_median" in result.columns


class TestErrors:
    def test_empty_group_by(self, df):
        with pytest.raises(AggregationConfigError, match="group_by must not be empty"):
            aggregate(df, group_by=[], agg_config={"revenue": ["sum"]})

    def test_empty_agg_config(self, df):
        with pytest.raises(AggregationConfigError, match="agg_config must not be empty"):
            aggregate(df, group_by=["city"], agg_config={})

    def test_column_not_found(self, df):
        with pytest.raises(ColumnNotFoundError):
            aggregate(df, group_by=["missing"], agg_config={"revenue": ["sum"]})

    def test_unknown_function(self, df):
        with pytest.raises(AggregationConfigError, match="Unknown aggregation function"):
            aggregate(df, group_by=["city"], agg_config={"revenue": ["unknown"]})
