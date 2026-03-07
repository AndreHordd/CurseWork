from __future__ import annotations

import pandas as pd
import pytest

from core.services.pipeline.change_report import ChangeReportBuilder
from core.services.pipeline.operation_registry import OperationResult
from core.services.profiler import ColumnProfile


@pytest.fixture()
def df_before():
    return pd.DataFrame({
        "a": [1, 2, None, 4, 2],
        "b": ["x", "y", "z", "x", "y"],
    })


class TestChangeReportBuilder:
    def test_basic_report(self, df_before):
        builder = ChangeReportBuilder(df_before)

        df_after = df_before.dropna().drop_duplicates().reset_index(drop=True)

        op_result = OperationResult(
            df=df_after,
            rows_before=5,
            rows_after=len(df_after),
        )
        builder.record_operation("test_op", {"key": "value"}, op_result)

        report = builder.build(df_after)

        assert report.rows_before == 5
        assert report.rows_after == len(df_after)
        assert len(report.operations_applied) == 1
        assert report.operations_applied[0]["operation"] == "test_op"

    def test_columns_added(self, df_before):
        builder = ChangeReportBuilder(df_before)

        df_after = df_before.copy()
        df_after["new_col"] = 42

        op_result = OperationResult(
            df=df_after,
            rows_before=5,
            rows_after=5,
            columns_added=["new_col"],
        )
        builder.record_operation("add_col", {}, op_result)

        report = builder.build(df_after)
        assert "new_col" in report.columns_added

    def test_columns_removed(self, df_before):
        builder = ChangeReportBuilder(df_before)

        df_after = df_before[["a"]].copy()

        op_result = OperationResult(
            df=df_after,
            rows_before=5,
            rows_after=5,
            columns_removed=["b"],
        )
        builder.record_operation("remove_col", {}, op_result)

        report = builder.build(df_after)
        assert "b" in report.columns_removed

    def test_type_changes(self, df_before):
        builder = ChangeReportBuilder(df_before)

        profiles_before = [
            ColumnProfile(name="a", inferred_type="int", nullable=True, distinct_count=3, missing_count=1, stats={}),
            ColumnProfile(name="b", inferred_type="string", nullable=False, distinct_count=3, missing_count=0, stats={}),
        ]
        profiles_after = [
            ColumnProfile(name="a", inferred_type="float", nullable=True, distinct_count=3, missing_count=1, stats={}),
            ColumnProfile(name="b", inferred_type="string", nullable=False, distinct_count=3, missing_count=0, stats={}),
        ]

        report = builder.build(df_before, profiles_before=profiles_before, profiles_after=profiles_after)
        assert len(report.type_changes) == 1
        assert report.type_changes[0]["column"] == "a"
        assert report.type_changes[0]["old_type"] == "int"
        assert report.type_changes[0]["new_type"] == "float"

    def test_to_dict(self, df_before):
        builder = ChangeReportBuilder(df_before)
        report = builder.build(df_before)
        d = report.to_dict()
        assert isinstance(d, dict)
        assert "rows_before" in d
        assert "operations_applied" in d
