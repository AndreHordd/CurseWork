from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from core.exceptions import ChartConfigError
from core.models import Chart, Dashboard
from core.services.analytics.snapshot_loader import load_dataframe
from core.services.transforms.filters import filter_rows

logger = logging.getLogger(__name__)

MAX_TABLE_ROWS = 1000
MAX_CATEGORIES = 50

NUMERIC_AGGREGATIONS = ("sum", "avg", "min", "max", "median")

_PANDAS_AGG = {
    "count": "count",
    "sum": "sum",
    "avg": "mean",
    "min": "min",
    "max": "max",
    "median": "median",
}


@dataclass
class ChartQueryResult:
    chart_type: str
    aggregated_df: pd.DataFrame
    x_column: str | None
    y_columns: list[str]
    group_by: list[str]
    aggregation: str | None
    source_row_count: int
    filtered_row_count: int
    applied_filters: list[dict] = field(default_factory=list)
    duration_ms: int = 0


def _apply_filters(df: pd.DataFrame, filters_config: dict | list | None) -> tuple[pd.DataFrame, list[dict]]:
    """Apply filter conditions, returning filtered df and list of applied conditions."""
    if not filters_config:
        return df, []

    if isinstance(filters_config, list):
        conditions = filters_config
        logic = "and"
    else:
        conditions = filters_config.get("conditions", [])
        logic = filters_config.get("logic", "and")

    if not conditions:
        return df, []

    try:
        filtered = filter_rows(df, conditions=conditions, logic=logic)
    except Exception:
        filtered = df

    return filtered, conditions


def execute_chart_query(chart: Chart, dashboard: Dashboard) -> ChartQueryResult:
    """Execute aggregation query for a chart, returning raw result for presenters."""
    start = time.monotonic()

    snapshot = dashboard.snapshot
    df = load_dataframe(snapshot)
    source_row_count = len(df)

    all_applied_filters = []

    df, global_applied = _apply_filters(df, dashboard.global_filters)
    all_applied_filters.extend(global_applied)

    df, local_applied = _apply_filters(df, chart.filters)
    all_applied_filters.extend(local_applied)

    filtered_row_count = len(df)

    x_column = chart.x
    y_columns = chart.y or []
    if isinstance(y_columns, str):
        y_columns = [y_columns]
    group_by = chart.group_by or []
    if isinstance(group_by, str):
        group_by = [group_by]
    aggregation = chart.aggregation

    _validate_columns(df, x_column, y_columns, group_by)

    if chart.chart_type == "table":
        result_df = _build_table_result(df, x_column, y_columns, group_by, aggregation)
    else:
        result_df = _build_chart_result(df, x_column, y_columns, group_by, aggregation)

    duration_ms = int((time.monotonic() - start) * 1000)

    logger.info(
        "Chart query: chart=%s type=%s rows=%d->%d->%d duration=%dms",
        chart.pk, chart.chart_type, source_row_count, filtered_row_count,
        len(result_df), duration_ms,
    )

    return ChartQueryResult(
        chart_type=chart.chart_type,
        aggregated_df=result_df,
        x_column=x_column,
        y_columns=y_columns,
        group_by=group_by,
        aggregation=aggregation,
        source_row_count=source_row_count,
        filtered_row_count=filtered_row_count,
        applied_filters=all_applied_filters,
        duration_ms=duration_ms,
    )


def _validate_columns(
    df: pd.DataFrame,
    x_column: str | None,
    y_columns: list[str],
    group_by: list[str],
):
    all_cols = set(df.columns)
    missing = []
    if x_column and x_column not in all_cols:
        missing.append(x_column)
    for col in y_columns:
        if col not in all_cols:
            missing.append(col)
    for col in group_by:
        if col not in all_cols:
            missing.append(col)
    if missing:
        raise ChartConfigError(
            f"Columns not found in snapshot: {missing}",
            details={"missing_columns": missing, "available_columns": list(all_cols)},
        )


def _build_chart_result(
    df: pd.DataFrame,
    x_column: str | None,
    y_columns: list[str],
    group_by: list[str],
    aggregation: str | None,
) -> pd.DataFrame:
    if not aggregation:
        raise ChartConfigError(
            "Aggregation is required for line/bar charts",
            details={"chart_type": "line/bar"},
        )
    if not y_columns:
        raise ChartConfigError("At least one Y column is required for line/bar charts")

    agg_func = _PANDAS_AGG.get(aggregation)
    if not agg_func:
        raise ChartConfigError(
            f"Unknown aggregation: {aggregation}",
            details={"aggregation": aggregation},
        )

    grouping_cols = []
    if x_column:
        grouping_cols.append(x_column)
    grouping_cols.extend(c for c in group_by if c not in grouping_cols)

    if not grouping_cols:
        raise ChartConfigError("At least x or group_by must be specified for line/bar charts")

    if aggregation == "count":
        agg_dict = {col: "count" for col in y_columns}
    else:
        for col in y_columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                numeric_check = pd.to_numeric(df[col], errors="coerce")
                if numeric_check.isna().all():
                    raise ChartConfigError(
                        f"Column '{col}' is not numeric, cannot apply '{aggregation}'",
                        details={"column": col, "aggregation": aggregation},
                    )
        agg_dict = {col: agg_func for col in y_columns}

    result = df.groupby(grouping_cols, as_index=False, dropna=False).agg(agg_dict)

    if len(result) > MAX_CATEGORIES:
        sort_col = y_columns[0]
        result = result.sort_values(sort_col, ascending=False).head(MAX_CATEGORIES)

    return result


def _build_table_result(
    df: pd.DataFrame,
    x_column: str | None,
    y_columns: list[str],
    group_by: list[str],
    aggregation: str | None,
) -> pd.DataFrame:
    if aggregation and group_by:
        grouping_cols = list(group_by)
        if x_column and x_column not in grouping_cols:
            grouping_cols.insert(0, x_column)

        value_cols = y_columns if y_columns else [c for c in df.columns if c not in grouping_cols]
        if not value_cols:
            raise ChartConfigError("No value columns for table aggregation")

        agg_func = _PANDAS_AGG.get(aggregation, "count")
        agg_dict = {col: agg_func for col in value_cols}
        result = df.groupby(grouping_cols, as_index=False, dropna=False).agg(agg_dict)
    else:
        select_cols = []
        if x_column:
            select_cols.append(x_column)
        select_cols.extend(c for c in y_columns if c not in select_cols)
        select_cols.extend(c for c in group_by if c not in select_cols)

        if select_cols:
            result = df[select_cols]
        else:
            result = df

    if len(result) > MAX_TABLE_ROWS:
        result = result.head(MAX_TABLE_ROWS)

    return result
