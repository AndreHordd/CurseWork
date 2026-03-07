from __future__ import annotations

import json

import numpy as np
import pandas as pd

from core.services.analytics.chart_query import ChartQueryResult


def _sanitize_value(val):
    """Convert numpy/pandas types to JSON-safe Python natives."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def _sanitize_list(values) -> list:
    return [_sanitize_value(v) for v in values]


def present_chart_data(result: ChartQueryResult) -> dict:
    """Convert ChartQueryResult into a Chart.js-compatible JSON structure."""
    if result.chart_type == "table":
        return _present_table(result)
    return _present_line_bar(result)


def _present_line_bar(result: ChartQueryResult) -> dict:
    df = result.aggregated_df
    x_col = result.x_column
    y_cols = result.y_columns

    if x_col and x_col in df.columns:
        labels = _sanitize_list(df[x_col].tolist())
    else:
        labels = list(range(len(df)))

    datasets = []
    for y_col in y_cols:
        if y_col in df.columns:
            datasets.append({
                "label": y_col,
                "data": _sanitize_list(df[y_col].tolist()),
            })

    return {
        "chart_type": result.chart_type,
        "labels": labels,
        "datasets": datasets,
        "meta": _build_meta(result),
    }


def _present_table(result: ChartQueryResult) -> dict:
    df = result.aggregated_df

    columns = list(df.columns)
    rows = []
    for _, row in df.iterrows():
        rows.append({col: _sanitize_value(row[col]) for col in columns})

    return {
        "chart_type": "table",
        "columns": columns,
        "rows": rows,
        "meta": _build_meta(result),
    }


def _build_meta(result: ChartQueryResult) -> dict:
    return {
        "source_row_count": result.source_row_count,
        "filtered_row_count": result.filtered_row_count,
        "result_row_count": len(result.aggregated_df),
        "aggregation": result.aggregation,
        "group_by": result.group_by,
        "applied_filters": result.applied_filters,
        "duration_ms": result.duration_ms,
    }
