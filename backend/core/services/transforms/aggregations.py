from __future__ import annotations

import pandas as pd

from core.exceptions import AggregationConfigError, ColumnNotFoundError

ALLOWED_FUNCS = ("count", "sum", "avg", "min", "max", "median")

_PANDAS_MAP = {
    "count": "count",
    "sum": "sum",
    "avg": "mean",
    "min": "min",
    "max": "max",
    "median": "median",
}


def aggregate(
    df: pd.DataFrame,
    *,
    group_by: list[str],
    agg_config: dict[str, list[str]],
) -> pd.DataFrame:
    if not group_by:
        raise AggregationConfigError("group_by must not be empty")
    if not agg_config:
        raise AggregationConfigError("agg_config must not be empty")

    all_cols = list(group_by) + list(agg_config.keys())
    missing = [c for c in all_cols if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )

    for col, funcs in agg_config.items():
        for f in funcs:
            if f not in ALLOWED_FUNCS:
                raise AggregationConfigError(
                    f"Unknown aggregation function '{f}'. Allowed: {ALLOWED_FUNCS}",
                    details={"function": f, "column": col},
                )

    pandas_agg: dict[str, list[str]] = {}
    for col, funcs in agg_config.items():
        pandas_agg[col] = [_PANDAS_MAP[f] for f in funcs]

    grouped = df.groupby(group_by, as_index=False).agg(pandas_agg)
    new_cols = []
    for col, func in grouped.columns:
        if func == "" or func is None:
            new_cols.append(col)
        else:
            new_cols.append(f"{col}_{func}")
    grouped.columns = new_cols

    if len(grouped) == 0:
        raise AggregationConfigError(
            "Aggregation produced 0 rows",
            details={"group_by": group_by},
        )

    return grouped
