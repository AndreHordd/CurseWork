from __future__ import annotations

import pandas as pd

from core.exceptions import ColumnNotFoundError, EmptyResultError, OperationConfigError

OPERATORS = (
    "eq", "neq", "gt", "gte", "lt", "lte",
    "contains", "not_contains",
    "in_list", "not_in_list",
    "is_null", "is_not_null",
    "date_range",
)


def _apply_condition(
    df: pd.DataFrame,
    column: str,
    operator: str,
    value,
) -> pd.Series:
    if column not in df.columns:
        raise ColumnNotFoundError(
            f"Column not found: '{column}'",
            details={"column": column},
        )

    s = df[column]

    if operator == "eq":
        return s == value
    if operator == "neq":
        return s != value
    if operator == "gt":
        return pd.to_numeric(s, errors="coerce") > value
    if operator == "gte":
        return pd.to_numeric(s, errors="coerce") >= value
    if operator == "lt":
        return pd.to_numeric(s, errors="coerce") < value
    if operator == "lte":
        return pd.to_numeric(s, errors="coerce") <= value
    if operator == "contains":
        return s.astype(str).str.contains(str(value), na=False, regex=False)
    if operator == "not_contains":
        return ~s.astype(str).str.contains(str(value), na=False, regex=False)
    if operator == "in_list":
        return s.isin(value)
    if operator == "not_in_list":
        return ~s.isin(value)
    if operator == "is_null":
        return s.isna()
    if operator == "is_not_null":
        return s.notna()
    if operator == "date_range":
        dt = pd.to_datetime(s, errors="coerce")
        start = pd.to_datetime(value.get("start")) if value.get("start") else None
        end = pd.to_datetime(value.get("end")) if value.get("end") else None
        mask = pd.Series(True, index=df.index)
        if start is not None:
            mask = mask & (dt >= start)
        if end is not None:
            mask = mask & (dt <= end)
        return mask

    raise OperationConfigError(
        f"Unknown operator '{operator}'. Allowed: {OPERATORS}",
        details={"operator": operator},
    )


def filter_rows(
    df: pd.DataFrame,
    *,
    conditions: list[dict],
    logic: str = "and",
) -> pd.DataFrame:
    if not conditions:
        raise OperationConfigError("At least one condition is required")

    masks = []
    for cond in conditions:
        col = cond.get("column")
        op = cond.get("operator")
        val = cond.get("value")
        if not col or not op:
            raise OperationConfigError(
                "Each condition must have 'column' and 'operator'",
                details={"condition": cond},
            )
        masks.append(_apply_condition(df, col, op, val))

    if logic == "or":
        combined = masks[0]
        for m in masks[1:]:
            combined = combined | m
    else:
        combined = masks[0]
        for m in masks[1:]:
            combined = combined & m

    result = df[combined].reset_index(drop=True)
    if len(result) == 0:
        raise EmptyResultError(
            "Filter produced 0 rows",
            details={"conditions": conditions},
        )
    return result
