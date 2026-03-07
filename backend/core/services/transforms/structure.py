from __future__ import annotations

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )


def rename_columns(
    df: pd.DataFrame,
    *,
    mapping: dict[str, str],
) -> pd.DataFrame:
    if not mapping:
        raise OperationConfigError("Rename mapping cannot be empty")
    _ensure_columns(df, list(mapping.keys()))
    return df.rename(columns=mapping)


def select_columns(
    df: pd.DataFrame,
    *,
    columns: list[str],
) -> pd.DataFrame:
    if not columns:
        raise OperationConfigError("Column list cannot be empty")
    _ensure_columns(df, columns)
    return df[columns].copy()


def sort_rows(
    df: pd.DataFrame,
    *,
    by: list[str],
    ascending: bool | list[bool] = True,
) -> pd.DataFrame:
    if not by:
        raise OperationConfigError("Sort 'by' cannot be empty")
    _ensure_columns(df, by)
    return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)


def reorder_columns(
    df: pd.DataFrame,
    *,
    order: list[str],
) -> pd.DataFrame:
    if not order:
        raise OperationConfigError("Column order cannot be empty")
    _ensure_columns(df, order)
    remaining = [c for c in df.columns if c not in order]
    return df[order + remaining].copy()
