from __future__ import annotations

import re

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError

CASE_TYPES = ("lower", "upper", "title")


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )


def normalize_case(
    df: pd.DataFrame,
    *,
    columns: list[str],
    case_type: str = "lower",
) -> pd.DataFrame:
    _ensure_columns(df, columns)
    if case_type not in CASE_TYPES:
        raise OperationConfigError(
            f"Unknown case_type '{case_type}'. Allowed: {CASE_TYPES}",
            details={"case_type": case_type},
        )

    result = df.copy()
    for col in columns:
        s = result[col].astype(str).where(result[col].notna(), other=None)
        if s is None:
            continue
        if case_type == "lower":
            result[col] = s.str.lower()
        elif case_type == "upper":
            result[col] = s.str.upper()
        else:
            result[col] = s.str.title()
        result[col] = result[col].where(df[col].notna(), other=None)
    return result


def trim_and_collapse(
    df: pd.DataFrame,
    *,
    columns: list[str],
) -> pd.DataFrame:
    _ensure_columns(df, columns)

    result = df.copy()
    for col in columns:
        s = result[col]
        if pd.api.types.is_string_dtype(s):
            cleaned = s.str.strip().str.replace(r"\s+", " ", regex=True)
            result[col] = cleaned.where(s.notna(), other=None)
    return result


def replace_values(
    df: pd.DataFrame,
    *,
    column: str,
    mapping: dict,
) -> pd.DataFrame:
    _ensure_columns(df, [column])
    if not mapping:
        raise OperationConfigError(
            "Mapping cannot be empty",
            details={"column": column},
        )

    result = df.copy()
    result[column] = result[column].replace(mapping)
    return result
