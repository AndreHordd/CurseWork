from __future__ import annotations

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError, TypeConversionError

ALLOWED_TYPES = ("int", "float", "date", "bool", "string")

_NULL_MARKERS = {"", "UNKNOWN", "N/A", "null", "None", "NA", "n/a", "none", "NULL"}


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )


def cast_type(
    df: pd.DataFrame,
    *,
    column: str,
    target_type: str,
) -> pd.DataFrame:
    _ensure_columns(df, [column])

    if target_type not in ALLOWED_TYPES:
        raise OperationConfigError(
            f"Unsupported target type '{target_type}'. Allowed: {ALLOWED_TYPES}",
            details={"target_type": target_type},
        )

    result = df.copy()
    series = result[column]

    try:
        if target_type == "int":
            result[column] = pd.to_numeric(series, errors="raise").astype("Int64")
        elif target_type == "float":
            result[column] = pd.to_numeric(series, errors="raise").astype("Float64")
        elif target_type == "date":
            result[column] = pd.to_datetime(series, errors="raise", format="mixed")
        elif target_type == "bool":
            mapping = {
                "true": True, "1": True, "yes": True, "t": True, "y": True,
                "false": False, "0": False, "no": False, "f": False, "n": False,
            }
            lowered = series.dropna().astype(str).str.strip().str.lower()
            unmapped = set(lowered.unique()) - set(mapping.keys())
            if unmapped:
                raise ValueError(f"Cannot map values to bool: {unmapped}")
            result[column] = series.map(
                lambda v: mapping.get(str(v).strip().lower(), v) if pd.notna(v) else v
            )
        else:
            result[column] = series.astype(str).where(series.notna(), other=None)
    except (ValueError, TypeError) as exc:
        raise TypeConversionError(
            f"Cannot cast column '{column}' to {target_type}: {exc}",
            details={"column": column, "target_type": target_type, "error": str(exc)},
        ) from exc

    return result


def trim_spaces(
    df: pd.DataFrame,
    *,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    cols = columns or [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
    _ensure_columns(df, cols)

    result = df.copy()
    for col in cols:
        if pd.api.types.is_string_dtype(result[col]):
            result[col] = result[col].str.strip()
    return result


def normalize_decimal_separators(
    df: pd.DataFrame,
    *,
    columns: list[str],
    from_sep: str = ",",
) -> pd.DataFrame:
    _ensure_columns(df, columns)

    result = df.copy()
    for col in columns:
        result[col] = (
            result[col]
            .astype(str)
            .str.replace(from_sep, ".", regex=False)
            .where(df[col].notna(), other=None)
        )
    return result


def convert_empty_to_null(
    df: pd.DataFrame,
    *,
    columns: list[str] | None = None,
    values: list[str] | None = None,
) -> pd.DataFrame:
    cols = columns or list(df.columns)
    _ensure_columns(df, cols)
    markers = set(values) if values else _NULL_MARKERS

    result = df.copy()
    for col in cols:
        if pd.api.types.is_string_dtype(result[col]):
            mask = result[col].isin(markers)
            result[col] = result[col].where(~mask, other=pd.NA)
    return result
