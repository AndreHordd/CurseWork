from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

_BOOL_TRUTHY = {"true", "1", "yes", "t", "y"}
_BOOL_FALSY = {"false", "0", "no", "f", "n"}
_BOOL_VALUES = _BOOL_TRUTHY | _BOOL_FALSY


@dataclass
class ColumnProfile:
    name: str
    inferred_type: str  # int | float | string | date | bool
    nullable: bool
    distinct_count: int
    missing_count: int
    stats: dict
    parse_warnings: list[str] = field(default_factory=list)


@dataclass
class ProfileResult:
    columns: list[ColumnProfile]
    row_count: int
    column_count: int
    warnings: list[str] = field(default_factory=list)


def profile_dataframe(df: pd.DataFrame) -> ProfileResult:
    columns: list[ColumnProfile] = []
    warnings: list[str] = []

    for col_name in df.columns:
        series = df[col_name]
        profile = _profile_column(col_name, series)
        columns.append(profile)
        warnings.extend(profile.parse_warnings)

    return ProfileResult(
        columns=columns,
        row_count=len(df),
        column_count=len(df.columns),
        warnings=warnings,
    )


def _profile_column(name: str, series: pd.Series) -> ColumnProfile:
    missing_count = int(series.isna().sum())
    non_null = series.dropna()
    distinct_count = int(non_null.nunique())
    nullable = missing_count > 0

    if len(non_null) == 0:
        return ColumnProfile(
            name=name,
            inferred_type="string",
            nullable=True,
            distinct_count=0,
            missing_count=missing_count,
            stats={},
            parse_warnings=[f"Column '{name}' is entirely null — typed as string"],
        )

    inferred_type, parse_warnings = _infer_type(name, non_null)
    stats = _compute_stats(non_null, inferred_type)

    return ColumnProfile(
        name=name,
        inferred_type=inferred_type,
        nullable=nullable,
        distinct_count=distinct_count,
        missing_count=missing_count,
        stats=stats,
        parse_warnings=parse_warnings,
    )


def _infer_type(name: str, non_null: pd.Series) -> tuple[str, list[str]]:
    warnings: list[str] = []

    if pd.api.types.is_datetime64_any_dtype(non_null):
        return "date", warnings

    if _is_bool(non_null):
        return "bool", warnings

    numeric = pd.to_numeric(non_null, errors="coerce")
    numeric_ratio = numeric.notna().sum() / len(non_null)
    if numeric_ratio == 1.0:
        if (numeric == numeric.astype(int)).all():
            return "int", warnings
        return "float", warnings
    if numeric_ratio >= 0.8:
        bad_count = int((numeric.isna()).sum())
        warnings.append(
            f"Column '{name}': {bad_count} values could not be parsed as number — typed as string"
        )
        return "string", warnings

    dt = pd.to_datetime(non_null, errors="coerce", format="mixed")
    dt_ratio = dt.notna().sum() / len(non_null)
    if dt_ratio >= 0.9:
        if dt_ratio < 1.0:
            bad_count = int(dt.isna().sum())
            warnings.append(
                f"Column '{name}': {bad_count} values could not be parsed as date"
            )
        return "date", warnings

    return "string", warnings


def _is_bool(series: pd.Series) -> bool:
    str_vals = series.astype(str).str.strip().str.lower()
    return str_vals.isin(_BOOL_VALUES).all() and len(str_vals) > 0


def _compute_stats(non_null: pd.Series, inferred_type: str) -> dict:
    stats: dict = {}

    if inferred_type in ("int", "float"):
        numeric = pd.to_numeric(non_null, errors="coerce").dropna()
        if not numeric.empty:
            stats["min"] = _safe_scalar(numeric.min())
            stats["max"] = _safe_scalar(numeric.max())
            stats["mean"] = round(float(numeric.mean()), 4)
            stats["median"] = _safe_scalar(numeric.median())

    elif inferred_type == "date":
        dt = pd.to_datetime(non_null, errors="coerce").dropna()
        if not dt.empty:
            stats["min_date"] = str(dt.min().date())
            stats["max_date"] = str(dt.max().date())

    elif inferred_type == "bool":
        str_vals = non_null.astype(str).str.strip().str.lower()
        truthy = int(str_vals.isin(_BOOL_TRUTHY).sum())
        falsy = int(str_vals.isin(_BOOL_FALSY).sum())
        stats["true_count"] = truthy
        stats["false_count"] = falsy

    else:
        str_vals = non_null.astype(str)
        lengths = str_vals.str.len()
        stats["max_length"] = int(lengths.max())
        stats["avg_length"] = round(float(lengths.mean()), 2)
        top = str_vals.value_counts().head(5)
        stats["top_values"] = {str(k): int(v) for k, v in top.items()}

    return stats


def _safe_scalar(val):
    """Convert numpy scalar to native Python type for JSON serialization."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6)
    return val
