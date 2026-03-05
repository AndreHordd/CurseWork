from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from core.services.profiler import ColumnProfile

_SAMPLE_ERRORS_LIMIT = 5


@dataclass
class ValidationResult:
    rule_name: str
    rule_params: dict | None
    status: str  # passed | failed | warn
    failed_count: int
    sample_errors: list | None = None


def run_validations(
    df: pd.DataFrame,
    column_profiles: list[ColumnProfile],
    *,
    columns_were_renamed: bool = False,
) -> list[ValidationResult]:
    results: list[ValidationResult] = []

    results.append(_check_table_not_empty(df))
    results.append(_check_column_names_unique(columns_were_renamed))
    results.append(_check_duplicate_rows(df))

    for cp in column_profiles:
        mv = _check_missing_values(df, cp)
        if mv is not None:
            results.append(mv)

    for cp in column_profiles:
        tp = _check_type_parse_issues(df, cp)
        if tp is not None:
            results.append(tp)

    return results


def has_blocking_failures(results: list[ValidationResult]) -> bool:
    return any(r.status == "failed" for r in results)


def _check_table_not_empty(df: pd.DataFrame) -> ValidationResult:
    if len(df) == 0:
        return ValidationResult(
            rule_name="table_not_empty",
            rule_params=None,
            status="failed",
            failed_count=0,
            sample_errors=["Dataset contains 0 rows"],
        )
    return ValidationResult(
        rule_name="table_not_empty",
        rule_params=None,
        status="passed",
        failed_count=0,
    )


def _check_column_names_unique(columns_were_renamed: bool) -> ValidationResult:
    if columns_were_renamed:
        return ValidationResult(
            rule_name="column_names_unique",
            rule_params=None,
            status="warn",
            failed_count=0,
            sample_errors=["Duplicate column names were auto-renamed"],
        )
    return ValidationResult(
        rule_name="column_names_unique",
        rule_params=None,
        status="passed",
        failed_count=0,
    )


def _check_duplicate_rows(df: pd.DataFrame) -> ValidationResult:
    dup_mask = df.duplicated(keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count > 0:
        sample_indices = (
            df.index[dup_mask].tolist()[:_SAMPLE_ERRORS_LIMIT]
        )
        return ValidationResult(
            rule_name="duplicate_rows",
            rule_params=None,
            status="warn",
            failed_count=dup_count,
            sample_errors=[f"Duplicate row at index {i}" for i in sample_indices],
        )
    return ValidationResult(
        rule_name="duplicate_rows",
        rule_params=None,
        status="passed",
        failed_count=0,
    )


def _check_missing_values(
    df: pd.DataFrame,
    cp: ColumnProfile,
) -> ValidationResult | None:
    if cp.missing_count == 0:
        return None
    null_indices = (
        df.index[df[cp.name].isna()].tolist()[:_SAMPLE_ERRORS_LIMIT]
    )
    return ValidationResult(
        rule_name=f"missing_values:{cp.name}",
        rule_params={"column": cp.name},
        status="warn",
        failed_count=cp.missing_count,
        sample_errors=[f"NULL at index {i}" for i in null_indices],
    )


def _check_type_parse_issues(
    df: pd.DataFrame,
    cp: ColumnProfile,
) -> ValidationResult | None:
    if cp.inferred_type in ("string", "bool"):
        return None

    series = df[cp.name].dropna()
    if series.empty:
        return None

    if cp.inferred_type in ("int", "float"):
        coerced = pd.to_numeric(series, errors="coerce")
        bad_mask = coerced.isna() & series.notna()
    elif cp.inferred_type == "date":
        coerced = pd.to_datetime(series, errors="coerce", format="mixed")
        bad_mask = coerced.isna() & series.notna()
    else:
        return None

    bad_count = int(bad_mask.sum())
    if bad_count == 0:
        return None

    bad_values = series[bad_mask].head(_SAMPLE_ERRORS_LIMIT).tolist()
    return ValidationResult(
        rule_name=f"type_parse_issues:{cp.name}",
        rule_params={"column": cp.name, "expected_type": cp.inferred_type},
        status="warn",
        failed_count=bad_count,
        sample_errors=[f"Cannot parse '{v}' as {cp.inferred_type}" for v in bad_values],
    )
