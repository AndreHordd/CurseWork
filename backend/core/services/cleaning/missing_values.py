from __future__ import annotations

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError

STRATEGIES = ("drop", "constant", "mean", "median", "mode", "keep")


def _validate_columns(df: pd.DataFrame, columns: list[str] | None) -> list[str]:
    if columns is None:
        return list(df.columns)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )
    return columns


def fill_missing(
    df: pd.DataFrame,
    *,
    columns: list[str] | None = None,
    strategy: str = "drop",
    fill_value=None,
) -> pd.DataFrame:
    if strategy not in STRATEGIES:
        raise OperationConfigError(
            f"Unknown strategy '{strategy}'. Allowed: {STRATEGIES}",
            details={"strategy": strategy, "allowed": list(STRATEGIES)},
        )

    cols = _validate_columns(df, columns)
    result = df.copy()

    if strategy == "keep":
        return result

    if strategy == "drop":
        return result.dropna(subset=cols).reset_index(drop=True)

    if strategy == "constant":
        for col in cols:
            result[col] = result[col].fillna(fill_value)
        return result

    numeric_strategies = ("mean", "median")
    if strategy in numeric_strategies:
        for col in cols:
            series = pd.to_numeric(result[col], errors="coerce")
            if series.notna().sum() == 0:
                raise OperationConfigError(
                    f"Cannot compute {strategy} for column '{col}': no numeric values",
                    details={"column": col, "strategy": strategy},
                )
            value = series.mean() if strategy == "mean" else series.median()
            result[col] = result[col].fillna(value)
        return result

    if strategy == "mode":
        for col in cols:
            mode_vals = result[col].mode()
            if len(mode_vals) == 0:
                continue
            result[col] = result[col].fillna(mode_vals.iloc[0])
        return result

    return result
