from __future__ import annotations

import numpy as np
import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError

METHODS = ("iqr", "zscore")
ACTIONS = ("mark", "remove", "cap")


def detect_outliers(
    df: pd.DataFrame,
    *,
    column: str,
    method: str = "iqr",
    action: str = "mark",
    threshold: float = 1.5,
) -> pd.DataFrame:
    if column not in df.columns:
        raise ColumnNotFoundError(
            f"Column not found: '{column}'",
            details={"column": column},
        )
    if method not in METHODS:
        raise OperationConfigError(
            f"Unknown method '{method}'. Allowed: {METHODS}",
            details={"method": method},
        )
    if action not in ACTIONS:
        raise OperationConfigError(
            f"Unknown action '{action}'. Allowed: {ACTIONS}",
            details={"action": action},
        )

    result = df.copy()
    numeric = pd.to_numeric(result[column], errors="coerce")

    if method == "iqr":
        q1 = numeric.quantile(0.25)
        q3 = numeric.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
        outlier_mask = (numeric < lower) | (numeric > upper)
    else:
        mean = numeric.mean()
        std = numeric.std()
        if std == 0 or np.isnan(std):
            outlier_mask = pd.Series(False, index=df.index)
            lower, upper = mean, mean
        else:
            z = (numeric - mean) / std
            outlier_mask = z.abs() > threshold
            lower = mean - threshold * std
            upper = mean + threshold * std

    outlier_mask = outlier_mask.fillna(False)

    if action == "mark":
        result[f"{column}_is_outlier"] = outlier_mask
    elif action == "remove":
        result = result[~outlier_mask].reset_index(drop=True)
    elif action == "cap":
        capped = numeric.copy()
        capped = capped.clip(lower=lower, upper=upper)
        result[column] = capped

    return result
