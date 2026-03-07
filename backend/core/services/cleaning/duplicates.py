from __future__ import annotations

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError

KEEP_OPTIONS = ("first", "last", False)


def drop_duplicates(
    df: pd.DataFrame,
    *,
    subset: list[str] | None = None,
    keep: str | bool = "first",
) -> pd.DataFrame:
    if keep not in KEEP_OPTIONS and keep != "false":
        raise OperationConfigError(
            f"Invalid keep option '{keep}'. Allowed: first, last, false",
            details={"keep": keep},
        )

    if keep == "false":
        keep = False

    if subset is not None:
        missing = [c for c in subset if c not in df.columns]
        if missing:
            raise ColumnNotFoundError(
                f"Columns not found: {missing}",
                details={"missing_columns": missing},
            )

    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
