from __future__ import annotations

import re

import pandas as pd

from core.exceptions import ColumnNotFoundError, OperationConfigError

_SAFE_TOKEN = re.compile(r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.\,]+$")


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"Columns not found: {missing}",
            details={"missing_columns": missing},
        )


def add_arithmetic_column(
    df: pd.DataFrame,
    *,
    new_name: str,
    expression: str,
) -> pd.DataFrame:
    if not _SAFE_TOKEN.match(expression):
        raise OperationConfigError(
            f"Expression contains disallowed characters: {expression}",
            details={"expression": expression},
        )

    result = df.copy()
    local_ns = {}
    for col in df.columns:
        local_ns[col] = pd.to_numeric(result[col], errors="coerce")

    try:
        result[new_name] = eval(expression, {"__builtins__": {}}, local_ns)  # noqa: S307
    except Exception as exc:
        raise OperationConfigError(
            f"Cannot evaluate expression '{expression}': {exc}",
            details={"expression": expression, "error": str(exc)},
        ) from exc

    return result


def add_ratio_column(
    df: pd.DataFrame,
    *,
    new_name: str,
    numerator: str,
    denominator: str,
) -> pd.DataFrame:
    _ensure_columns(df, [numerator, denominator])

    result = df.copy()
    num = pd.to_numeric(result[numerator], errors="coerce")
    den = pd.to_numeric(result[denominator], errors="coerce")
    result[new_name] = num / den.replace(0, float("nan"))
    return result


def add_conditional_column(
    df: pd.DataFrame,
    *,
    new_name: str,
    conditions: list[dict],
    default=None,
) -> pd.DataFrame:
    """Each condition: {"column", "operator", "value", "result"}."""
    if not conditions:
        raise OperationConfigError("At least one condition is required")

    result = df.copy()
    output = pd.Series(default, index=df.index)

    for cond in reversed(conditions):
        col = cond.get("column")
        op = cond.get("operator", "eq")
        val = cond.get("value")
        res = cond.get("result")

        if col not in df.columns:
            raise ColumnNotFoundError(
                f"Column not found: '{col}'",
                details={"column": col},
            )

        s = result[col]
        if op == "eq":
            mask = s == val
        elif op == "neq":
            mask = s != val
        elif op == "gt":
            mask = pd.to_numeric(s, errors="coerce") > val
        elif op == "lt":
            mask = pd.to_numeric(s, errors="coerce") < val
        elif op == "gte":
            mask = pd.to_numeric(s, errors="coerce") >= val
        elif op == "lte":
            mask = pd.to_numeric(s, errors="coerce") <= val
        elif op == "contains":
            mask = s.astype(str).str.contains(str(val), na=False, regex=False)
        else:
            raise OperationConfigError(f"Unsupported operator in condition: {op}")

        output = output.where(~mask, res)

    result[new_name] = output
    return result


def add_concat_column(
    df: pd.DataFrame,
    *,
    new_name: str,
    columns: list[str],
    separator: str = " ",
) -> pd.DataFrame:
    _ensure_columns(df, columns)

    result = df.copy()
    parts = [result[c].astype(str).fillna("") for c in columns]
    result[new_name] = parts[0]
    for p in parts[1:]:
        result[new_name] = result[new_name] + separator + p
    return result
