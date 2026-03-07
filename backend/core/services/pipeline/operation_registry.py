from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from core.exceptions import OperationConfigError
from core.services.cleaning.categorical import (
    normalize_case,
    replace_values,
    trim_and_collapse,
)
from core.services.cleaning.duplicates import drop_duplicates
from core.services.cleaning.missing_values import fill_missing
from core.services.cleaning.outliers import detect_outliers
from core.services.cleaning.type_normalization import (
    cast_type,
    convert_empty_to_null,
    normalize_decimal_separators,
    trim_spaces,
)
from core.services.transforms.aggregations import aggregate
from core.services.transforms.derived_columns import (
    add_arithmetic_column,
    add_concat_column,
    add_conditional_column,
    add_ratio_column,
)
from core.services.transforms.filters import filter_rows
from core.services.transforms.structure import (
    rename_columns,
    reorder_columns,
    select_columns,
    sort_rows,
)


@dataclass
class OperationResult:
    df: pd.DataFrame
    rows_before: int
    rows_after: int
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)


OperationFn = Callable[[pd.DataFrame, dict], OperationResult]

_REGISTRY: dict[str, OperationFn] = {}


def register(name: str):
    def decorator(fn: OperationFn):
        _REGISTRY[name] = fn
        return fn
    return decorator


def get_operation(name: str) -> OperationFn:
    fn = _REGISTRY.get(name)
    if fn is None:
        raise OperationConfigError(
            f"Unknown operation '{name}'. Available: {list(_REGISTRY.keys())}",
            details={"operation": name, "available": list(_REGISTRY.keys())},
        )
    return fn


def list_operations() -> list[str]:
    return list(_REGISTRY.keys())


def _diff_columns(before: list[str], after: list[str]):
    before_set, after_set = set(before), set(after)
    return list(after_set - before_set), list(before_set - after_set)


# ── Cleaning operations ─────────────────────────────────────────────


@register("fill_missing")
def _op_fill_missing(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = fill_missing(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
        details={"strategy": params.get("strategy")},
    )


@register("drop_duplicates")
def _op_drop_duplicates(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = drop_duplicates(df, **params)
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        details={"removed": rows_before - len(result)},
    )


@register("cast_type")
def _op_cast_type(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = cast_type(df, **params)
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        details={"column": params.get("column"), "target_type": params.get("target_type")},
    )


@register("trim_spaces")
def _op_trim_spaces(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = trim_spaces(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("normalize_decimal_separators")
def _op_normalize_decimal_sep(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = normalize_decimal_separators(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("convert_empty_to_null")
def _op_convert_empty(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = convert_empty_to_null(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("detect_outliers")
def _op_detect_outliers(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = detect_outliers(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
        details={"method": params.get("method"), "action": params.get("action")},
    )


@register("normalize_case")
def _op_normalize_case(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = normalize_case(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("trim_and_collapse")
def _op_trim_and_collapse(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = trim_and_collapse(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("replace_values")
def _op_replace_values(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = replace_values(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


# ── Transform operations ────────────────────────────────────────────


@register("filter_rows")
def _op_filter_rows(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = filter_rows(df, **params)
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        details={"filtered_out": rows_before - len(result)},
    )


@register("add_arithmetic_column")
def _op_arithmetic(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = add_arithmetic_column(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("add_ratio_column")
def _op_ratio(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = add_ratio_column(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("add_conditional_column")
def _op_conditional(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = add_conditional_column(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("add_concat_column")
def _op_concat(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = add_concat_column(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("aggregate")
def _op_aggregate(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = aggregate(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("rename_columns")
def _op_rename(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = rename_columns(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("select_columns")
def _op_select(df: pd.DataFrame, params: dict) -> OperationResult:
    cols_before = list(df.columns)
    rows_before = len(df)
    result = select_columns(df, **params)
    added, removed = _diff_columns(cols_before, list(result.columns))
    return OperationResult(
        df=result, rows_before=rows_before, rows_after=len(result),
        columns_added=added, columns_removed=removed,
    )


@register("sort_rows")
def _op_sort(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = sort_rows(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))


@register("reorder_columns")
def _op_reorder(df: pd.DataFrame, params: dict) -> OperationResult:
    rows_before = len(df)
    result = reorder_columns(df, **params)
    return OperationResult(df=result, rows_before=rows_before, rows_after=len(result))
