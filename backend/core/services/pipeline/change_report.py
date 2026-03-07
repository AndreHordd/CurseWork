from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from core.services.pipeline.operation_registry import OperationResult
from core.services.profiler import ColumnProfile


@dataclass
class ChangeReport:
    rows_before: int
    rows_after: int
    columns_before: list[str]
    columns_after: list[str]
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    missing_before: dict[str, int] = field(default_factory=dict)
    missing_after: dict[str, int] = field(default_factory=dict)
    duplicates_before: int = 0
    duplicates_after: int = 0
    type_changes: list[dict] = field(default_factory=list)
    operations_applied: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "columns_before": self.columns_before,
            "columns_after": self.columns_after,
            "columns_added": self.columns_added,
            "columns_removed": self.columns_removed,
            "missing_before": self.missing_before,
            "missing_after": self.missing_after,
            "duplicates_before": self.duplicates_before,
            "duplicates_after": self.duplicates_after,
            "type_changes": self.type_changes,
            "operations_applied": self.operations_applied,
        }


class ChangeReportBuilder:
    def __init__(self, df_before: pd.DataFrame):
        self._rows_before = len(df_before)
        self._cols_before = list(df_before.columns)
        self._missing_before = {
            col: int(df_before[col].isna().sum()) for col in df_before.columns
        }
        self._duplicates_before = int(df_before.duplicated().sum())
        self._operations: list[dict] = []

    def record_operation(
        self,
        operation_name: str,
        params: dict,
        result: OperationResult,
    ) -> None:
        self._operations.append({
            "operation": operation_name,
            "params": _sanitize_params(params),
            "rows_before": result.rows_before,
            "rows_after": result.rows_after,
            "columns_added": result.columns_added,
            "columns_removed": result.columns_removed,
            "details": result.details,
        })

    def build(
        self,
        df_after: pd.DataFrame,
        profiles_before: list[ColumnProfile] | None = None,
        profiles_after: list[ColumnProfile] | None = None,
    ) -> ChangeReport:
        cols_after = list(df_after.columns)
        cols_before_set = set(self._cols_before)
        cols_after_set = set(cols_after)

        missing_after = {
            col: int(df_after[col].isna().sum()) for col in df_after.columns
        }
        duplicates_after = int(df_after.duplicated().sum())

        type_changes = []
        if profiles_before and profiles_after:
            before_map = {p.name: p.inferred_type for p in profiles_before}
            after_map = {p.name: p.inferred_type for p in profiles_after}
            for col in after_map:
                if col in before_map and before_map[col] != after_map[col]:
                    type_changes.append({
                        "column": col,
                        "old_type": before_map[col],
                        "new_type": after_map[col],
                    })

        return ChangeReport(
            rows_before=self._rows_before,
            rows_after=len(df_after),
            columns_before=self._cols_before,
            columns_after=cols_after,
            columns_added=list(cols_after_set - cols_before_set),
            columns_removed=list(cols_before_set - cols_after_set),
            missing_before=self._missing_before,
            missing_after=missing_after,
            duplicates_before=self._duplicates_before,
            duplicates_after=duplicates_after,
            type_changes=type_changes,
            operations_applied=self._operations,
        )


def _sanitize_params(params: dict) -> dict:
    """Return a JSON-safe copy of params (strip DataFrames, etc.)."""
    safe = {}
    for k, v in params.items():
        if isinstance(v, pd.DataFrame):
            continue
        safe[k] = v
    return safe
