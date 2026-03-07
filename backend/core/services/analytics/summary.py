from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from core.models import Snapshot
from core.services.analytics.snapshot_loader import load_dataframe

logger = logging.getLogger(__name__)

NUMERIC_TYPES = ("int", "float")


def _safe(val):
    """Convert numpy scalars to native Python types."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 6)
    if pd.isna(val):
        return None
    return val


def compute_summary(snapshot: Snapshot) -> dict:
    """Compute summary statistics for a snapshot using ColumnMetadata + DataFrame."""
    columns_meta = list(snapshot.column_metadata.all())
    df = load_dataframe(snapshot)

    row_count = snapshot.row_count or len(df)
    column_count = snapshot.column_count or len(df.columns)

    total_missing = sum(c.missing_count or 0 for c in columns_meta)
    duplicate_rows = int(df.duplicated().sum())

    numeric_columns = []
    categorical_columns = []
    datetime_columns = []

    for cm in columns_meta:
        col_name = cm.name
        if col_name not in df.columns:
            continue

        if cm.inferred_type in NUMERIC_TYPES:
            series = pd.to_numeric(df[col_name], errors="coerce").dropna()
            stats = {}
            if not series.empty:
                stats = {
                    "mean": _safe(series.mean()),
                    "median": _safe(series.median()),
                    "min": _safe(series.min()),
                    "max": _safe(series.max()),
                    "range": _safe(series.max() - series.min()),
                    "std": _safe(series.std()),
                }
            numeric_columns.append({
                "name": col_name,
                "inferred_type": cm.inferred_type,
                "missing_count": cm.missing_count or 0,
                "distinct_count": cm.distinct_count or 0,
                "stats": stats,
            })

        elif cm.inferred_type == "date":
            dt = pd.to_datetime(df[col_name], errors="coerce").dropna()
            stats = {}
            if not dt.empty:
                stats = {
                    "min_date": str(dt.min().date()),
                    "max_date": str(dt.max().date()),
                }
            datetime_columns.append({
                "name": col_name,
                "inferred_type": "date",
                "missing_count": cm.missing_count or 0,
                "distinct_count": cm.distinct_count or 0,
                "stats": stats,
            })

        else:
            str_vals = df[col_name].dropna().astype(str)
            top = str_vals.value_counts().head(5)
            categorical_columns.append({
                "name": col_name,
                "inferred_type": cm.inferred_type,
                "missing_count": cm.missing_count or 0,
                "distinct_count": cm.distinct_count or 0,
                "top_values": {str(k): int(v) for k, v in top.items()},
            })

    missing_pct = round(total_missing / (row_count * column_count) * 100, 2) if (row_count * column_count) else 0
    dup_pct = round(duplicate_rows / row_count * 100, 2) if row_count else 0

    return {
        "snapshot_id": str(snapshot.pk),
        "row_count": row_count,
        "column_count": column_count,
        "missing_total": total_missing,
        "missing_percentage": missing_pct,
        "duplicate_rows": duplicate_rows,
        "duplicate_percentage": dup_pct,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "datetime_columns": datetime_columns,
        "quality_summary": {
            "total_missing": total_missing,
            "missing_percentage": missing_pct,
            "duplicate_rows": duplicate_rows,
            "duplicate_percentage": dup_pct,
            "numeric_count": len(numeric_columns),
            "categorical_count": len(categorical_columns),
            "datetime_count": len(datetime_columns),
        },
    }
