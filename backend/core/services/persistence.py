from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from django.conf import settings
from django.db import transaction

from core.models import ColumnMetadata, Dataset, Snapshot, Validation
from core.services.profiler import ColumnProfile
from core.services.validators import ValidationResult


@dataclass
class PersistResult:
    dataset: Dataset
    snapshot: Snapshot


def persist_import(
    *,
    owner,
    name: str,
    file_format: str,
    original_filename: str,
    import_settings: dict | None,
    df: pd.DataFrame,
    column_profiles: list[ColumnProfile],
    validation_results: list[ValidationResult],
) -> PersistResult:
    preview_n = getattr(settings, "IMPORT_PREVIEW_ROWS", 20)
    preview_rows = _df_to_json_safe(df.head(preview_n))
    data_json = _df_to_json_safe(df)

    with transaction.atomic():
        dataset = Dataset.objects.create(
            owner=owner,
            name=name,
            source_type="file",
            file_format=file_format,
            original_filename=original_filename,
            import_settings=import_settings,
        )

        snapshot = Snapshot.objects.create(
            dataset=dataset,
            stage="raw",
            is_active=True,
            is_ready_for_analysis=False,
            storage_type="jsonb",
            data_json=data_json,
            preview_rows=preview_rows,
            row_count=len(df),
            column_count=len(df.columns),
        )

        col_meta_objs = [
            ColumnMetadata(
                snapshot=snapshot,
                name=cp.name,
                inferred_type=cp.inferred_type,
                nullable=cp.nullable,
                distinct_count=cp.distinct_count,
                missing_count=cp.missing_count,
                stats=cp.stats,
            )
            for cp in column_profiles
        ]
        ColumnMetadata.objects.bulk_create(col_meta_objs)

        val_objs = [
            Validation(
                snapshot=snapshot,
                rule_name=vr.rule_name,
                rule_params=vr.rule_params,
                status=vr.status,
                failed_count=vr.failed_count,
                sample_errors=vr.sample_errors,
            )
            for vr in validation_results
        ]
        Validation.objects.bulk_create(val_objs)

    return PersistResult(dataset=dataset, snapshot=snapshot)


def _df_to_json_safe(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to a list of dicts safe for JSONB storage.

    Uses json round-trip to guarantee NaN/NaT become null, not the
    literal ``NaN`` token that PostgreSQL JSONB rejects.
    """
    import json
    return json.loads(df.to_json(orient="records", date_format="iso"))
