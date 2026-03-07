from __future__ import annotations

import json

import pandas as pd
from django.conf import settings
from django.db import transaction

from core.models import ColumnMetadata, Snapshot, Validation
from core.services.profiler import ColumnProfile
from core.services.validators import ValidationResult


def persist_snapshot(
    *,
    parent_snapshot: Snapshot,
    stage: str,
    df: pd.DataFrame,
    step_config: dict,
    column_profiles: list[ColumnProfile],
    validation_results: list[ValidationResult],
) -> Snapshot:
    preview_n = getattr(settings, "IMPORT_PREVIEW_ROWS", 20)
    preview_rows = _df_to_json_safe(df.head(preview_n))
    data_json = _df_to_json_safe(df)

    with transaction.atomic():
        Snapshot.objects.filter(
            dataset=parent_snapshot.dataset,
            is_active=True,
        ).update(is_active=False)

        snapshot = Snapshot.objects.create(
            dataset=parent_snapshot.dataset,
            parent_snapshot=parent_snapshot,
            stage=stage,
            is_active=True,
            is_ready_for_analysis=(stage == "ready"),
            storage_type="jsonb",
            data_json=data_json,
            preview_rows=preview_rows,
            row_count=len(df),
            column_count=len(df.columns),
            step_config=step_config,
        )

        ColumnMetadata.objects.bulk_create([
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
        ])

        Validation.objects.bulk_create([
            Validation(
                snapshot=snapshot,
                rule_name=vr.rule_name,
                rule_params=vr.rule_params,
                status=vr.status,
                failed_count=vr.failed_count,
                sample_errors=vr.sample_errors,
            )
            for vr in validation_results
        ])

    return snapshot


def _df_to_json_safe(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", date_format="iso"))
