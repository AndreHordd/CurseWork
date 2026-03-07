from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
from django.conf import settings

from core.exceptions import DataCleanError, OperationConfigError
from core.models import Snapshot
from core.services.pipeline.change_report import ChangeReport, ChangeReportBuilder
from core.services.pipeline.operation_registry import OperationResult, get_operation
from core.services.pipeline.persistence import persist_snapshot
from core.services.profiler import ProfileResult, profile_dataframe
from core.services.validators import run_validations

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    snapshot_id: str | None
    stage: str
    change_report: dict
    preview_rows: list[dict]
    row_count: int
    column_count: int
    warnings: list[str] = field(default_factory=list)
    duration_ms: int = 0


def _load_dataframe(snapshot: Snapshot) -> pd.DataFrame:
    if snapshot.data_json:
        return pd.DataFrame(snapshot.data_json)
    raise OperationConfigError(
        "Snapshot has no data to process",
        details={"snapshot_id": str(snapshot.pk)},
    )


def run_pipeline(
    *,
    snapshot: Snapshot,
    operations: list[dict],
    pipeline_type: str = "clean",
    preview_only: bool = False,
) -> PipelineResult:
    start = time.monotonic()

    if not operations:
        raise OperationConfigError("At least one operation is required")

    df = _load_dataframe(snapshot)

    old_profiles = list(snapshot.column_metadata.all())
    old_column_profiles = [
        type("FakeProfile", (), {"name": cm.name, "inferred_type": cm.inferred_type})()
        for cm in old_profiles
    ]

    report_builder = ChangeReportBuilder(df)

    for step in operations:
        op_name = step.get("operation")
        params = step.get("params", {})

        op_fn = get_operation(op_name)
        try:
            result: OperationResult = op_fn(df, params)
        except DataCleanError:
            raise
        except Exception as exc:
            raise OperationConfigError(
                f"Operation '{op_name}' failed: {exc}",
                details={"operation": op_name, "error": str(exc)},
            ) from exc

        report_builder.record_operation(op_name, params, result)
        df = result.df

    profile: ProfileResult = profile_dataframe(df)
    warnings = list(profile.warnings)

    change_report: ChangeReport = report_builder.build(
        df,
        profiles_before=old_column_profiles,
        profiles_after=profile.columns,
    )

    preview_n = getattr(settings, "IMPORT_PREVIEW_ROWS", 20)
    preview_rows = json.loads(
        df.head(preview_n).to_json(orient="records", date_format="iso")
    )

    if preview_only:
        duration_ms = int((time.monotonic() - start) * 1000)
        return PipelineResult(
            snapshot_id=None,
            stage=_resolve_stage(pipeline_type),
            change_report=change_report.to_dict(),
            preview_rows=preview_rows,
            row_count=len(df),
            column_count=len(df.columns),
            warnings=warnings,
            duration_ms=duration_ms,
        )

    validations = run_validations(df, profile.columns)

    step_config = _build_step_config(pipeline_type, operations)
    stage = _resolve_stage(pipeline_type)

    new_snapshot = persist_snapshot(
        parent_snapshot=snapshot,
        stage=stage,
        df=df,
        step_config=step_config,
        column_profiles=profile.columns,
        validation_results=validations,
    )

    duration_ms = int((time.monotonic() - start) * 1000)

    logger.info(
        "Pipeline OK: snapshot=%s -> %s type=%s ops=%d rows=%d duration=%dms",
        snapshot.pk,
        new_snapshot.pk,
        pipeline_type,
        len(operations),
        len(df),
        duration_ms,
    )

    return PipelineResult(
        snapshot_id=str(new_snapshot.pk),
        stage=stage,
        change_report=change_report.to_dict(),
        preview_rows=preview_rows,
        row_count=len(df),
        column_count=len(df.columns),
        warnings=warnings,
        duration_ms=duration_ms,
    )


def _resolve_stage(pipeline_type: str) -> str:
    return "cleaned" if pipeline_type == "clean" else "transformed"


def _build_step_config(pipeline_type: str, operations: list[dict]) -> dict:
    steps = []
    for i, op in enumerate(operations):
        steps.append({
            "step_id": f"s{i + 1}",
            "operation": op.get("operation"),
            "params": op.get("params", {}),
            "depends_on": f"s{i}" if i > 0 else None,
            "status": "applied",
        })

    return {
        "pipeline_type": pipeline_type,
        "steps": steps,
        "applied_at": datetime.now(timezone.utc).isoformat(),
    }
