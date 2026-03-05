from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field

from django.conf import settings

from core.exceptions import DataLimitError, DataValidationError, FileValidationError
from core.services.file_readers import ReadResult, read_file
from core.services.persistence import PersistResult, persist_import
from core.services.profiler import ProfileResult, profile_dataframe
from core.services.validators import (
    ValidationResult,
    has_blocking_failures,
    run_validations,
)

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    dataset_id: str
    snapshot_id: str
    row_count: int
    column_count: int
    preview_rows: list[dict]
    validation_summary: dict
    warnings: list[str] = field(default_factory=list)
    duration_ms: int = 0


def import_dataset(
    *,
    owner,
    name: str,
    file_obj,
    original_filename: str,
    import_settings: dict | None = None,
) -> ImportResult:
    start = time.monotonic()

    file_format = _resolve_format(original_filename)
    _check_file_size(file_obj)

    read_result: ReadResult = read_file(file_obj, file_format, import_settings)
    df = read_result.df
    warnings = list(read_result.warnings)

    columns_were_renamed = any("Renamed duplicate" in w for w in warnings)

    _check_data_limits(df)

    profile: ProfileResult = profile_dataframe(df)
    warnings.extend(profile.warnings)

    validations: list[ValidationResult] = run_validations(
        df,
        profile.columns,
        columns_were_renamed=columns_were_renamed,
    )

    if has_blocking_failures(validations):
        failed = [v for v in validations if v.status == "failed"]
        messages = [f"{v.rule_name}: {v.sample_errors}" for v in failed]
        raise DataValidationError(
            "Import blocked by validation failures",
            details={"failures": messages},
        )

    merged_settings = {**(import_settings or {}), **read_result.actual_settings}

    persist_result: PersistResult = persist_import(
        owner=owner,
        name=name,
        file_format=file_format,
        original_filename=original_filename,
        import_settings=merged_settings,
        df=df,
        column_profiles=profile.columns,
        validation_results=validations,
    )

    duration_ms = int((time.monotonic() - start) * 1000)

    import json as _json

    preview_n = getattr(settings, "IMPORT_PREVIEW_ROWS", 20)
    preview_rows = _json.loads(
        df.head(preview_n).to_json(orient="records", date_format="iso")
    )

    summary = _build_validation_summary(validations)

    logger.info(
        "Import OK: user=%s dataset=%s format=%s rows=%d cols=%d duration=%dms",
        owner.pk,
        name,
        file_format,
        profile.row_count,
        profile.column_count,
        duration_ms,
    )

    return ImportResult(
        dataset_id=str(persist_result.dataset.pk),
        snapshot_id=str(persist_result.snapshot.pk),
        row_count=profile.row_count,
        column_count=profile.column_count,
        preview_rows=preview_rows,
        validation_summary=summary,
        warnings=warnings,
        duration_ms=duration_ms,
    )


def _resolve_format(filename: str) -> str:
    ext = os.path.splitext(filename)[-1].lstrip(".").lower()
    allowed = getattr(settings, "IMPORT_ALLOWED_FORMATS", ["csv", "xlsx", "json"])
    if ext not in allowed:
        raise FileValidationError(
            f"Unsupported file extension: .{ext}",
            details={"extension": ext, "allowed": allowed},
        )
    return ext


def _check_file_size(file_obj) -> None:
    max_bytes = getattr(settings, "IMPORT_MAX_FILE_SIZE_MB", 50) * 1024 * 1024
    if hasattr(file_obj, "size"):
        size = file_obj.size
    else:
        pos = file_obj.tell()
        file_obj.seek(0, 2)
        size = file_obj.tell()
        file_obj.seek(pos)

    if size > max_bytes:
        raise FileValidationError(
            f"File size {size / 1024 / 1024:.1f} MB exceeds limit "
            f"of {settings.IMPORT_MAX_FILE_SIZE_MB} MB",
            details={
                "file_size_mb": round(size / 1024 / 1024, 2),
                "limit_mb": settings.IMPORT_MAX_FILE_SIZE_MB,
            },
        )


def _check_data_limits(df) -> None:
    max_rows = getattr(settings, "IMPORT_MAX_ROWS", 100_000)
    max_cols = getattr(settings, "IMPORT_MAX_COLUMNS", 200)

    if len(df) > max_rows:
        raise DataLimitError(
            f"Row count {len(df)} exceeds limit of {max_rows}",
            details={"row_count": len(df), "limit": max_rows},
        )
    if len(df.columns) > max_cols:
        raise DataLimitError(
            f"Column count {len(df.columns)} exceeds limit of {max_cols}",
            details={"column_count": len(df.columns), "limit": max_cols},
        )


def _build_validation_summary(validations: list[ValidationResult]) -> dict:
    total = len(validations)
    passed = sum(1 for v in validations if v.status == "passed")
    warned = sum(1 for v in validations if v.status == "warn")
    failed = sum(1 for v in validations if v.status == "failed")
    return {
        "total_rules": total,
        "passed": passed,
        "warn": warned,
        "failed": failed,
        "rules": [
            {
                "rule_name": v.rule_name,
                "status": v.status,
                "failed_count": v.failed_count,
            }
            for v in validations
        ],
    }
