from __future__ import annotations

import json

from django.conf import settings
from rest_framework import serializers

from core.models import ColumnMetadata, Dataset, Snapshot, Validation


# ────────────────────────── Import (input) ──────────────────────────


class DatasetImportSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=200)
    file = serializers.FileField()
    import_settings = serializers.CharField(required=False, allow_blank=True)

    def validate_file(self, value):
        max_mb = getattr(settings, "IMPORT_MAX_FILE_SIZE_MB", 50)
        if value.size > max_mb * 1024 * 1024:
            raise serializers.ValidationError(
                f"File size {value.size / 1024 / 1024:.1f} MB exceeds limit of {max_mb} MB"
            )

        allowed = getattr(settings, "IMPORT_ALLOWED_FORMATS", ["csv", "xlsx", "json"])
        ext = value.name.rsplit(".", 1)[-1].lower() if "." in value.name else ""
        if ext not in allowed:
            raise serializers.ValidationError(
                f"Unsupported file extension: .{ext}. Allowed: {allowed}"
            )
        return value

    def validate_import_settings(self, value):
        if not value:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise serializers.ValidationError(
                f"import_settings must be valid JSON: {exc}"
            ) from exc


# ────────────────────────── Import result (output) ──────────────────


class ValidationRuleSummarySerializer(serializers.Serializer):
    rule_name = serializers.CharField()
    status = serializers.CharField()
    failed_count = serializers.IntegerField()


class ValidationSummarySerializer(serializers.Serializer):
    total_rules = serializers.IntegerField()
    passed = serializers.IntegerField()
    warn = serializers.IntegerField()
    failed = serializers.IntegerField()
    rules = ValidationRuleSummarySerializer(many=True)


class ImportResultSerializer(serializers.Serializer):
    dataset_id = serializers.CharField()
    snapshot_id = serializers.CharField()
    row_count = serializers.IntegerField()
    column_count = serializers.IntegerField()
    preview_rows = serializers.ListField()
    validation_summary = ValidationSummarySerializer()
    warnings = serializers.ListField(child=serializers.CharField())
    duration_ms = serializers.IntegerField()


# ────────────────────────── Dataset list / detail ───────────────────


class DatasetListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dataset
        fields = [
            "id",
            "name",
            "source_type",
            "file_format",
            "original_filename",
            "created_at",
            "updated_at",
        ]


class ColumnMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = ColumnMetadata
        fields = [
            "id",
            "name",
            "inferred_type",
            "nullable",
            "distinct_count",
            "missing_count",
            "stats",
        ]


class ValidationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Validation
        fields = [
            "id",
            "rule_name",
            "rule_params",
            "status",
            "failed_count",
            "sample_errors",
        ]


class SnapshotBriefSerializer(serializers.ModelSerializer):
    class Meta:
        model = Snapshot
        fields = [
            "id",
            "stage",
            "is_active",
            "row_count",
            "column_count",
            "created_at",
        ]


class DatasetDetailSerializer(serializers.ModelSerializer):
    active_snapshot = serializers.SerializerMethodField()

    class Meta:
        model = Dataset
        fields = [
            "id",
            "name",
            "source_type",
            "file_format",
            "original_filename",
            "import_settings",
            "created_at",
            "updated_at",
            "active_snapshot",
        ]

    def get_active_snapshot(self, obj: Dataset):
        snap = obj.snapshots.filter(is_active=True).first()
        if snap is None:
            return None
        return SnapshotBriefSerializer(snap).data


# ────────────────────────── Snapshot preview ────────────────────────


class SnapshotPreviewSerializer(serializers.ModelSerializer):
    columns = ColumnMetadataSerializer(source="column_metadata", many=True)
    validations = ValidationSerializer(many=True)

    class Meta:
        model = Snapshot
        fields = [
            "id",
            "stage",
            "row_count",
            "column_count",
            "preview_rows",
            "columns",
            "validations",
        ]


# ────────────────────────── Stage 6: Clean / Transform ──────────────

CLEAN_OPERATIONS = [
    "fill_missing", "drop_duplicates", "cast_type", "trim_spaces",
    "normalize_decimal_separators", "convert_empty_to_null",
    "detect_outliers", "normalize_case", "trim_and_collapse",
    "replace_values",
]

TRANSFORM_OPERATIONS = [
    "filter_rows", "add_arithmetic_column", "add_ratio_column",
    "add_conditional_column", "add_concat_column", "aggregate",
    "rename_columns", "select_columns", "sort_rows", "reorder_columns",
]


class OperationStepSerializer(serializers.Serializer):
    operation = serializers.CharField()
    params = serializers.DictField(default=dict)


class SnapshotCleanSerializer(serializers.Serializer):
    operations = OperationStepSerializer(many=True, min_length=1)
    preview_only = serializers.BooleanField(default=False)

    def validate_operations(self, value):
        for step in value:
            if step["operation"] not in CLEAN_OPERATIONS:
                raise serializers.ValidationError(
                    f"Unknown clean operation '{step['operation']}'. "
                    f"Allowed: {CLEAN_OPERATIONS}"
                )
        return value


class SnapshotTransformSerializer(serializers.Serializer):
    operations = OperationStepSerializer(many=True, min_length=1)
    preview_only = serializers.BooleanField(default=False)

    def validate_operations(self, value):
        for step in value:
            if step["operation"] not in TRANSFORM_OPERATIONS:
                raise serializers.ValidationError(
                    f"Unknown transform operation '{step['operation']}'. "
                    f"Allowed: {TRANSFORM_OPERATIONS}"
                )
        return value


class PipelineResultSerializer(serializers.Serializer):
    snapshot_id = serializers.CharField(allow_null=True)
    stage = serializers.CharField()
    change_report = serializers.DictField()
    preview_rows = serializers.ListField()
    row_count = serializers.IntegerField()
    column_count = serializers.IntegerField()
    warnings = serializers.ListField(child=serializers.CharField())
    duration_ms = serializers.IntegerField()


class SnapshotHistorySerializer(serializers.ModelSerializer):
    step_config = serializers.JSONField()
    parent_snapshot_id = serializers.UUIDField(allow_null=True)

    class Meta:
        model = Snapshot
        fields = [
            "id",
            "stage",
            "is_active",
            "row_count",
            "column_count",
            "step_config",
            "parent_snapshot_id",
            "created_at",
        ]


class QualityColumnSerializer(serializers.Serializer):
    name = serializers.CharField()
    inferred_type = serializers.CharField()
    missing_count = serializers.IntegerField()
    distinct_count = serializers.IntegerField()
    nullable = serializers.BooleanField()


class QualitySummarySerializer(serializers.Serializer):
    snapshot_id = serializers.CharField()
    stage = serializers.CharField()
    row_count = serializers.IntegerField()
    column_count = serializers.IntegerField()
    total_missing = serializers.IntegerField()
    duplicate_rows = serializers.IntegerField()
    columns = QualityColumnSerializer(many=True)
    validations = ValidationSerializer(many=True)


class SnapshotDiffSerializer(serializers.Serializer):
    snapshot_a = serializers.CharField()
    snapshot_b = serializers.CharField()
    rows_a = serializers.IntegerField()
    rows_b = serializers.IntegerField()
    columns_a = serializers.ListField(child=serializers.CharField())
    columns_b = serializers.ListField(child=serializers.CharField())
    columns_added = serializers.ListField(child=serializers.CharField())
    columns_removed = serializers.ListField(child=serializers.CharField())
    missing_a = serializers.DictField()
    missing_b = serializers.DictField()
    type_changes = serializers.ListField()
