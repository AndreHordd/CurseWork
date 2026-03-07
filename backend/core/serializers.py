from __future__ import annotations

import json

from django.conf import settings
from rest_framework import serializers

from core.models import Chart, ColumnMetadata, Dashboard, Dataset, Snapshot, Validation


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


# ────────────────────────── Stage 7: Analytics / Dashboards ──────────

STAGE7_CHART_TYPES = ("line", "bar", "table")
STAGE7_AGGREGATIONS = ("count", "sum", "avg", "min", "max", "median")
NUMERIC_AGGREGATIONS = ("sum", "avg", "min", "max", "median")


class SnapshotSummarySerializer(serializers.Serializer):
    snapshot_id = serializers.CharField()
    row_count = serializers.IntegerField()
    column_count = serializers.IntegerField()
    missing_total = serializers.IntegerField()
    missing_percentage = serializers.FloatField()
    duplicate_rows = serializers.IntegerField()
    duplicate_percentage = serializers.FloatField()
    numeric_columns = serializers.ListField()
    categorical_columns = serializers.ListField()
    datetime_columns = serializers.ListField()
    quality_summary = serializers.DictField()


# ── Dashboard ────────────────────────────────────────────────────────


class ChartBriefSerializer(serializers.ModelSerializer):
    class Meta:
        model = Chart
        fields = [
            "id", "chart_type", "title", "x", "y",
            "aggregation", "group_by", "filters", "options", "position",
        ]


class DashboardCreateSerializer(serializers.Serializer):
    snapshot_id = serializers.UUIDField()
    title = serializers.CharField(max_length=200)
    description = serializers.CharField(required=False, allow_blank=True, default="")
    layout = serializers.JSONField(required=False, default=dict)
    global_filters = serializers.JSONField(required=False, default=None)

    def validate_global_filters(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return {"conditions": value, "logic": "and"}
        if isinstance(value, dict):
            if "conditions" not in value:
                raise serializers.ValidationError(
                    "global_filters must contain 'conditions' key"
                )
            return value
        raise serializers.ValidationError("global_filters must be a list or object")


class DashboardUpdateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=200, required=False)
    description = serializers.CharField(required=False, allow_blank=True)
    layout = serializers.JSONField(required=False)
    global_filters = serializers.JSONField(required=False, allow_null=True)

    def validate_global_filters(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return {"conditions": value, "logic": "and"}
        if isinstance(value, dict):
            if "conditions" not in value:
                raise serializers.ValidationError(
                    "global_filters must contain 'conditions' key"
                )
            return value
        raise serializers.ValidationError("global_filters must be a list or object")


class DashboardDetailSerializer(serializers.ModelSerializer):
    charts = ChartBriefSerializer(many=True, read_only=True)
    snapshot_id = serializers.UUIDField(source="snapshot.pk", read_only=True)

    class Meta:
        model = Dashboard
        fields = [
            "id", "title", "description", "snapshot_id",
            "layout", "global_filters", "charts", "created_at",
        ]


class DashboardListSerializer(serializers.ModelSerializer):
    snapshot_id = serializers.UUIDField(source="snapshot.pk", read_only=True)
    chart_count = serializers.SerializerMethodField()

    class Meta:
        model = Dashboard
        fields = [
            "id", "title", "description", "snapshot_id",
            "chart_count", "created_at",
        ]

    def get_chart_count(self, obj):
        return obj.charts.count()


# ── Chart ────────────────────────────────────────────────────────────


class ChartCreateSerializer(serializers.Serializer):
    dashboard_id = serializers.UUIDField()
    chart_type = serializers.ChoiceField(choices=STAGE7_CHART_TYPES)
    title = serializers.CharField(max_length=200, required=False, allow_blank=True, default="")
    x = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True, default=None)
    y = serializers.ListField(child=serializers.CharField(), required=False, default=list)
    aggregation = serializers.ChoiceField(choices=STAGE7_AGGREGATIONS, required=False, allow_null=True, default=None)
    group_by = serializers.ListField(child=serializers.CharField(), required=False, default=list)
    filters = serializers.JSONField(required=False, default=None)
    options = serializers.JSONField(required=False, default=dict)
    position = serializers.JSONField(required=False, default=dict)

    def validate(self, attrs):
        chart_type = attrs["chart_type"]
        aggregation = attrs.get("aggregation")
        y_cols = attrs.get("y", [])

        if chart_type in ("line", "bar"):
            if not aggregation:
                raise serializers.ValidationError(
                    {"aggregation": "Aggregation is required for line/bar charts."}
                )
            if not y_cols:
                raise serializers.ValidationError(
                    {"y": "At least one Y column is required for line/bar charts."}
                )
            if not attrs.get("x") and not attrs.get("group_by"):
                raise serializers.ValidationError(
                    {"x": "Either x or group_by must be specified for line/bar charts."}
                )

        return attrs

    def validate_filters(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return {"conditions": value, "logic": "and"}
        if isinstance(value, dict):
            return value
        raise serializers.ValidationError("filters must be a list or object")


class ChartUpdateSerializer(serializers.Serializer):
    chart_type = serializers.ChoiceField(choices=STAGE7_CHART_TYPES, required=False)
    title = serializers.CharField(max_length=200, required=False, allow_blank=True)
    x = serializers.CharField(max_length=255, required=False, allow_blank=True, allow_null=True)
    y = serializers.ListField(child=serializers.CharField(), required=False)
    aggregation = serializers.ChoiceField(choices=STAGE7_AGGREGATIONS, required=False, allow_null=True)
    group_by = serializers.ListField(child=serializers.CharField(), required=False)
    filters = serializers.JSONField(required=False, allow_null=True)
    options = serializers.JSONField(required=False)
    position = serializers.JSONField(required=False)

    def validate_filters(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return {"conditions": value, "logic": "and"}
        if isinstance(value, dict):
            return value
        raise serializers.ValidationError("filters must be a list or object")


class ChartDetailSerializer(serializers.ModelSerializer):
    dashboard_id = serializers.UUIDField(source="dashboard.pk", read_only=True)

    class Meta:
        model = Chart
        fields = [
            "id", "dashboard_id", "chart_type", "title", "x", "y",
            "aggregation", "group_by", "filters", "options", "position",
        ]


class ChartDataSerializer(serializers.Serializer):
    chart_type = serializers.CharField()
    labels = serializers.ListField(required=False)
    datasets = serializers.ListField(required=False)
    columns = serializers.ListField(required=False)
    rows = serializers.ListField(required=False)
    meta = serializers.DictField()
