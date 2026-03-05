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
