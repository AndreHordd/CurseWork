from django.contrib import admin

from .models import (
    Chart,
    ColumnMetadata,
    Dashboard,
    Dataset,
    Experiment,
    ExperimentResult,
    Snapshot,
    Validation,
)


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ["name", "owner", "source_type", "file_format", "created_at"]
    list_filter = ["source_type", "file_format"]
    search_fields = ["name", "owner__username"]
    readonly_fields = ["id", "created_at", "updated_at"]


@admin.register(Snapshot)
class SnapshotAdmin(admin.ModelAdmin):
    list_display = ["dataset", "stage", "is_active", "is_ready_for_analysis", "row_count", "created_at"]
    list_filter = ["stage", "is_active", "is_ready_for_analysis", "storage_type"]
    search_fields = ["dataset__name"]
    readonly_fields = ["id", "created_at"]


@admin.register(ColumnMetadata)
class ColumnMetadataAdmin(admin.ModelAdmin):
    list_display = ["name", "snapshot", "inferred_type", "nullable", "distinct_count", "missing_count"]
    list_filter = ["inferred_type", "nullable"]
    search_fields = ["name", "snapshot__dataset__name"]
    readonly_fields = ["id", "created_at"]


@admin.register(Validation)
class ValidationAdmin(admin.ModelAdmin):
    list_display = ["rule_name", "snapshot", "status", "failed_count", "created_at"]
    list_filter = ["status"]
    search_fields = ["rule_name", "snapshot__dataset__name"]
    readonly_fields = ["id", "created_at"]


@admin.register(Dashboard)
class DashboardAdmin(admin.ModelAdmin):
    list_display = ["title", "owner", "snapshot", "created_at"]
    list_filter = ["owner"]
    search_fields = ["title", "owner__username"]
    readonly_fields = ["id", "created_at"]


@admin.register(Chart)
class ChartAdmin(admin.ModelAdmin):
    list_display = ["title", "dashboard", "chart_type", "x", "y", "aggregation"]
    list_filter = ["chart_type", "aggregation"]
    search_fields = ["title", "dashboard__title"]
    readonly_fields = ["id"]


@admin.register(Experiment)
class ExperimentAdmin(admin.ModelAdmin):
    list_display = ["name", "owner", "snapshot", "test_type", "status", "created_at"]
    list_filter = ["status", "test_type", "srm_enabled", "cuped_enabled"]
    search_fields = ["name", "owner__username"]
    readonly_fields = ["id", "created_at"]


@admin.register(ExperimentResult)
class ExperimentResultAdmin(admin.ModelAdmin):
    list_display = ["experiment", "decision", "p_value", "effect_abs", "effect_rel", "computed_at"]
    list_filter = ["decision"]
    search_fields = ["experiment__name"]
    readonly_fields = ["id", "computed_at"]
