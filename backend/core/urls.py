from django.urls import path

from core.views import (
    ChartDataView,
    ChartDetailView,
    ChartListCreateView,
    DashboardDetailView,
    DashboardListCreateView,
    DatasetDetailView,
    DatasetImportView,
    DatasetListView,
    SnapshotCleanView,
    SnapshotDiffView,
    SnapshotHistoryView,
    SnapshotPreviewCleanView,
    SnapshotPreviewView,
    SnapshotQualityView,
    SnapshotSetActiveView,
    SnapshotSummaryView,
    SnapshotTransformView,
)

urlpatterns = [
    # Stage 5 — import & browse
    path("datasets/import/", DatasetImportView.as_view(), name="dataset-import"),
    path("datasets/", DatasetListView.as_view(), name="dataset-list"),
    path("datasets/<uuid:pk>/", DatasetDetailView.as_view(), name="dataset-detail"),
    path("snapshots/<uuid:pk>/preview/", SnapshotPreviewView.as_view(), name="snapshot-preview"),

    # Stage 6 — clean / transform
    path("snapshots/<uuid:pk>/clean/", SnapshotCleanView.as_view(), name="snapshot-clean"),
    path("snapshots/<uuid:pk>/transform/", SnapshotTransformView.as_view(), name="snapshot-transform"),
    path("snapshots/<uuid:pk>/quality/", SnapshotQualityView.as_view(), name="snapshot-quality"),
    path("snapshots/<uuid:pk>/history/", SnapshotHistoryView.as_view(), name="snapshot-history"),
    path("snapshots/<uuid:pk>/preview-clean/", SnapshotPreviewCleanView.as_view(), name="snapshot-preview-clean"),
    path("snapshots/<uuid:pk>/set-active/", SnapshotSetActiveView.as_view(), name="snapshot-set-active"),
    path("snapshots/<uuid:pk>/diff/<uuid:other_pk>/", SnapshotDiffView.as_view(), name="snapshot-diff"),

    # Stage 7 — analytics & dashboards
    path("snapshots/<uuid:pk>/summary/", SnapshotSummaryView.as_view(), name="snapshot-summary"),
    path("dashboards/", DashboardListCreateView.as_view(), name="dashboard-list-create"),
    path("dashboards/<uuid:pk>/", DashboardDetailView.as_view(), name="dashboard-detail"),
    path("charts/", ChartListCreateView.as_view(), name="chart-list-create"),
    path("charts/<uuid:pk>/", ChartDetailView.as_view(), name="chart-detail"),
    path("charts/<uuid:pk>/data/", ChartDataView.as_view(), name="chart-data"),
]
