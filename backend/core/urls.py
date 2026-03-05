from django.urls import path

from core.views import (
    DatasetDetailView,
    DatasetImportView,
    DatasetListView,
    SnapshotPreviewView,
)

urlpatterns = [
    path("datasets/import/", DatasetImportView.as_view(), name="dataset-import"),
    path("datasets/", DatasetListView.as_view(), name="dataset-list"),
    path("datasets/<uuid:pk>/", DatasetDetailView.as_view(), name="dataset-detail"),
    path("snapshots/<uuid:pk>/preview/", SnapshotPreviewView.as_view(), name="snapshot-preview"),
]
