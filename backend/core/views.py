from __future__ import annotations

import logging
import time

from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.exceptions import DataCleanError, DataImportError
from core.models import ColumnMetadata, Dataset, Snapshot
from core.serializers import (
    DatasetDetailSerializer,
    DatasetImportSerializer,
    DatasetListSerializer,
    ImportResultSerializer,
    PipelineResultSerializer,
    QualitySummarySerializer,
    SnapshotCleanSerializer,
    SnapshotDiffSerializer,
    SnapshotHistorySerializer,
    SnapshotPreviewSerializer,
    SnapshotTransformSerializer,
    ValidationSerializer,
)
from core.services.import_service import import_dataset
from core.services.pipeline.snapshot_pipeline import run_pipeline

logger = logging.getLogger(__name__)


def _success(data, warnings=None, meta=None, http_status=status.HTTP_200_OK):
    return Response(
        {
            "success": True,
            "data": data,
            "errors": [],
            "warnings": warnings or [],
            "meta": meta or {},
        },
        status=http_status,
    )


def _error(errors, http_status=status.HTTP_400_BAD_REQUEST, warnings=None, meta=None):
    return Response(
        {
            "success": False,
            "data": None,
            "errors": errors if isinstance(errors, list) else [errors],
            "warnings": warnings or [],
            "meta": meta or {},
        },
        status=http_status,
    )


class DatasetImportView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        ser = DatasetImportSerializer(data=request.data)
        if not ser.is_valid():
            return _error(
                [{"field": k, "messages": v} for k, v in ser.errors.items()],
                http_status=status.HTTP_400_BAD_REQUEST,
            )

        validated = ser.validated_data
        file_obj = validated["file"]

        try:
            result = import_dataset(
                owner=request.user,
                name=validated["name"],
                file_obj=file_obj,
                original_filename=file_obj.name,
                import_settings=validated.get("import_settings"),
            )
        except DataImportError as exc:
            logger.warning("Import failed: %s [%s] %s", exc.message, exc.code, exc.details)
            return _error(
                {"code": exc.code, "message": exc.message, "details": exc.details},
                http_status=status.HTTP_400_BAD_REQUEST,
            )

        out = ImportResultSerializer(result).data
        return _success(
            data=out,
            warnings=result.warnings,
            meta={
                "duration_ms": result.duration_ms,
                "format": file_obj.name.rsplit(".", 1)[-1].lower(),
                "rows": result.row_count,
                "cols": result.column_count,
            },
            http_status=status.HTTP_201_CREATED,
        )


class DatasetListView(generics.ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = DatasetListSerializer

    def get_queryset(self):
        return Dataset.objects.filter(owner=self.request.user).order_by("-created_at")

    def list(self, request, *args, **kwargs):
        qs = self.get_queryset()
        serializer = self.get_serializer(qs, many=True)
        return _success(data=serializer.data)


class DatasetDetailView(generics.RetrieveAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = DatasetDetailSerializer
    lookup_field = "pk"

    def get_queryset(self):
        return Dataset.objects.filter(owner=self.request.user)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        return _success(data=serializer.data)


class SnapshotPreviewView(generics.RetrieveAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = SnapshotPreviewSerializer
    lookup_field = "pk"

    def get_queryset(self):
        return Snapshot.objects.filter(
            dataset__owner=self.request.user
        ).select_related("dataset").prefetch_related("column_metadata", "validations")

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        return _success(data=serializer.data)


# ────────────────────────── Stage 6: Clean / Transform ──────────────


def _get_user_snapshot(user, pk):
    return Snapshot.objects.select_related("dataset").prefetch_related(
        "column_metadata", "validations",
    ).get(pk=pk, dataset__owner=user)


class SnapshotCleanView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        ser = SnapshotCleanSerializer(data=request.data)
        if not ser.is_valid():
            return _error(
                [{"field": k, "messages": v} for k, v in ser.errors.items()],
            )

        try:
            result = run_pipeline(
                snapshot=snapshot,
                operations=ser.validated_data["operations"],
                pipeline_type="clean",
                preview_only=ser.validated_data["preview_only"],
            )
        except DataCleanError as exc:
            return _error(
                {"code": exc.code, "message": exc.message, "details": exc.details},
            )

        out = PipelineResultSerializer(result).data
        http_code = status.HTTP_200_OK if ser.validated_data["preview_only"] else status.HTTP_201_CREATED
        return _success(
            data=out,
            warnings=result.warnings,
            meta={"duration_ms": result.duration_ms},
            http_status=http_code,
        )


class SnapshotTransformView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        ser = SnapshotTransformSerializer(data=request.data)
        if not ser.is_valid():
            return _error(
                [{"field": k, "messages": v} for k, v in ser.errors.items()],
            )

        try:
            result = run_pipeline(
                snapshot=snapshot,
                operations=ser.validated_data["operations"],
                pipeline_type="transform",
                preview_only=ser.validated_data["preview_only"],
            )
        except DataCleanError as exc:
            return _error(
                {"code": exc.code, "message": exc.message, "details": exc.details},
            )

        out = PipelineResultSerializer(result).data
        http_code = status.HTTP_200_OK if ser.validated_data["preview_only"] else status.HTTP_201_CREATED
        return _success(
            data=out,
            warnings=result.warnings,
            meta={"duration_ms": result.duration_ms},
            http_status=http_code,
        )


class SnapshotPreviewCleanView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        data = request.data.copy() if hasattr(request.data, "copy") else dict(request.data)
        data["preview_only"] = True

        ser = SnapshotCleanSerializer(data=data)
        if not ser.is_valid():
            return _error(
                [{"field": k, "messages": v} for k, v in ser.errors.items()],
            )

        try:
            result = run_pipeline(
                snapshot=snapshot,
                operations=ser.validated_data["operations"],
                pipeline_type="clean",
                preview_only=True,
            )
        except DataCleanError as exc:
            return _error(
                {"code": exc.code, "message": exc.message, "details": exc.details},
            )

        out = PipelineResultSerializer(result).data
        return _success(data=out, warnings=result.warnings)


class SnapshotQualityView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        columns = snapshot.column_metadata.all()
        validations = snapshot.validations.all()

        total_missing = sum(c.missing_count or 0 for c in columns)

        import pandas as pd
        if snapshot.data_json:
            df = pd.DataFrame(snapshot.data_json)
            duplicate_rows = int(df.duplicated().sum())
        else:
            duplicate_rows = 0

        quality_data = {
            "snapshot_id": str(snapshot.pk),
            "stage": snapshot.stage,
            "row_count": snapshot.row_count or 0,
            "column_count": snapshot.column_count or 0,
            "total_missing": total_missing,
            "duplicate_rows": duplicate_rows,
            "columns": [
                {
                    "name": c.name,
                    "inferred_type": c.inferred_type,
                    "missing_count": c.missing_count or 0,
                    "distinct_count": c.distinct_count or 0,
                    "nullable": c.nullable,
                }
                for c in columns
            ],
            "validations": ValidationSerializer(validations, many=True).data,
        }

        return _success(data=quality_data)


class SnapshotHistoryView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        chain = []
        current = snapshot
        while current is not None:
            chain.append(current)
            current = current.parent_snapshot

        chain.reverse()

        serializer = SnapshotHistorySerializer(chain, many=True)
        return _success(data=serializer.data)


class SnapshotSetActiveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            snapshot = _get_user_snapshot(request.user, pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        Snapshot.objects.filter(
            dataset=snapshot.dataset, is_active=True,
        ).update(is_active=False)

        snapshot.is_active = True
        snapshot.save(update_fields=["is_active"])

        return _success(data={"snapshot_id": str(snapshot.pk), "is_active": True})


class SnapshotDiffView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk, other_pk):
        try:
            snap_a = _get_user_snapshot(request.user, pk)
            snap_b = _get_user_snapshot(request.user, other_pk)
        except Snapshot.DoesNotExist:
            return _error("Snapshot not found", http_status=status.HTTP_404_NOT_FOUND)

        cols_a = [c.name for c in snap_a.column_metadata.all()]
        cols_b = [c.name for c in snap_b.column_metadata.all()]
        cols_a_set, cols_b_set = set(cols_a), set(cols_b)

        missing_a = {c.name: c.missing_count or 0 for c in snap_a.column_metadata.all()}
        missing_b = {c.name: c.missing_count or 0 for c in snap_b.column_metadata.all()}

        types_a = {c.name: c.inferred_type for c in snap_a.column_metadata.all()}
        types_b = {c.name: c.inferred_type for c in snap_b.column_metadata.all()}
        type_changes = []
        for col in cols_b_set & cols_a_set:
            if types_a.get(col) != types_b.get(col):
                type_changes.append({
                    "column": col,
                    "type_a": types_a.get(col),
                    "type_b": types_b.get(col),
                })

        diff_data = {
            "snapshot_a": str(snap_a.pk),
            "snapshot_b": str(snap_b.pk),
            "rows_a": snap_a.row_count or 0,
            "rows_b": snap_b.row_count or 0,
            "columns_a": cols_a,
            "columns_b": cols_b,
            "columns_added": list(cols_b_set - cols_a_set),
            "columns_removed": list(cols_a_set - cols_b_set),
            "missing_a": missing_a,
            "missing_b": missing_b,
            "type_changes": type_changes,
        }

        return _success(data=diff_data)
