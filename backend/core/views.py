from __future__ import annotations

import logging
import time

from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.exceptions import DataImportError
from core.models import Dataset, Snapshot
from core.serializers import (
    DatasetDetailSerializer,
    DatasetImportSerializer,
    DatasetListSerializer,
    ImportResultSerializer,
    SnapshotPreviewSerializer,
)
from core.services.import_service import import_dataset

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
