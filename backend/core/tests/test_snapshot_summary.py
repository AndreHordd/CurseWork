from __future__ import annotations

import json

import pandas as pd
import pytest
from django.urls import reverse

from core.services.analytics.summary import compute_summary
from core.tests.conftest import ANALYTICS_DATA


pytestmark = pytest.mark.django_db


# ── Unit tests: compute_summary ──────────────────────────────────────


class TestComputeSummary:
    def test_basic_counts(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)

        assert result["snapshot_id"] == str(snapshot_with_data.pk)
        assert result["row_count"] == 6
        assert result["column_count"] == 5

    def test_numeric_columns_present(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        numeric_names = [c["name"] for c in result["numeric_columns"]]
        assert "revenue" in numeric_names
        assert "cost" in numeric_names

    def test_numeric_stats(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        revenue = next(c for c in result["numeric_columns"] if c["name"] == "revenue")
        stats = revenue["stats"]

        assert stats["min"] == 100
        assert stats["max"] == 400
        assert stats["range"] == 300
        assert "mean" in stats
        assert "median" in stats
        assert "std" in stats

    def test_categorical_columns(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        cat_names = [c["name"] for c in result["categorical_columns"]]
        assert "region" in cat_names or "product" in cat_names

    def test_categorical_top_values(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        for cat_col in result["categorical_columns"]:
            if cat_col["name"] == "region":
                assert "top_values" in cat_col
                assert isinstance(cat_col["top_values"], dict)
                assert len(cat_col["top_values"]) > 0

    def test_duplicate_rows(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        assert result["duplicate_rows"] == 0

    def test_missing_total(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        assert result["missing_total"] == 0

    def test_quality_summary_block(self, snapshot_with_data):
        result = compute_summary(snapshot_with_data)
        qs = result["quality_summary"]
        assert "total_missing" in qs
        assert "missing_percentage" in qs
        assert "numeric_count" in qs
        assert "categorical_count" in qs
        assert "datetime_count" in qs

    def test_with_missing_data(self, user):
        from core.tests.conftest import _create_snapshot_with_data

        data = [
            {"a": 1, "b": "x"},
            {"a": None, "b": "y"},
            {"a": 3, "b": None},
        ]
        _, snap = _create_snapshot_with_data(user, data)
        result = compute_summary(snap)

        assert result["row_count"] == 3
        assert result["missing_total"] > 0

    def test_with_duplicates(self, user):
        from core.tests.conftest import _create_snapshot_with_data

        data = [
            {"x": 1, "y": "a"},
            {"x": 1, "y": "a"},
            {"x": 2, "y": "b"},
        ]
        _, snap = _create_snapshot_with_data(user, data)
        result = compute_summary(snap)

        assert result["duplicate_rows"] == 1


# ── Integration test: GET /api/v1/snapshots/{id}/summary/ ───────────


class TestSnapshotSummaryAPI:
    def test_get_summary_ok(self, auth_client, snapshot_with_data):
        url = reverse("snapshot-summary", args=[snapshot_with_data.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        data = body["data"]
        assert data["row_count"] == 6
        assert data["column_count"] == 5
        assert len(data["numeric_columns"]) >= 2
        assert "quality_summary" in data

    def test_summary_not_found(self, auth_client):
        import uuid
        url = reverse("snapshot-summary", args=[uuid.uuid4()])
        resp = auth_client.get(url)

        assert resp.status_code == 404

    def test_summary_requires_auth(self, client, snapshot_with_data):
        url = reverse("snapshot-summary", args=[snapshot_with_data.pk])
        resp = client.get(url)

        assert resp.status_code in (401, 403)

    def test_summary_other_user_forbidden(self, auth_client, snapshot_with_data):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        other = User.objects.create_user(username="other", password="pass123")
        other_client = auth_client.__class__()
        other_client.force_login(other)

        url = reverse("snapshot-summary", args=[snapshot_with_data.pk])
        resp = other_client.get(url)

        assert resp.status_code == 404
