from __future__ import annotations

import io
import json

import pytest

from core.models import ColumnMetadata, Snapshot, Validation

pytestmark = pytest.mark.django_db

IMPORT_URL = "/api/v1/datasets/import/"


def _import_csv(client, rows, name="Test Dataset"):
    lines = [",".join(str(c) for c in row) for row in rows]
    data = "\n".join(lines).encode()
    buf = io.BytesIO(data)
    buf.name = "test.csv"
    return client.post(IMPORT_URL, {"name": name, "file": buf}, format="multipart")


def _clean(client, snapshot_id, operations, preview_only=False):
    return client.post(
        f"/api/v1/snapshots/{snapshot_id}/clean/",
        json.dumps({"operations": operations, "preview_only": preview_only}),
        content_type="application/json",
    )


class TestSnapshotCleanAPI:
    def test_fill_missing_creates_new_snapshot(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "", ""],
            ["3", "Carol", "30"],
        ])
        assert resp.status_code == 201
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"columns": ["name"], "strategy": "constant", "fill_value": "Unknown"}},
        ])
        assert resp2.status_code == 201
        body = resp2.json()
        assert body["success"] is True
        assert body["data"]["snapshot_id"] is not None
        assert body["data"]["snapshot_id"] != snap_id

        new_snap = Snapshot.objects.get(pk=body["data"]["snapshot_id"])
        assert new_snap.stage == "cleaned"
        assert new_snap.is_active is True
        assert str(new_snap.parent_snapshot_id) == snap_id

    def test_drop_duplicates(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "Bob", "20"],
            ["1", "Alice", "10"],
        ], name="Dup Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "drop_duplicates", "params": {"keep": "first"}},
        ])
        assert resp2.status_code == 201
        assert resp2.json()["data"]["row_count"] == 2

    def test_cast_date_column(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "event_date"],
            ["1", "2026-01-01"],
            ["2", "2026-02-15"],
            ["3", "2026-03-20"],
        ], name="Date Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "cast_type", "params": {"column": "event_date", "target_type": "date"}},
        ])
        assert resp2.status_code == 201

        new_snap_id = resp2.json()["data"]["snapshot_id"]
        meta = ColumnMetadata.objects.get(snapshot_id=new_snap_id, name="event_date")
        assert meta.inferred_type == "date"

    def test_invalid_column_returns_400(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
        ], name="Invalid Col")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"columns": ["nonexistent"], "strategy": "drop"}},
        ])
        assert resp2.status_code == 400
        assert resp2.json()["success"] is False

    def test_unknown_operation_returns_400(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
        ], name="Bad Op")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "does_not_exist", "params": {}},
        ])
        assert resp2.status_code == 400

    def test_preview_does_not_create_snapshot(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
            ["2", ""],
        ], name="Preview Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        count_before = Snapshot.objects.count()

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"strategy": "drop"}},
        ], preview_only=True)
        assert resp2.status_code == 200
        assert resp2.json()["data"]["snapshot_id"] is None
        assert Snapshot.objects.count() == count_before

    def test_old_snapshot_deactivated(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
            ["2", ""],
        ], name="Deactivate Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"strategy": "drop"}},
        ])

        old_snap = Snapshot.objects.get(pk=snap_id)
        assert old_snap.is_active is False

    def test_metadata_and_validations_updated(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "", ""],
            ["3", "Carol", "30"],
        ], name="Meta Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"strategy": "drop"}},
        ])
        new_snap_id = resp2.json()["data"]["snapshot_id"]

        assert ColumnMetadata.objects.filter(snapshot_id=new_snap_id).count() > 0
        assert Validation.objects.filter(snapshot_id=new_snap_id).count() > 0

    def test_step_config_saved(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
        ], name="Config Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "trim_spaces", "params": {}},
        ])
        new_snap_id = resp2.json()["data"]["snapshot_id"]
        new_snap = Snapshot.objects.get(pk=new_snap_id)
        assert new_snap.step_config is not None
        assert new_snap.step_config["pipeline_type"] == "clean"
        assert len(new_snap.step_config["steps"]) == 1


class TestPreviewCleanAPI:
    def test_preview_clean_endpoint(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
            ["2", ""],
        ], name="Preview Endpoint")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = auth_client.post(
            f"/api/v1/snapshots/{snap_id}/preview-clean/",
            json.dumps({"operations": [
                {"operation": "fill_missing", "params": {"strategy": "drop"}},
            ]}),
            content_type="application/json",
        )
        assert resp2.status_code == 200
        assert resp2.json()["data"]["snapshot_id"] is None


class TestQualityAPI:
    def test_quality_endpoint(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "", ""],
            ["3", "Carol", "30"],
        ], name="Quality Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = auth_client.get(f"/api/v1/snapshots/{snap_id}/quality/")
        assert resp2.status_code == 200
        data = resp2.json()["data"]
        assert data["row_count"] == 3
        assert data["total_missing"] >= 0
        assert len(data["columns"]) > 0


class TestHistoryAPI:
    def test_history_returns_chain(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
            ["2", ""],
        ], name="History Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"strategy": "drop"}},
        ])
        new_snap_id = resp2.json()["data"]["snapshot_id"]

        resp3 = auth_client.get(f"/api/v1/snapshots/{new_snap_id}/history/")
        assert resp3.status_code == 200
        chain = resp3.json()["data"]
        assert len(chain) == 2
        assert chain[0]["stage"] == "raw"
        assert chain[1]["stage"] == "cleaned"


class TestSetActiveAPI:
    def test_set_active(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "value"],
            ["1", "10"],
        ], name="Active Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "trim_spaces", "params": {}},
        ])
        new_snap_id = resp2.json()["data"]["snapshot_id"]

        resp3 = auth_client.post(
            f"/api/v1/snapshots/{snap_id}/set-active/",
            content_type="application/json",
        )
        assert resp3.status_code == 200

        old_snap = Snapshot.objects.get(pk=snap_id)
        assert old_snap.is_active is True
        new_snap = Snapshot.objects.get(pk=new_snap_id)
        assert new_snap.is_active is False
