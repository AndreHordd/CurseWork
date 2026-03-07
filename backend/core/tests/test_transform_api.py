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


def _transform(client, snapshot_id, operations, preview_only=False):
    return client.post(
        f"/api/v1/snapshots/{snapshot_id}/transform/",
        json.dumps({"operations": operations, "preview_only": preview_only}),
        content_type="application/json",
    )


class TestTransformFilterAPI:
    def test_filter_rows(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "status", "amount"],
            ["1", "paid", "100"],
            ["2", "pending", "200"],
            ["3", "paid", "300"],
        ], name="Filter Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _transform(auth_client, snap_id, [
            {"operation": "filter_rows", "params": {
                "conditions": [{"column": "status", "operator": "eq", "value": "paid"}],
            }},
        ])
        assert resp2.status_code == 201
        assert resp2.json()["data"]["row_count"] == 2
        assert resp2.json()["data"]["stage"] == "transformed"


class TestTransformDerivedAPI:
    def test_add_column(self, auth_client):
        resp = _import_csv(auth_client, [
            ["price", "qty"],
            ["100", "2"],
            ["200", "3"],
        ], name="Derived Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _transform(auth_client, snap_id, [
            {"operation": "add_arithmetic_column", "params": {
                "new_name": "total",
                "expression": "price * qty",
            }},
        ])
        assert resp2.status_code == 201
        assert resp2.json()["data"]["column_count"] == 3


class TestTransformAggregationAPI:
    def test_groupby(self, auth_client):
        resp = _import_csv(auth_client, [
            ["city", "revenue"],
            ["NYC", "100"],
            ["LA", "200"],
            ["NYC", "150"],
            ["LA", "250"],
        ], name="Agg Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _transform(auth_client, snap_id, [
            {"operation": "aggregate", "params": {
                "group_by": ["city"],
                "agg_config": {"revenue": ["sum", "avg"]},
            }},
        ])
        assert resp2.status_code == 201
        assert resp2.json()["data"]["row_count"] == 2


class TestTransformStructureAPI:
    def test_rename_and_select(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "Bob", "20"],
        ], name="Structure Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _transform(auth_client, snap_id, [
            {"operation": "rename_columns", "params": {"mapping": {"name": "full_name"}}},
        ])
        assert resp2.status_code == 201


class TestDiffAPI:
    def test_diff_two_snapshots(self, auth_client):
        resp = _import_csv(auth_client, [
            ["id", "name", "value"],
            ["1", "Alice", "10"],
            ["2", "", ""],
            ["3", "Carol", "30"],
        ], name="Diff Test")
        snap_id = resp.json()["data"]["snapshot_id"]

        resp2 = _clean(auth_client, snap_id, [
            {"operation": "fill_missing", "params": {"strategy": "drop"}},
        ])
        new_snap_id = resp2.json()["data"]["snapshot_id"]

        resp3 = auth_client.get(f"/api/v1/snapshots/{snap_id}/diff/{new_snap_id}/")
        assert resp3.status_code == 200
        diff = resp3.json()["data"]
        assert diff["rows_a"] == 3
        assert diff["rows_b"] == 2


class TestEndToEnd:
    """Full scenario: import -> clean missing -> drop duplicates -> cast date."""

    def test_full_pipeline(self, auth_client):
        resp = _import_csv(auth_client, [
            ["user_id", "event_date", "amount", "status"],
            ["1", "2026-01-01", "100", "paid"],
            ["2", "2026-02-15", "", "pending"],
            ["1", "2026-01-01", "100", "paid"],
            ["3", "2026-03-20", "300", "paid"],
        ], name="E2E Test")
        assert resp.status_code == 201
        raw_snap_id = resp.json()["data"]["snapshot_id"]

        # Step 1: Fill missing values
        resp2 = _clean(auth_client, raw_snap_id, [
            {"operation": "fill_missing", "params": {"columns": ["amount"], "strategy": "constant", "fill_value": "0"}},
        ])
        assert resp2.status_code == 201
        clean1_id = resp2.json()["data"]["snapshot_id"]

        # Step 2: Drop duplicates
        resp3 = _clean(auth_client, clean1_id, [
            {"operation": "drop_duplicates", "params": {"keep": "first"}},
        ])
        assert resp3.status_code == 201
        clean2_id = resp3.json()["data"]["snapshot_id"]
        assert resp3.json()["data"]["row_count"] == 3

        # Step 3: Cast date
        resp4 = _clean(auth_client, clean2_id, [
            {"operation": "cast_type", "params": {"column": "event_date", "target_type": "date"}},
        ])
        assert resp4.status_code == 201
        clean3_id = resp4.json()["data"]["snapshot_id"]

        date_meta = ColumnMetadata.objects.get(snapshot_id=clean3_id, name="event_date")
        assert date_meta.inferred_type == "date"

        # Verify the full chain
        resp5 = auth_client.get(f"/api/v1/snapshots/{clean3_id}/history/")
        assert resp5.status_code == 200
        chain = resp5.json()["data"]
        assert len(chain) == 4
        assert chain[0]["stage"] == "raw"
        assert chain[1]["stage"] == "cleaned"
        assert chain[2]["stage"] == "cleaned"
        assert chain[3]["stage"] == "cleaned"

        # Verify old snapshots are deactivated
        for snap_id in [raw_snap_id, clean1_id, clean2_id]:
            snap = Snapshot.objects.get(pk=snap_id)
            assert snap.is_active is False

        final_snap = Snapshot.objects.get(pk=clean3_id)
        assert final_snap.is_active is True

        # Step 4: Transform - filter by status
        resp6 = _transform(auth_client, clean3_id, [
            {"operation": "filter_rows", "params": {
                "conditions": [{"column": "status", "operator": "eq", "value": "paid"}],
            }},
        ])
        assert resp6.status_code == 201
        assert resp6.json()["data"]["row_count"] == 2
        assert resp6.json()["data"]["stage"] == "transformed"

        # Full chain is now 5 snapshots
        final_id = resp6.json()["data"]["snapshot_id"]
        resp7 = auth_client.get(f"/api/v1/snapshots/{final_id}/history/")
        chain = resp7.json()["data"]
        assert len(chain) == 5
