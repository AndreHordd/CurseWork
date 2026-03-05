from __future__ import annotations

import io
import json

import openpyxl
import pytest
from django.test import override_settings

from core.models import ColumnMetadata, Dataset, Snapshot, Validation

IMPORT_URL = "/api/v1/datasets/import/"
DATASETS_URL = "/api/v1/datasets/"

pytestmark = pytest.mark.django_db


def _upload(client, buf, name="Test Dataset", import_settings=None):
    data = {"name": name, "file": buf}
    if import_settings is not None:
        data["import_settings"] = json.dumps(import_settings)
    return client.post(IMPORT_URL, data, format="multipart")


class TestCSVImport:
    def test_success(self, auth_client, valid_csv):
        resp = _upload(auth_client, valid_csv)
        assert resp.status_code == 201
        body = resp.json()
        assert body["success"] is True
        d = body["data"]
        assert d["row_count"] == 3
        assert d["column_count"] == 4
        assert len(d["preview_rows"]) == 3

        assert Dataset.objects.count() == 1
        assert Snapshot.objects.count() == 1
        snap = Snapshot.objects.first()
        assert snap.stage == "raw"
        assert snap.is_active is True
        assert ColumnMetadata.objects.filter(snapshot=snap).count() == 4
        assert Validation.objects.filter(snapshot=snap).count() >= 3


class TestXLSXImport:
    def test_success(self, auth_client, valid_xlsx):
        resp = _upload(auth_client, valid_xlsx, name="XLSX Dataset")
        assert resp.status_code == 201
        body = resp.json()
        assert body["data"]["row_count"] == 2


class TestJSONImport:
    def test_success(self, auth_client, valid_json):
        resp = _upload(auth_client, valid_json, name="JSON Dataset")
        assert resp.status_code == 201
        body = resp.json()
        assert body["data"]["row_count"] == 2


class TestImportErrors:
    def test_unsupported_extension(self, auth_client):
        buf = io.BytesIO(b"data")
        buf.name = "file.txt"
        resp = _upload(auth_client, buf)
        assert resp.status_code == 400

    @override_settings(IMPORT_MAX_FILE_SIZE_MB=0)
    def test_file_too_large(self, auth_client, valid_csv):
        resp = _upload(auth_client, valid_csv)
        assert resp.status_code == 400

    def test_empty_csv(self, auth_client, empty_csv):
        resp = _upload(auth_client, empty_csv, name="Empty")
        assert resp.status_code == 400
        assert Dataset.objects.count() == 0

    def test_unauthenticated(self, client, valid_csv):
        resp = _upload(client, valid_csv)
        assert resp.status_code == 403


class TestDuplicatesAndMissing:
    def test_duplicates_import_with_warn(self, auth_client, csv_with_duplicates):
        resp = _upload(auth_client, csv_with_duplicates, name="Dups")
        assert resp.status_code == 201
        snap = Snapshot.objects.first()
        dup_rule = Validation.objects.filter(
            snapshot=snap, rule_name="duplicate_rows"
        ).first()
        assert dup_rule is not None
        assert dup_rule.status == "warn"
        assert dup_rule.failed_count == 1

    def test_missing_values_import_with_warn(self, auth_client, csv_with_missing):
        resp = _upload(auth_client, csv_with_missing, name="Missing")
        assert resp.status_code == 201
        snap = Snapshot.objects.first()
        missing_rules = Validation.objects.filter(
            snapshot=snap,
            rule_name__startswith="missing_values:",
        )
        assert missing_rules.exists()
        for rule in missing_rules:
            assert rule.status == "warn"


class TestDatasetListAndDetail:
    def test_list_empty(self, auth_client):
        resp = auth_client.get(DATASETS_URL)
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_list_after_import(self, auth_client, valid_csv):
        _upload(auth_client, valid_csv)
        resp = auth_client.get(DATASETS_URL)
        assert resp.status_code == 200
        assert len(resp.json()["data"]) == 1

    def test_detail(self, auth_client, valid_csv):
        imp_resp = _upload(auth_client, valid_csv)
        ds_id = imp_resp.json()["data"]["dataset_id"]
        resp = auth_client.get(f"{DATASETS_URL}{ds_id}/")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["name"] == "Test Dataset"
        assert data["active_snapshot"] is not None


class TestSnapshotPreview:
    def test_preview(self, auth_client, valid_csv):
        imp_resp = _upload(auth_client, valid_csv)
        snap_id = imp_resp.json()["data"]["snapshot_id"]
        resp = auth_client.get(f"/api/v1/snapshots/{snap_id}/preview/")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["row_count"] == 3
        assert len(data["columns"]) == 4
        assert len(data["validations"]) >= 3


class TestTransactionAtomicity:
    def test_failed_import_leaves_no_orphans(self, auth_client, empty_csv):
        _upload(auth_client, empty_csv, name="Fail")
        assert Dataset.objects.count() == 0
        assert Snapshot.objects.count() == 0
        assert ColumnMetadata.objects.count() == 0
        assert Validation.objects.count() == 0
