from __future__ import annotations

import json
import uuid

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse

from core.models import Dashboard

User = get_user_model()

pytestmark = pytest.mark.django_db


class TestDashboardCreate:
    def test_create_ok(self, auth_client, snapshot_with_data):
        url = reverse("dashboard-list-create")
        payload = {
            "snapshot_id": str(snapshot_with_data.pk),
            "title": "My Dashboard",
            "description": "Test desc",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        body = resp.json()
        assert body["success"] is True
        data = body["data"]
        assert data["title"] == "My Dashboard"
        assert data["snapshot_id"] == str(snapshot_with_data.pk)
        assert data["charts"] == []

    def test_create_with_global_filters(self, auth_client, snapshot_with_data):
        url = reverse("dashboard-list-create")
        payload = {
            "snapshot_id": str(snapshot_with_data.pk),
            "title": "Filtered Dashboard",
            "global_filters": {
                "conditions": [
                    {"column": "region", "operator": "eq", "value": "UA"}
                ],
                "logic": "and",
            },
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        data = resp.json()["data"]
        assert data["global_filters"]["conditions"][0]["column"] == "region"

    def test_create_with_layout(self, auth_client, snapshot_with_data):
        url = reverse("dashboard-list-create")
        payload = {
            "snapshot_id": str(snapshot_with_data.pk),
            "title": "Layout Dashboard",
            "layout": {"columns": 2, "rows": 3},
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        data = resp.json()["data"]
        assert data["layout"]["columns"] == 2

    def test_create_snapshot_not_found(self, auth_client):
        url = reverse("dashboard-list-create")
        payload = {
            "snapshot_id": str(uuid.uuid4()),
            "title": "Bad Dashboard",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 404

    def test_create_missing_title(self, auth_client, snapshot_with_data):
        url = reverse("dashboard-list-create")
        payload = {"snapshot_id": str(snapshot_with_data.pk)}
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 400

    def test_create_requires_auth(self, client, snapshot_with_data):
        url = reverse("dashboard-list-create")
        payload = {
            "snapshot_id": str(snapshot_with_data.pk),
            "title": "No auth",
        }
        resp = client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code in (401, 403)


class TestDashboardList:
    def test_list_empty(self, auth_client):
        url = reverse("dashboard-list-create")
        resp = auth_client.get(url)

        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_list_with_dashboards(self, auth_client, dashboard):
        url = reverse("dashboard-list-create")
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data) == 1
        assert data[0]["title"] == "Test Dashboard"
        assert "chart_count" in data[0]

    def test_list_only_own(self, auth_client, dashboard):
        other = User.objects.create_user(username="other2", password="pass")
        from core.tests.conftest import _create_snapshot_with_data
        _, snap2 = _create_snapshot_with_data(other)
        Dashboard.objects.create(owner=other, snapshot=snap2, title="Other Dashboard")

        url = reverse("dashboard-list-create")
        resp = auth_client.get(url)

        data = resp.json()["data"]
        assert len(data) == 1
        assert data[0]["title"] == "Test Dashboard"


class TestDashboardDetail:
    def test_get_detail(self, auth_client, dashboard):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["title"] == "Test Dashboard"
        assert "charts" in data

    def test_get_detail_with_charts(self, auth_client, dashboard, bar_chart, line_chart):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.get(url)

        data = resp.json()["data"]
        assert len(data["charts"]) == 2

    def test_get_not_found(self, auth_client):
        url = reverse("dashboard-detail", args=[uuid.uuid4()])
        resp = auth_client.get(url)
        assert resp.status_code == 404

    def test_other_user_not_found(self, auth_client, dashboard):
        other = User.objects.create_user(username="other3", password="pass")
        other_client = auth_client.__class__()
        other_client.force_login(other)

        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = other_client.get(url)
        assert resp.status_code == 404


class TestDashboardUpdate:
    def test_patch_title(self, auth_client, dashboard):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.patch(
            url,
            data=json.dumps({"title": "Updated Title"}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.json()["data"]["title"] == "Updated Title"

    def test_patch_layout(self, auth_client, dashboard):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.patch(
            url,
            data=json.dumps({"layout": {"cols": 3}}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.json()["data"]["layout"]["cols"] == 3

    def test_patch_global_filters(self, auth_client, dashboard):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        filters = {
            "conditions": [{"column": "region", "operator": "eq", "value": "PL"}],
            "logic": "and",
        }
        resp = auth_client.patch(
            url,
            data=json.dumps({"global_filters": filters}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["global_filters"]["conditions"][0]["value"] == "PL"


class TestDashboardDelete:
    def test_delete_ok(self, auth_client, dashboard):
        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.delete(url)

        assert resp.status_code == 200
        assert resp.json()["data"]["deleted"] is True
        assert not Dashboard.objects.filter(pk=dashboard.pk).exists()

    def test_delete_cascades_charts(self, auth_client, dashboard, bar_chart, line_chart):
        from core.models import Chart
        assert Chart.objects.filter(dashboard=dashboard).count() == 2

        url = reverse("dashboard-detail", args=[dashboard.pk])
        resp = auth_client.delete(url)

        assert resp.status_code == 200
        assert Chart.objects.filter(dashboard=dashboard).count() == 0

    def test_delete_not_found(self, auth_client):
        url = reverse("dashboard-detail", args=[uuid.uuid4()])
        resp = auth_client.delete(url)
        assert resp.status_code == 404
