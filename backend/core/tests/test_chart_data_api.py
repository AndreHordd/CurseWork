from __future__ import annotations

import json
import uuid

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse

from core.models import Chart

User = get_user_model()

pytestmark = pytest.mark.django_db


# ── Chart CRUD ───────────────────────────────────────────────────────


class TestChartCreate:
    def test_create_bar_chart(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "title": "Revenue by Region",
            "x": "region",
            "y": ["revenue"],
            "aggregation": "sum",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        data = resp.json()["data"]
        assert data["chart_type"] == "bar"
        assert data["x"] == "region"
        assert data["y"] == ["revenue"]
        assert data["aggregation"] == "sum"

    def test_create_line_chart(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "line",
            "title": "Revenue over Time",
            "x": "date",
            "y": ["revenue", "cost"],
            "aggregation": "avg",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        data = resp.json()["data"]
        assert data["y"] == ["revenue", "cost"]

    def test_create_table_chart(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "table",
            "title": "Raw Data",
            "y": ["region", "product", "revenue"],
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")

        assert resp.status_code == 201
        data = resp.json()["data"]
        assert data["chart_type"] == "table"

    def test_create_bar_without_aggregation_fails(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "y": ["revenue"],
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 400

    def test_create_bar_without_y_fails(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "aggregation": "sum",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 400

    def test_create_nonexistent_column_fails(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "nonexistent",
            "y": ["revenue"],
            "aggregation": "sum",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 400

    def test_create_avg_on_text_column_fails(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "y": ["product"],
            "aggregation": "avg",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 400

    def test_create_dashboard_not_found(self, auth_client):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(uuid.uuid4()),
            "chart_type": "bar",
            "x": "region",
            "y": ["revenue"],
            "aggregation": "sum",
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 404

    def test_create_with_filters(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "y": ["revenue"],
            "aggregation": "sum",
            "filters": [{"column": "region", "operator": "eq", "value": "UA"}],
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 201

    def test_create_with_group_by(self, auth_client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "y": ["revenue"],
            "aggregation": "sum",
            "group_by": ["product"],
        }
        resp = auth_client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code == 201

    def test_create_requires_auth(self, client, dashboard):
        url = reverse("chart-list-create")
        payload = {
            "dashboard_id": str(dashboard.pk),
            "chart_type": "bar",
            "x": "region",
            "y": ["revenue"],
            "aggregation": "sum",
        }
        resp = client.post(url, data=json.dumps(payload), content_type="application/json")
        assert resp.status_code in (401, 403)


class TestChartList:
    def test_list_all(self, auth_client, bar_chart, line_chart):
        url = reverse("chart-list-create")
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data) == 2

    def test_list_by_dashboard(self, auth_client, dashboard, bar_chart, line_chart):
        url = reverse("chart-list-create") + f"?dashboard_id={dashboard.pk}"
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data) == 2


class TestChartDetail:
    def test_get_detail(self, auth_client, bar_chart):
        url = reverse("chart-detail", args=[bar_chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["chart_type"] == "bar"
        assert data["title"] == "Revenue by Region"

    def test_get_not_found(self, auth_client):
        url = reverse("chart-detail", args=[uuid.uuid4()])
        resp = auth_client.get(url)
        assert resp.status_code == 404


class TestChartUpdate:
    def test_patch_title(self, auth_client, bar_chart):
        url = reverse("chart-detail", args=[bar_chart.pk])
        resp = auth_client.patch(
            url,
            data=json.dumps({"title": "Updated Chart"}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.json()["data"]["title"] == "Updated Chart"

    def test_patch_aggregation(self, auth_client, bar_chart):
        url = reverse("chart-detail", args=[bar_chart.pk])
        resp = auth_client.patch(
            url,
            data=json.dumps({"aggregation": "avg"}),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.json()["data"]["aggregation"] == "avg"


class TestChartDelete:
    def test_delete_ok(self, auth_client, bar_chart):
        url = reverse("chart-detail", args=[bar_chart.pk])
        resp = auth_client.delete(url)

        assert resp.status_code == 200
        assert not Chart.objects.filter(pk=bar_chart.pk).exists()


# ── Chart Data ───────────────────────────────────────────────────────


class TestChartData:
    def test_bar_chart_data(self, auth_client, bar_chart):
        url = reverse("chart-data", args=[bar_chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["chart_type"] == "bar"
        assert "labels" in data
        assert "datasets" in data
        assert len(data["datasets"]) == 1
        assert data["datasets"][0]["label"] == "revenue"
        assert len(data["labels"]) == len(data["datasets"][0]["data"])
        assert "meta" in data

    def test_line_chart_data(self, auth_client, line_chart):
        url = reverse("chart-data", args=[line_chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["chart_type"] == "line"
        assert "labels" in data
        assert "datasets" in data

    def test_table_chart_data(self, auth_client, table_chart):
        url = reverse("chart-data", args=[table_chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["chart_type"] == "table"
        assert "columns" in data
        assert "rows" in data
        assert len(data["rows"]) == 6

    def test_chart_data_meta(self, auth_client, bar_chart):
        url = reverse("chart-data", args=[bar_chart.pk])
        resp = auth_client.get(url)

        meta = resp.json()["data"]["meta"]
        assert meta["source_row_count"] == 6
        assert "filtered_row_count" in meta
        assert "aggregation" in meta
        assert "duration_ms" in meta

    def test_chart_data_with_global_filters(self, auth_client, dashboard, bar_chart):
        dashboard.global_filters = {
            "conditions": [{"column": "region", "operator": "eq", "value": "UA"}],
            "logic": "and",
        }
        dashboard.save()

        url = reverse("chart-data", args=[bar_chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        meta = data["meta"]
        assert meta["filtered_row_count"] < meta["source_row_count"]

    def test_chart_data_with_local_filters(self, auth_client, dashboard):
        from core.models import Chart
        chart = Chart.objects.create(
            dashboard=dashboard,
            chart_type="bar",
            title="Filtered Chart",
            x="region",
            y=["revenue"],
            aggregation="sum",
            filters={
                "conditions": [{"column": "product", "operator": "eq", "value": "A"}],
                "logic": "and",
            },
        )

        url = reverse("chart-data", args=[chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        meta = resp.json()["data"]["meta"]
        assert meta["filtered_row_count"] < meta["source_row_count"]

    def test_chart_data_not_found(self, auth_client):
        url = reverse("chart-data", args=[uuid.uuid4()])
        resp = auth_client.get(url)
        assert resp.status_code == 404

    def test_chart_data_requires_auth(self, client, bar_chart):
        url = reverse("chart-data", args=[bar_chart.pk])
        resp = client.get(url)
        assert resp.status_code in (401, 403)

    def test_bar_data_multiple_y(self, auth_client, dashboard):
        from core.models import Chart
        chart = Chart.objects.create(
            dashboard=dashboard,
            chart_type="bar",
            title="Multi Y",
            x="region",
            y=["revenue", "cost"],
            aggregation="sum",
        )

        url = reverse("chart-data", args=[chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data["datasets"]) == 2
        assert data["datasets"][0]["label"] == "revenue"
        assert data["datasets"][1]["label"] == "cost"

    def test_table_with_aggregation(self, auth_client, dashboard):
        from core.models import Chart
        chart = Chart.objects.create(
            dashboard=dashboard,
            chart_type="table",
            title="Aggregated Table",
            x=None,
            y=["revenue"],
            aggregation="sum",
            group_by=["region"],
        )

        url = reverse("chart-data", args=[chart.pk])
        resp = auth_client.get(url)

        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["chart_type"] == "table"
        assert "columns" in data
        assert "rows" in data
        assert len(data["rows"]) <= 3

    def test_all_aggregation_types(self, auth_client, dashboard):
        from core.models import Chart

        for agg in ("count", "sum", "avg", "min", "max", "median"):
            chart = Chart.objects.create(
                dashboard=dashboard,
                chart_type="bar",
                title=f"Agg {agg}",
                x="region",
                y=["revenue"],
                aggregation=agg,
            )
            url = reverse("chart-data", args=[chart.pk])
            resp = auth_client.get(url)

            assert resp.status_code == 200, f"Failed for aggregation={agg}"
            data = resp.json()["data"]
            assert len(data["datasets"]) == 1
            chart.delete()

    def test_chart_data_other_user_forbidden(self, auth_client, bar_chart):
        other = User.objects.create_user(username="other4", password="pass")
        other_client = auth_client.__class__()
        other_client.force_login(other)

        url = reverse("chart-data", args=[bar_chart.pk])
        resp = other_client.get(url)
        assert resp.status_code == 404
