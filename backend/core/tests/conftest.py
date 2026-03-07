from __future__ import annotations

import io
import json

import openpyxl
import pandas as pd
import pytest
from django.contrib.auth import get_user_model

User = get_user_model()


@pytest.fixture()
def user(db):
    return User.objects.create_user(username="tester", password="testpass123")


@pytest.fixture()
def auth_client(client, user):
    client.force_login(user)
    return client


# ──────────────── file fixtures ────────────────


def _csv_bytes(rows: list[list], delimiter=",", encoding="utf-8") -> bytes:
    lines = [delimiter.join(str(c) for c in row) for row in rows]
    return "\n".join(lines).encode(encoding)


@pytest.fixture()
def valid_csv() -> io.BytesIO:
    data = _csv_bytes([
        ["id", "name", "value", "active"],
        ["1", "Alice", "10.5", "true"],
        ["2", "Bob", "20.3", "false"],
        ["3", "Carol", "30.1", "true"],
    ])
    buf = io.BytesIO(data)
    buf.name = "valid.csv"
    return buf


@pytest.fixture()
def valid_xlsx() -> io.BytesIO:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name", "value"])
    ws.append([1, "Alice", 10.5])
    ws.append([2, "Bob", 20.3])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    buf.name = "valid.xlsx"
    return buf


@pytest.fixture()
def valid_json() -> io.BytesIO:
    data = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 20.3},
    ]
    buf = io.BytesIO(json.dumps(data).encode())
    buf.name = "valid.json"
    return buf


@pytest.fixture()
def empty_csv() -> io.BytesIO:
    buf = io.BytesIO(b"id,name,value\n")
    buf.name = "empty.csv"
    return buf


@pytest.fixture()
def malformed_csv() -> io.BytesIO:
    buf = io.BytesIO(b"\x80\x81\x82\xff\xfe")
    buf.name = "malformed.csv"
    return buf


@pytest.fixture()
def csv_with_duplicates() -> io.BytesIO:
    data = _csv_bytes([
        ["id", "name", "value"],
        ["1", "Alice", "10"],
        ["2", "Bob", "20"],
        ["1", "Alice", "10"],
    ])
    buf = io.BytesIO(data)
    buf.name = "duplicates.csv"
    return buf


@pytest.fixture()
def csv_with_missing() -> io.BytesIO:
    data = _csv_bytes([
        ["id", "name", "value"],
        ["1", "Alice", "10"],
        ["2", "", ""],
        ["3", "Carol", "30"],
    ])
    buf = io.BytesIO(data)
    buf.name = "missing.csv"
    return buf


@pytest.fixture()
def csv_with_duplicate_columns() -> io.BytesIO:
    data = b"name,name,value\nAlice,A,10\nBob,B,20\n"
    buf = io.BytesIO(data)
    buf.name = "dupcols.csv"
    return buf


# ──────────────── Stage 7: analytics fixtures ────────────────


ANALYTICS_DATA = [
    {"region": "UA", "product": "A", "revenue": 100, "cost": 40, "date": "2025-01-01"},
    {"region": "UA", "product": "B", "revenue": 200, "cost": 80, "date": "2025-01-02"},
    {"region": "PL", "product": "A", "revenue": 150, "cost": 60, "date": "2025-01-03"},
    {"region": "PL", "product": "B", "revenue": 300, "cost": 120, "date": "2025-01-04"},
    {"region": "UA", "product": "A", "revenue": 120, "cost": 50, "date": "2025-02-01"},
    {"region": "DE", "product": "C", "revenue": 400, "cost": 200, "date": "2025-02-02"},
]


def _create_snapshot_with_data(user, data=None):
    """Helper: create Dataset + Snapshot + ColumnMetadata from raw dicts."""
    from core.models import ColumnMetadata, Dataset, Snapshot
    from core.services.profiler import profile_dataframe

    data = data or ANALYTICS_DATA
    df = pd.DataFrame(data)
    profile = profile_dataframe(df)

    dataset = Dataset.objects.create(
        owner=user,
        name=f"analytics_ds_{Dataset.objects.count()}",
        source_type="file",
        file_format="csv",
    )
    snapshot = Snapshot.objects.create(
        dataset=dataset,
        stage="transformed",
        is_active=True,
        is_ready_for_analysis=True,
        storage_type="jsonb",
        data_json=json.loads(df.to_json(orient="records")),
        preview_rows=json.loads(df.head(5).to_json(orient="records", date_format="iso")),
        row_count=len(df),
        column_count=len(df.columns),
    )
    for col_prof in profile.columns:
        ColumnMetadata.objects.create(
            snapshot=snapshot,
            name=col_prof.name,
            inferred_type=col_prof.inferred_type,
            nullable=col_prof.nullable,
            distinct_count=col_prof.distinct_count,
            missing_count=col_prof.missing_count,
            stats=col_prof.stats,
        )
    return dataset, snapshot


@pytest.fixture()
def snapshot_with_data(user):
    """Dataset + Snapshot with analytics data and ColumnMetadata."""
    _ds, snap = _create_snapshot_with_data(user)
    return snap


@pytest.fixture()
def dashboard(user, snapshot_with_data):
    """Dashboard linked to snapshot_with_data."""
    from core.models import Dashboard
    return Dashboard.objects.create(
        owner=user,
        snapshot=snapshot_with_data,
        title="Test Dashboard",
        description="For testing",
    )


@pytest.fixture()
def bar_chart(dashboard):
    """Bar chart: revenue by region."""
    from core.models import Chart
    return Chart.objects.create(
        dashboard=dashboard,
        chart_type="bar",
        title="Revenue by Region",
        x="region",
        y=["revenue"],
        aggregation="sum",
        group_by=[],
    )


@pytest.fixture()
def line_chart(dashboard):
    """Line chart: revenue by date."""
    from core.models import Chart
    return Chart.objects.create(
        dashboard=dashboard,
        chart_type="line",
        title="Revenue over Time",
        x="date",
        y=["revenue"],
        aggregation="sum",
        group_by=[],
    )


@pytest.fixture()
def table_chart(dashboard):
    """Table chart: raw rows."""
    from core.models import Chart
    return Chart.objects.create(
        dashboard=dashboard,
        chart_type="table",
        title="Raw Data",
        x=None,
        y=["region", "product", "revenue", "cost"],
        aggregation=None,
        group_by=[],
    )
