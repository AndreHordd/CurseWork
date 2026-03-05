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
