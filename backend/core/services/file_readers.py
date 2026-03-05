from __future__ import annotations

import io
import json as json_stdlib
from dataclasses import dataclass, field

import pandas as pd
from django.conf import settings

from core.exceptions import FileParseError, FileValidationError


@dataclass
class ReadResult:
    df: pd.DataFrame
    warnings: list[str] = field(default_factory=list)
    actual_settings: dict = field(default_factory=dict)


def read_file(
    file_obj,
    file_format: str,
    import_settings: dict | None = None,
) -> ReadResult:
    import_settings = import_settings or {}

    readers = {
        "csv": _read_csv,
        "xlsx": _read_xlsx,
        "json": _read_json,
    }
    reader = readers.get(file_format)
    if reader is None:
        raise FileValidationError(
            f"Unsupported format: {file_format}",
            details={"format": file_format, "allowed": settings.IMPORT_ALLOWED_FORMATS},
        )

    result = reader(file_obj, import_settings)
    norm_warnings = _normalize_columns(result.df)
    result.warnings.extend(norm_warnings)
    return result


# --------------- CSV ---------------


def _read_csv(file_obj, import_settings: dict) -> ReadResult:
    encoding = import_settings.get("encoding", settings.IMPORT_DEFAULT_CSV_ENCODING)
    delimiter = import_settings.get("delimiter", settings.IMPORT_DEFAULT_CSV_DELIMITER)
    has_header = import_settings.get("has_header", True)
    header = 0 if has_header else None

    raw = file_obj.read()
    actual_encoding = encoding
    try:
        df = pd.read_csv(
            io.BytesIO(raw),
            delimiter=delimiter,
            encoding=encoding,
            header=header,
        )
    except UnicodeDecodeError:
        actual_encoding = "latin-1"
        try:
            df = pd.read_csv(
                io.BytesIO(raw),
                delimiter=delimiter,
                encoding="latin-1",
                header=header,
            )
        except Exception as exc:
            raise FileParseError(f"Cannot parse CSV: {exc}") from exc
    except Exception as exc:
        raise FileParseError(f"Cannot parse CSV: {exc}") from exc

    warnings: list[str] = []
    if actual_encoding != encoding:
        warnings.append(
            f"Fallback encoding used: {actual_encoding} (requested {encoding})"
        )

    return ReadResult(
        df=df,
        warnings=warnings,
        actual_settings={
            "encoding": actual_encoding,
            "delimiter": delimiter,
            "has_header": has_header,
        },
    )


# --------------- XLSX ---------------


def _read_xlsx(file_obj, import_settings: dict) -> ReadResult:
    sheet_name = import_settings.get("sheet_name", 0)
    try:
        df = pd.read_excel(
            file_obj,
            sheet_name=sheet_name,
            engine="openpyxl",
        )
    except Exception as exc:
        raise FileParseError(f"Cannot parse XLSX: {exc}") from exc

    warnings: list[str] = []
    if df.empty:
        warnings.append("The selected sheet is empty")

    return ReadResult(
        df=df,
        warnings=warnings,
        actual_settings={"sheet_name": sheet_name},
    )


# --------------- JSON ---------------


def _read_json(file_obj, import_settings: dict) -> ReadResult:
    try:
        raw = file_obj.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json_stdlib.loads(raw)
    except (json_stdlib.JSONDecodeError, UnicodeDecodeError) as exc:
        raise FileParseError(f"Cannot parse JSON: {exc}") from exc

    if isinstance(data, list):
        if not all(isinstance(row, dict) for row in data):
            raise FileParseError(
                "JSON must be an array of objects (records orientation)"
            )
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        try:
            df = pd.DataFrame(data)
        except ValueError as exc:
            raise FileParseError(
                f"JSON object could not be converted to a table: {exc}"
            ) from exc
    else:
        raise FileParseError("JSON root must be an array or an object")

    return ReadResult(df=df, warnings=[], actual_settings={})


# --------------- Column normalization ---------------


def _normalize_columns(df: pd.DataFrame) -> list[str]:
    warnings: list[str] = []

    df.columns = [str(c).strip() for c in df.columns]

    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df.drop(columns=empty_cols, inplace=True)
        warnings.append(f"Dropped entirely empty columns: {empty_cols}")

    # Pandas auto-mangles duplicate headers to "col.1", "col.2", etc.
    # Detect and re-rename to our convention "col__2", "col__3".
    import re

    pandas_dup_re = re.compile(r"^(.+)\.(\d+)$")
    base_names = set(df.columns)
    new_cols = list(df.columns)
    renamed: list[str] = []
    for i, col in enumerate(new_cols):
        m = pandas_dup_re.match(col)
        if m and m.group(1) in base_names:
            base = m.group(1)
            idx = int(m.group(2)) + 1
            new_name = f"{base}__{idx}"
            renamed.append(f"{col} -> {new_name}")
            new_cols[i] = new_name

    # Also handle true duplicates that may come from non-CSV sources
    seen: dict[str, int] = {}
    for i, col in enumerate(new_cols):
        if col in seen:
            seen[col] += 1
            new_name = f"{col}__{seen[col]}"
            renamed.append(f"{col} -> {new_name}")
            new_cols[i] = new_name
        else:
            seen[col] = 1

    if renamed:
        df.columns = new_cols
        warnings.append(f"Renamed duplicate columns: {renamed}")

    return warnings
