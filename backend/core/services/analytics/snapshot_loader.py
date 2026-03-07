from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from core.exceptions import SnapshotLoadError
from core.models import Snapshot

logger = logging.getLogger(__name__)


def load_dataframe(snapshot: Snapshot) -> pd.DataFrame:
    """Load a pandas DataFrame from a Snapshot, supporting both JSONB and file storage."""
    if snapshot.data_json:
        try:
            return pd.DataFrame(snapshot.data_json)
        except Exception as exc:
            raise SnapshotLoadError(
                f"Failed to load DataFrame from data_json: {exc}",
                details={"snapshot_id": str(snapshot.pk)},
            ) from exc

    if snapshot.data_path:
        path = Path(snapshot.data_path)
        if not path.exists():
            raise SnapshotLoadError(
                f"Data file not found: {snapshot.data_path}",
                details={"snapshot_id": str(snapshot.pk), "data_path": snapshot.data_path},
            )
        try:
            suffix = path.suffix.lower()
            if suffix == ".csv":
                return pd.read_csv(path)
            if suffix in (".xls", ".xlsx"):
                return pd.read_excel(path)
            if suffix == ".json":
                return pd.read_json(path)
            if suffix == ".parquet":
                return pd.read_parquet(path)
            raise SnapshotLoadError(
                f"Unsupported file format: {suffix}",
                details={"snapshot_id": str(snapshot.pk), "suffix": suffix},
            )
        except SnapshotLoadError:
            raise
        except Exception as exc:
            raise SnapshotLoadError(
                f"Failed to load DataFrame from file: {exc}",
                details={"snapshot_id": str(snapshot.pk), "data_path": snapshot.data_path},
            ) from exc

    raise SnapshotLoadError(
        "Snapshot has no data (neither data_json nor data_path)",
        details={"snapshot_id": str(snapshot.pk)},
    )
