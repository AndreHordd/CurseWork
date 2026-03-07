from __future__ import annotations

import pandas as pd
import pytest

from core.exceptions import ColumnNotFoundError, OperationConfigError
from core.services.cleaning.duplicates import drop_duplicates


@pytest.fixture()
def df_with_dups():
    return pd.DataFrame({
        "id": [1, 2, 1, 3, 2],
        "name": ["Alice", "Bob", "Alice", "Carol", "Bob"],
        "value": [10, 20, 10, 30, 20],
    })


class TestDropDuplicates:
    def test_drop_full_duplicates_keep_first(self, df_with_dups):
        result = drop_duplicates(df_with_dups, keep="first")
        assert len(result) == 3

    def test_drop_full_duplicates_keep_last(self, df_with_dups):
        result = drop_duplicates(df_with_dups, keep="last")
        assert len(result) == 3

    def test_drop_all_duplicates(self, df_with_dups):
        result = drop_duplicates(df_with_dups, keep="false")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Carol"

    def test_drop_by_subset(self, df_with_dups):
        result = drop_duplicates(df_with_dups, subset=["id"], keep="first")
        assert len(result) == 3

    def test_no_duplicates(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = drop_duplicates(df)
        assert len(result) == 3


class TestErrors:
    def test_invalid_keep(self, df_with_dups):
        with pytest.raises(OperationConfigError, match="Invalid keep"):
            drop_duplicates(df_with_dups, keep="invalid")

    def test_nonexistent_subset_column(self, df_with_dups):
        with pytest.raises(ColumnNotFoundError):
            drop_duplicates(df_with_dups, subset=["nonexistent"])
