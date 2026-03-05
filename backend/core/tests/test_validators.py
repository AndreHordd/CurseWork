from __future__ import annotations

import pandas as pd
import pytest

from core.services.profiler import ColumnProfile, profile_dataframe
from core.services.validators import has_blocking_failures, run_validations


def _profiles(df):
    return profile_dataframe(df).columns


class TestTableNotEmpty:
    def test_passes_for_non_empty(self):
        df = pd.DataFrame({"x": [1, 2]})
        results = run_validations(df, _profiles(df))
        rule = next(r for r in results if r.rule_name == "table_not_empty")
        assert rule.status == "passed"

    def test_fails_for_empty(self):
        df = pd.DataFrame({"x": pd.Series(dtype="int64")})
        profiles = [
            ColumnProfile(
                name="x", inferred_type="int", nullable=False,
                distinct_count=0, missing_count=0, stats={},
            )
        ]
        results = run_validations(df, profiles)
        rule = next(r for r in results if r.rule_name == "table_not_empty")
        assert rule.status == "failed"
        assert has_blocking_failures(results)


class TestColumnNamesUnique:
    def test_passed_when_no_renames(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        results = run_validations(df, _profiles(df), columns_were_renamed=False)
        rule = next(r for r in results if r.rule_name == "column_names_unique")
        assert rule.status == "passed"

    def test_warn_when_renamed(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        results = run_validations(df, _profiles(df), columns_were_renamed=True)
        rule = next(r for r in results if r.rule_name == "column_names_unique")
        assert rule.status == "warn"


class TestDuplicateRows:
    def test_passed_no_duplicates(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        results = run_validations(df, _profiles(df))
        rule = next(r for r in results if r.rule_name == "duplicate_rows")
        assert rule.status == "passed"

    def test_warn_with_duplicates(self):
        df = pd.DataFrame({"x": [1, 2, 1]})
        results = run_validations(df, _profiles(df))
        rule = next(r for r in results if r.rule_name == "duplicate_rows")
        assert rule.status == "warn"
        assert rule.failed_count == 1


class TestMissingValues:
    def test_no_rule_when_no_missing(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        results = run_validations(df, _profiles(df))
        missing_rules = [r for r in results if r.rule_name.startswith("missing_values:")]
        assert len(missing_rules) == 0

    def test_warn_when_missing(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        results = run_validations(df, _profiles(df))
        missing_rules = [r for r in results if r.rule_name.startswith("missing_values:")]
        assert len(missing_rules) == 1
        assert missing_rules[0].status == "warn"
        assert missing_rules[0].failed_count == 1


class TestTypeParseIssues:
    def test_no_issues_for_clean_data(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        results = run_validations(df, _profiles(df))
        type_rules = [r for r in results if r.rule_name.startswith("type_parse_issues:")]
        assert len(type_rules) == 0
