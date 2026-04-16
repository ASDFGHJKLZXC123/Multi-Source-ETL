"""
Unit tests for src/utils/validators.py.

Run with:
    python -m pytest tests/test_validators.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.utils.validators import (
    log_data_quality_report,
    normalize_city_name,
    validate_dataframe,
)


class TestNormalizeCityName:
    def test_strips_accents(self):
        assert normalize_city_name("São Paulo") == "sao paulo"

    def test_lowercases(self):
        assert normalize_city_name("RECIFE") == "recife"

    def test_strips_whitespace(self):
        assert normalize_city_name("  Curitiba  ") == "curitiba"

    def test_cedilla(self):
        assert normalize_city_name("Fortaleza") == "fortaleza"

    def test_tilde(self):
        assert normalize_city_name("João Pessoa") == "joao pessoa"

    def test_combined_accent_and_space(self):
        assert normalize_city_name("  Belém  ") == "belem"

    def test_non_string_returns_empty(self):
        assert normalize_city_name(None) == ""  # type: ignore[arg-type]

    def test_already_clean(self):
        assert normalize_city_name("manaus") == "manaus"


class TestValidateDataframe:
    def _make_df(self):
        return pd.DataFrame({
            "order_id": ["a", "b", "c"],
            "price":    [10.0, 20.5, None],
            "qty":      [1, 2, 3],
        })

    def test_passes_on_valid_schema(self):
        df = self._make_df()
        schema = {"order_id": "object", "price": "float", "qty": "int"}
        issues = validate_dataframe(df, schema, max_null_rate=1.0)
        assert issues == []

    def test_reports_missing_column(self):
        df = self._make_df()
        schema = {"order_id": "object", "nonexistent": "float"}
        issues = validate_dataframe(df, schema)
        assert any("nonexistent" in msg for msg in issues)

    def test_reports_high_null_rate(self):
        df = pd.DataFrame({"col_a": [None, None, None, 1.0]})
        schema = {"col_a": "float"}
        issues = validate_dataframe(df, schema, max_null_rate=0.5)
        assert any("null rate" in msg.lower() for msg in issues)

    def test_no_false_positive_on_acceptable_null_rate(self):
        df = pd.DataFrame({"col_a": [None, 1.0, 2.0, 3.0]})
        schema = {"col_a": "float"}
        issues = validate_dataframe(df, schema, max_null_rate=0.5)
        assert issues == []

    def test_reports_dtype_mismatch(self):
        df = pd.DataFrame({"amount": ["a", "b", "c"]})
        schema = {"amount": "float"}
        issues = validate_dataframe(df, schema)
        assert any("dtype" in msg.lower() for msg in issues)


class TestLogDataQualityReport:
    def test_returns_dict_with_expected_keys(self):
        df = pd.DataFrame({"x": [1, 2, None], "y": ["a", "b", "a"]})
        report = log_data_quality_report(df, "test_table")
        assert report["table"] == "test_table"
        assert report["row_count"] == 3
        assert report["col_count"] == 2
        assert "x" in report["null_pct_by_column"]
        assert "y" in report["null_pct_by_column"]

    def test_counts_duplicates(self):
        df = pd.DataFrame({"id": [1, 1, 2]})
        report = log_data_quality_report(df, "dup_test")
        assert report["duplicate_rows"] == 1

    def test_numeric_range_recorded(self):
        df = pd.DataFrame({"score": [1.0, 5.0, 3.0]})
        report = log_data_quality_report(df, "scores")
        assert report["numeric_range"]["score"]["min"] == pytest.approx(1.0)
        assert report["numeric_range"]["score"]["max"] == pytest.approx(5.0)
