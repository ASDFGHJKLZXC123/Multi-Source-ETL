"""
tests/test_silver_utils.py
--------------------------
Unit tests for utility functions in ``src.transform.utils``.

Tested functions
----------------
* ``log_transform_summary`` — logs INFO when drop% < 10%, WARNING when >= 10%.
* ``write_silver``          — writes a date-stamped Parquet file to the Silver dir.
* ``quarantine_rows``       — returns None for empty input; writes annotated Parquet
                              for non-empty input.
* ``read_latest_bronze_parquet`` — raises FileNotFoundError when the directory is
                                   empty; returns the newest file when files exist.

Design
------
File-I/O tests use pytest's ``tmp_path`` fixture and monkeypatching of module-level
path constants so no writes reach the real project data directories.  Log-level
assertions intercept loguru's sink via a temporary ``logger.add`` call scoped to
each test via try/finally.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from loguru import logger

import src.transform.utils as utils_mod
from src.transform.utils import (
    log_transform_summary,
    quarantine_rows,
    read_latest_bronze_parquet,
    write_silver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_simple_df(n: int = 3) -> pd.DataFrame:
    """Return a trivial n-row DataFrame suitable for write/read round-trips."""
    return pd.DataFrame(
        {"id": list(range(1, n + 1)), "value": [f"v{i}" for i in range(1, n + 1)]}
    )


# ===========================================================================
# log_transform_summary
# ===========================================================================


class TestLogTransformSummary:
    """Tests for ``log_transform_summary(stage, before, after, quarantined)``."""

    def test_log_summary_below_threshold(self) -> None:
        """A 5% drop rate (1 quarantined out of 20) must emit INFO, not WARNING."""
        records: list[dict] = []
        sink_id = logger.add(lambda msg: records.append(msg.record), level="DEBUG")
        try:
            log_transform_summary("test_stage", before=20, after=19, quarantined=1)
        finally:
            logger.remove(sink_id)

        levels = [r["level"].name for r in records]
        assert "INFO" in levels
        assert "WARNING" not in levels

    def test_log_summary_at_threshold(self) -> None:
        """A 10% drop rate (exactly at boundary) must emit WARNING."""
        records: list[dict] = []
        sink_id = logger.add(lambda msg: records.append(msg.record), level="DEBUG")
        try:
            log_transform_summary("test_stage", before=10, after=9, quarantined=1)
        finally:
            logger.remove(sink_id)

        levels = [r["level"].name for r in records]
        assert "WARNING" in levels

    def test_log_summary_above_threshold(self) -> None:
        """A 50% drop rate must also emit WARNING."""
        records: list[dict] = []
        sink_id = logger.add(lambda msg: records.append(msg.record), level="DEBUG")
        try:
            log_transform_summary("test_stage", before=10, after=5, quarantined=5)
        finally:
            logger.remove(sink_id)

        levels = [r["level"].name for r in records]
        assert "WARNING" in levels

    def test_log_summary_zero_input(self) -> None:
        """Calling with before=0 must not raise ZeroDivisionError."""
        try:
            log_transform_summary("empty_stage", before=0, after=0, quarantined=0)
        except ZeroDivisionError as exc:
            pytest.fail(f"log_transform_summary raised ZeroDivisionError with before=0: {exc}")

    def test_log_summary_message_contains_stage_name(self) -> None:
        """The logged message must include the stage name."""
        records: list[dict] = []
        sink_id = logger.add(lambda msg: records.append(msg.record), level="DEBUG")
        try:
            log_transform_summary("unique_stage_xyz", before=5, after=5, quarantined=0)
        finally:
            logger.remove(sink_id)

        messages = [r["message"] for r in records]
        assert any("unique_stage_xyz" in m for m in messages)


# ===========================================================================
# write_silver
# ===========================================================================


class TestWriteSilver:
    """Tests for ``write_silver(df, domain, name) -> Path``."""

    def test_write_silver_creates_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The Parquet file must exist on disk after a successful write."""
        monkeypatch.setattr(utils_mod, "SILVER_DIR", tmp_path)

        write_silver(_make_simple_df(), "sales", "orders")

        written_files = list((tmp_path / "sales").glob("orders_*.parquet"))
        assert len(written_files) == 1

    def test_write_silver_returns_correct_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The returned Path must point to the file that was actually written."""
        monkeypatch.setattr(utils_mod, "SILVER_DIR", tmp_path)

        returned_path = write_silver(_make_simple_df(), "fx", "fx_rates")

        assert returned_path.exists()
        assert returned_path.suffix == ".parquet"
        assert returned_path.parent == tmp_path / "fx"

    def test_write_silver_parquet_is_readable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The written Parquet file must be readable back into an equivalent DataFrame."""
        monkeypatch.setattr(utils_mod, "SILVER_DIR", tmp_path)

        df = _make_simple_df(5)
        returned_path = write_silver(df, "weather", "weather")

        recovered = pd.read_parquet(returned_path)
        assert len(recovered) == 5
        assert list(recovered.columns) == list(df.columns)

    def test_write_silver_creates_subdirectory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The domain sub-directory must be created automatically if absent."""
        monkeypatch.setattr(utils_mod, "SILVER_DIR", tmp_path)

        subdir = tmp_path / "brand_new_domain"
        assert not subdir.exists()

        write_silver(_make_simple_df(), "brand_new_domain", "some_table")
        assert subdir.is_dir()


# ===========================================================================
# quarantine_rows
# ===========================================================================


class TestQuarantineRows:
    """Tests for ``quarantine_rows(df, reasons, transform_name) -> Path | None``."""

    def test_quarantine_rows_empty_df_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Passing an empty DataFrame must return None without writing any file."""
        monkeypatch.setattr(utils_mod, "QUARANTINE_DIR", Path("/nonexistent/should/not/matter"))

        df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        reasons = pd.Series([], dtype="object")

        result = quarantine_rows(df, reasons, "empty_transform")
        assert result is None

    def test_quarantine_rows_writes_parquet(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-empty DataFrame must result in a Parquet file being written."""
        monkeypatch.setattr(utils_mod, "QUARANTINE_DIR", tmp_path)
        monkeypatch.setattr("src.transform.utils.timestamp_suffix", lambda: "20170101_120000")

        df = pd.DataFrame({"id": [1, 2], "val": ["x", "y"]})
        reasons = pd.Series(["bad id", "bad val"])

        result = quarantine_rows(df, reasons, "test_transform")

        assert result is not None
        assert result.exists()
        assert result.suffix == ".parquet"

    def test_quarantine_rows_parquet_has_quarantine_reason_column(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The written Parquet file must contain a ``quarantine_reason`` column."""
        monkeypatch.setattr(utils_mod, "QUARANTINE_DIR", tmp_path)
        monkeypatch.setattr("src.transform.utils.timestamp_suffix", lambda: "20170101_130000")

        df = pd.DataFrame({"id": [5], "amount": [99.9]})
        reasons = pd.Series(["negative amount"])

        out_path = quarantine_rows(df, reasons, "check_amount")
        recovered = pd.read_parquet(out_path)

        assert "quarantine_reason" in recovered.columns
        assert recovered["quarantine_reason"].iloc[0] == "negative amount"

    def test_quarantine_rows_parquet_has_quarantined_at_column(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The written Parquet file must contain a ``quarantined_at`` column."""
        monkeypatch.setattr(utils_mod, "QUARANTINE_DIR", tmp_path)
        monkeypatch.setattr("src.transform.utils.timestamp_suffix", lambda: "20170101_140000")

        df = pd.DataFrame({"id": [7]})
        reasons = pd.Series(["missing data"])

        out_path = quarantine_rows(df, reasons, "check_missing")
        recovered = pd.read_parquet(out_path)

        assert "quarantined_at" in recovered.columns

    def test_quarantine_rows_returns_path_object(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The returned value for a non-empty DataFrame must be a ``pathlib.Path``."""
        monkeypatch.setattr(utils_mod, "QUARANTINE_DIR", tmp_path)
        monkeypatch.setattr("src.transform.utils.timestamp_suffix", lambda: "20170101_150000")

        df = pd.DataFrame({"col": [1]})
        reasons = pd.Series(["reason"])

        result = quarantine_rows(df, reasons, "path_type_check")
        assert isinstance(result, Path)


# ===========================================================================
# read_latest_bronze_parquet
# ===========================================================================


class TestReadLatestBronzeParquet:
    """Tests for ``read_latest_bronze_parquet(table_dir) -> pd.DataFrame``."""

    def test_read_latest_bronze_raises_when_empty(self, tmp_path: Path) -> None:
        """An empty directory must raise ``FileNotFoundError``."""
        with pytest.raises(FileNotFoundError, match="No Parquet files found"):
            read_latest_bronze_parquet(tmp_path)

    def test_read_latest_bronze_raises_with_only_tmp_files(self, tmp_path: Path) -> None:
        """A directory with only ``.parquet.tmp`` in-progress files must raise
        ``FileNotFoundError`` because those files are excluded from the glob."""
        (tmp_path / "data_20170101.parquet.tmp").write_bytes(b"")

        with pytest.raises(FileNotFoundError):
            read_latest_bronze_parquet(tmp_path)

    def test_read_latest_bronze_returns_latest_file(self, tmp_path: Path) -> None:
        """When multiple dated Parquet files are present, only the latest
        (lexicographically last) file must be read."""
        df_old = pd.DataFrame({"id": [1], "tag": ["old"]})
        df_new = pd.DataFrame({"id": [2], "tag": ["new"]})

        df_old.to_parquet(tmp_path / "orders_20170101.parquet", index=False)
        df_new.to_parquet(tmp_path / "orders_20171231.parquet", index=False)

        result = read_latest_bronze_parquet(tmp_path)
        assert result.iloc[0]["tag"] == "new"

    def test_read_latest_bronze_single_file(self, tmp_path: Path) -> None:
        """A directory with exactly one Parquet file must return its contents."""
        df = _make_simple_df(4)
        df.to_parquet(tmp_path / "table_20170601.parquet", index=False)

        result = read_latest_bronze_parquet(tmp_path)
        assert len(result) == 4

    def test_read_latest_bronze_excludes_tmp_suffix(self, tmp_path: Path) -> None:
        """A ``.parquet.tmp`` file alongside a valid ``.parquet`` file must be ignored."""
        df_real = pd.DataFrame({"id": [99], "label": ["real"]})
        df_real.to_parquet(tmp_path / "data_20170901.parquet", index=False)
        (tmp_path / "data_20171201.parquet.tmp").write_bytes(b"garbage")

        result = read_latest_bronze_parquet(tmp_path)
        assert result.iloc[0]["label"] == "real"
