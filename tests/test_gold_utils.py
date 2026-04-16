"""
tests/test_gold_utils.py
------------------------
Unit tests for pure utility functions in ``src.transform.gold_utils``.

Tested functions
----------------
* ``assign_surrogate_keys`` — prepends a 1-based integer key column.
* ``check_referential_integrity`` — identifies fact rows with unresolved FKs.

All tests operate exclusively on in-memory DataFrames.  No file I/O,
database connections, or network calls are made.  The functions under test
are pure (no side-effects beyond loguru INFO/WARNING messages), so no
mocking is required.
"""

from __future__ import annotations

import pandas as pd

from src.transform.gold_utils import assign_surrogate_keys, check_referential_integrity

# ===========================================================================
# assign_surrogate_keys
# ===========================================================================


class TestAssignSurrogateKeys:
    """Tests for ``assign_surrogate_keys(df, key_col, start=1)``."""

    def test_assign_surrogate_keys_creates_column(self) -> None:
        """The surrogate key column must appear as the first (leftmost) column."""
        df = pd.DataFrame({"name": ["alice", "bob", "carol"]})
        result = assign_surrogate_keys(df, "customer_key")

        assert (
            result.columns[0] == "customer_key"
        ), f"Expected 'customer_key' as first column; got {list(result.columns)}"
        assert "name" in result.columns

    def test_assign_surrogate_keys_start_value(self) -> None:
        """Default start=1 must produce keys beginning at 1."""
        df = pd.DataFrame({"x": [10, 20, 30]})
        result = assign_surrogate_keys(df, "sk")

        assert list(result["sk"]) == [
            1,
            2,
            3,
        ], f"Expected [1, 2, 3] with default start; got {list(result['sk'])}"

    def test_assign_surrogate_keys_custom_start(self) -> None:
        """start=100 must produce keys 100, 101, 102 for a 3-row DataFrame."""
        df = pd.DataFrame({"val": ["a", "b", "c"]})
        result = assign_surrogate_keys(df, "dim_key", start=100)

        assert list(result["dim_key"]) == [
            100,
            101,
            102,
        ], f"Expected [100, 101, 102]; got {list(result['dim_key'])}"

    def test_assign_surrogate_keys_empty_df(self) -> None:
        """An empty input DataFrame must return an empty DataFrame that still
        contains the key column as its first column."""
        df = pd.DataFrame({"col": pd.Series([], dtype="object")})
        result = assign_surrogate_keys(df, "empty_key")

        assert len(result) == 0
        assert result.columns[0] == "empty_key"
        assert "col" in result.columns

    def test_assign_surrogate_keys_does_not_mutate_input(self) -> None:
        """The function must not mutate the caller's DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        original_cols = list(df.columns)
        assign_surrogate_keys(df, "sk")

        assert (
            list(df.columns) == original_cols
        ), "Input DataFrame columns were mutated by assign_surrogate_keys"

    def test_assign_surrogate_keys_resets_index(self) -> None:
        """Keys must be assigned by position after a reset_index, not by the
        original DataFrame index."""
        df = pd.DataFrame({"v": [10, 20, 30]}, index=[5, 10, 15])
        result = assign_surrogate_keys(df, "sk")

        assert list(result["sk"]) == [1, 2, 3]


# ===========================================================================
# check_referential_integrity
# ===========================================================================


class TestCheckReferentialIntegrity:
    """Tests for ``check_referential_integrity``."""

    def _make_fact(self, fk_values: list) -> pd.DataFrame:
        return pd.DataFrame({"order_id": fk_values, "amount": [100.0] * len(fk_values)})

    def _make_dim(self, pk_values: list) -> pd.DataFrame:
        return pd.DataFrame({"customer_id": pk_values, "name": [f"c{v}" for v in pk_values]})

    def test_check_ri_no_orphans(self) -> None:
        """When every FK value matches a dim PK, orphan_count must be 0."""
        fact_df = self._make_fact([1, 2, 3, 4])
        dim_df = self._make_dim([1, 2, 3, 4])

        orphan_df, orphan_count = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_no_orphans"
        )

        assert orphan_count == 0
        assert orphan_df.empty

    def test_check_ri_some_orphans(self) -> None:
        """When 2 of 4 FK values are absent from the dim PK, orphan_count must be 2."""
        fact_df = self._make_fact([1, 2, 99, 100])
        dim_df = self._make_dim([1, 2])

        orphan_df, orphan_count = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_some_orphans"
        )

        assert orphan_count == 2
        assert len(orphan_df) == 2
        assert set(orphan_df["order_id"].tolist()) == {99, 100}

    def test_check_ri_all_orphans(self) -> None:
        """When no FK values exist in the dim, every fact row is an orphan."""
        fact_df = self._make_fact([10, 20, 30])
        dim_df = self._make_dim([1, 2, 3])

        orphan_df, orphan_count = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_all_orphans"
        )

        assert orphan_count == 3
        assert set(orphan_df["order_id"].tolist()) == {10, 20, 30}

    def test_check_ri_null_dim_keys_ignored(self) -> None:
        """NaN/None values in the dimension PK column must NOT be treated as valid keys."""
        dim_df = pd.DataFrame({"customer_id": [1, None], "name": ["c1", "unknown"]})
        fact_df = self._make_fact([1, 2])

        orphan_df, orphan_count = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_null_dim_keys"
        )

        assert orphan_count == 1
        assert orphan_df.iloc[0]["order_id"] == 2

    def test_check_ri_returns_copy_of_orphan_rows(self) -> None:
        """Mutating the returned orphan_df must not affect the original fact_df."""
        fact_df = self._make_fact([1, 99])
        dim_df = self._make_dim([1])

        orphan_df, _ = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_copy"
        )

        orphan_df["amount"] = -1.0
        assert fact_df.loc[fact_df["order_id"] == 99, "amount"].iloc[0] == 100.0

    def test_check_ri_empty_fact_returns_zero_orphans(self) -> None:
        """An empty fact DataFrame must yield zero orphans."""
        fact_df = pd.DataFrame({"order_id": pd.Series([], dtype="int64"), "amount": []})
        dim_df = self._make_dim([1, 2, 3])

        orphan_df, orphan_count = check_referential_integrity(
            fact_df, dim_df, "order_id", "customer_id", "test_empty_fact"
        )

        assert orphan_count == 0
        assert orphan_df.empty
