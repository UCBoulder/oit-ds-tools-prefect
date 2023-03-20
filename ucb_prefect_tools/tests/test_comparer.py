"""Unit tests for the comparer module"""

import pandas as pd
import pytest

from ucb_prefect_tools import comparer

# pylint:disable=protected-access
# pylint:disable=redefined-outer-name


def assert_dataframes_are_equal(left, right):
    """Asserts if the two dataframes have equal values. Useful for checking task output."""

    assert left.astype("object").fillna("").to_dict("records") == right.astype(
        "object"
    ).fillna("").to_dict("records")


@pytest.fixture
def sample_data():
    """Fixture that returns a sample pandas DataFrame."""

    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "date": [pd.Timestamp("2022-01-01")] * 3,
        "value": [1.234, 2.345, 3.456],
    }
    return pd.DataFrame(data)


@pytest.fixture
def standardized_data():
    """Fixture that returns a sample pandas DataFrame in standardized string values format"""

    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": ["25.0", "30.0", "35.0"],
            "date": ["2022-01-01T00:00:00"] * 3,
            "value": ["1.23", "2.35", "3.46"],
        }
    )


def test_list2str():
    """Test function for `_list2str` method, which converts a list into a simple string
    representation."""

    assert comparer._list2str([]) == ""
    assert comparer._list2str(["a"]) == "a"
    assert comparer._list2str(["a", "b"]) == "a, b"


def test_subsets():
    """Tests the `_subsets` method which returns a list of subsets from a list where subset size is
    between 0 and the size of the original list"""

    assert comparer._subsets([1, 2]) == [[1], [2]]
    assert comparer._subsets([1, 2, 3]) == [[1], [2], [3], [1, 2], [1, 3], [2, 3]]


def test_standardize(sample_data, standardized_data):
    """Tests the `_standardize` method which converts a DataFrame's values to strings in a way
    that makes it easy to compare with other DataFrames"""

    actual = comparer._standardize(sample_data)
    assert_dataframes_are_equal(actual, standardized_data)


def test_changes_in_column(standardized_data):
    """Tests the `_changes_in_column` method which returns a dict of the most frequent changes in
    values in the given column"""

    left = standardized_data
    right = left.copy()
    right.loc[0, "date"] = "2022-01-02T00:00:00"
    right.loc[1:2, "date"] = "2022-01-03T00:00:00"
    merged_df = pd.merge(left, right, on="name")

    # Test top_n > actual changes
    expected = {
        "date: 2022-01-01T00:00:00 => 2022-01-03T00:00:00": 2,
        "date: 2022-01-01T00:00:00 => 2022-01-02T00:00:00": 1,
    }
    assert comparer._changes_in_column(merged_df, "date", 10) == expected

    # Test top_n < actual changes
    expected = {
        "date: 2022-01-01T00:00:00 => 2022-01-03T00:00:00": 2,
    }
    assert comparer._changes_in_column(merged_df, "date", 1) == expected


def test_auto_merge(sample_data):
    """Tests the `auto_merge` method merges two sample dataframes as expected"""

    left = sample_data
    right = sample_data.copy()
    right["name"] = ["Alice", "Bob", "David"]
    merged, metrics = comparer.auto_merge(left, right)

    # Check merged
    expected = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age_x": [25, 30],
            "age_y": [25, 30],
            "date_x": [pd.Timestamp("2022-01-01")] * 2,
            "date_y": [pd.Timestamp("2022-01-01")] * 2,
            "value_x": [1.234, 2.345],
            "value_y": [1.234, 2.345],
        }
    )
    assert_dataframes_are_equal(merged, expected)

    # Check metrics
    expected = {
        "merged_rows": 2,
        "unmerged_right_rows": 1,
        "unmerged_left_rows": 1,
        "merge_key_columns": "name",
    }
    assert metrics == expected


def test_reconcile(sample_data, standardized_data):
    """Tests the `reconcile` method properly trims two dataframes down to comparable values"""

    left = sample_data
    right = sample_data.copy()
    # Add a duplicate row
    right.loc[len(right.index)] = right.loc[0]
    # Add an extra column
    left["test"] = "test"

    new_left, new_right, metrics = comparer.reconcile(left, right)
    assert_dataframes_are_equal(new_left, standardized_data)
    assert_dataframes_are_equal(new_right, standardized_data)
    expected = {"left_only_columns": "test", "right_duplicates": 1}
    assert metrics == expected
