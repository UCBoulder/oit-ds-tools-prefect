"""Unit tests for the comparer module"""

import io

import pandas as pd
import pytest
from prefect.logging import disable_run_logger

from ucb_prefect_tools import data_archive, object_storage

# pylint:disable=protected-access
# pylint:disable=redefined-outer-name


def assert_dataframes_are_equal(left, right):
    """Asserts if the two dataframes have equal values. Useful for checking task output."""

    assert left.astype("object").fillna("").to_dict("records") == right.astype(
        "object"
    ).fillna("").to_dict("records")


@pytest.fixture
def sample_archive():
    """Fixture that returns the path in the remote archive storage to a sample archive file."""

    # Current state include Bob and Charlie and Alice being deleted
    dataframe = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "archive_last_updated_at": [
                pd.Timestamp(i) for i in ["2023-01-01", "2023-01-02", "2023-01-03"]
            ],
            "archive_deleted_at": [pd.Timestamp("2023-01-03"), None, None],
        }
    )
    data = io.BytesIO()
    dataframe.to_pickle(data)
    data.seek(0)
    path = "test/data_archive_unit_tests/sample_archive"
    with disable_run_logger():
        object_storage.put.fn(data, path, data_archive.archive_storage())
    return path


def test_get_new_rows(sample_archive):
    """Tests the `get_new_rows` task."""

    # This dataframe adds Alica back in, adds Darlene for the first time, and updates Bob's age
    dataframe = pd.DataFrame(
        {"name": ["Alice", "Charlie", "Bob", "Darlene"], "age": [25, 35, 31, 40]}
    )

    # First test matching on all columns
    with disable_run_logger():
        actual_results = data_archive.get_new_rows.fn(dataframe, sample_archive)
        expected_results = pd.DataFrame(
            {"name": ["Alice", "Bob", "Darlene"], "age": [25, 31, 40]}
        )
        assert_dataframes_are_equal(actual_results, expected_results)

    # Next, test matching on name
    with disable_run_logger():
        # With this, Bob does not count as a brand new row anymore
        actual_results = data_archive.get_new_rows.fn(
            dataframe, sample_archive, match_on="name"
        )
        expected_results = pd.DataFrame({"name": ["Alice", "Darlene"], "age": [25, 40]})
        assert_dataframes_are_equal(actual_results, expected_results)


def test_get_changed_rows(sample_archive):
    """Tests the `get_changed_rows` task."""

    # This dataframe adds Darlene for the first time and updates Bob's age
    dataframe = pd.DataFrame(
        {"name": ["Charlie", "Bob", "Darlene"], "age": [35, 31, 40]}
    )

    with disable_run_logger():
        actual_results = data_archive.get_changed_rows.fn(
            dataframe, sample_archive, match_on="name"
        )
        expected_results = pd.DataFrame({"name": ["Bob"], "age": [31]})
        assert_dataframes_are_equal(actual_results, expected_results)


def test_get_dropped_rows(sample_archive):
    """Tests the `get_dropped_rows` task."""

    # This dataframe adds Darlene for the first time, updates Bob's age, and drops Charlie
    dataframe = pd.DataFrame({"name": ["Bob", "Darlene"], "age": [31, 40]})

    # When matching on all rows, both Bob and Charlie's rows count as dropped
    with disable_run_logger():
        actual_results = data_archive.get_dropped_rows.fn(dataframe, sample_archive)
        expected_results = pd.DataFrame({"name": ["Bob", "Charlie"], "age": [30, 35]})
        assert_dataframes_are_equal(actual_results, expected_results)

    # When matching on name, just Charlie counts as dropped
    with disable_run_logger():
        actual_results = data_archive.get_dropped_rows.fn(
            dataframe, sample_archive, match_on=["name"]
        )
        expected_results = pd.DataFrame({"name": ["Charlie"], "age": [35]})
        assert_dataframes_are_equal(actual_results, expected_results)


def test_update_archive(sample_archive):
    """Tests the `update_archive` task."""

    # This dataframe adds Alica back in, adds Darlene for the first time, updates Bob's age, and
    # drops Charlie
    dataframe = pd.DataFrame({"name": ["Alice", "Bob", "Darlene"], "age": [25, 31, 40]})

    # Update the archive and then download it to check it
    with disable_run_logger():
        data_archive.update_archive.fn(
            dataframe, sample_archive, dt_override=pd.Timestamp("2023-01-04")
        )
        archive_bytes = object_storage.get.fn(
            sample_archive, data_archive.archive_storage()
        )
    actual_results = pd.read_pickle(io.BytesIO(archive_bytes))

    # Expect that Alice is added back, Bob is replaced, Charlie is deleted, and Darlene is added
    # Order is new, unchanged, freshly deleted, previously deleted
    expected_results = pd.DataFrame(
        columns=["name", "age", "archive_last_updated_at", "archive_deleted_at"],
        data=[
            ["Alice", 25, pd.Timestamp("2023-01-04"), None],
            ["Bob", 31, pd.Timestamp("2023-01-04"), None],
            ["Darlene", 40, pd.Timestamp("2023-01-04"), None],
            ["Bob", 30, pd.Timestamp("2023-01-02"), pd.Timestamp("2023-01-04")],
            ["Charlie", 35, pd.Timestamp("2023-01-03"), pd.Timestamp("2023-01-04")],
            ["Alice", 25, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-03")],
        ],
    )
    print(expected_results)

    assert_dataframes_are_equal(actual_results, expected_results)
