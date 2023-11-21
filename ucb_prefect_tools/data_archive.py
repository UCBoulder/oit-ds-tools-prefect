"""Prefect tasks and functions for interacting with data archives, which are pickled Pandas
dataframes stored in an object_storage system that track how rows in a dataset have changed over
time and allow for quick calculation of new rows compared to the previous archive version.

Dataset archives are kept in the object_storage system described by the Prefect JSON block
identified by ARCHIVE_STORAGE_BLOCK. Keep in mind you should vary `archive_path` by environment
to keep test and prod archives distinct.

Dataset archives always contain an additional archive_last_updated_at column which identifies the
America/Denver datetime when that row was added to the dataset. They also contain an
archive_deleted_at column which indicates when a row was dropped from the dataset (otherwise is
None). These two columns are not included in the get_*_rows return values, and the get_*_rows tasks
only consider un-dropped archive rows when calculating differences.

Dataset archives are stored as Pandas pickle files to best preserve datatypes. If an empty file is
found in the archive_path instead of a pickle file, it is treated as an empty archive with columns
matching the `dataset` argument. This allows you to quickly initialize an archive without having to
know all the column names.
"""

import io
import datetime

import pandas as pd
from prefect import task, get_run_logger
from prefect.logging import disable_run_logger
from prefect.blocks.system import JSON
import dateutil

from .object_storage import get, put

ARCHIVE_STORAGE_BLOCK = "ds-prod-storage"

#####
# Tasks to use within Prefect Flows
#####


@task
def get_new_rows(
    dataframe: pd.DataFrame,
    archive_path: str,
    match_on: list = None,
) -> pd.DataFrame:
    """Returns rows which are in dataframe which are not found in the identified archived dataset
    based only on the match_on columns."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    # Retrieve the archived dataset
    with disable_run_logger():
        archive_bytes = get.fn(archive_path, connection_info=connection_info)
    if not archive_bytes:  # If archive is empty
        get_run_logger().info(
            "data_archive: Found %s new rows compared to empty archive at %s",
            len(dataframe),
            archive_path,
        )
        return dataframe

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))
    archive_df = archive_df[
        archive_df["archive_deleted_at"].isna()
    ]  # Consider only non-deleted rows

    # If no columns are specified for matching, use all columns except the archive-specific ones
    if match_on is None:
        match_on = [
            col
            for col in dataframe.columns
            if col not in ["archive_last_updated_at", "archive_deleted_at"]
        ]

    # Merging the provided dataframe with the archive
    merged_df = pd.merge(dataframe, archive_df, on=match_on, how="left", indicator=True)

    # Filtering to get new rows
    new_rows = merged_df[merged_df["_merge"] == "left_only"]
    new_rows = new_rows.drop(
        columns=[
            col for col in new_rows.columns if col.endswith("_y") or col == "_merge"
        ]
    )

    get_run_logger().info(
        "data_archive: Found %s new rows compared to archived dataset at %s",
        len(new_rows),
        archive_path,
    )
    return new_rows


@task
def get_changed_rows(
    dataframe: pd.DataFrame,
    archive_path: str,
    match_on: list = None,
) -> pd.DataFrame:
    """Returns rows which are in dataframe which ARE found in the identified archived dataset
    based on the match_on columns, BUT which differ according to any of the other columns. If
    match_on is None (default), matches on all columns."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    # Retrieve the archived dataset
    with disable_run_logger():
        archive_bytes = get.fn(archive_path, connection_info=connection_info)
    if not archive_bytes:  # If archive is empty
        get_run_logger().info(
            "data_archive: Found 0 changed rows compared to empty archive at %s",
            archive_path,
        )
        return pd.DataFrame()

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))
    archive_df = archive_df[
        archive_df["archive_deleted_at"].isna()
    ]  # Consider only non-deleted rows

    # If no columns are specified for matching, use all columns except the archive-specific ones
    if match_on is None:
        match_on = [
            col
            for col in dataframe.columns
            if col not in ["archive_last_updated_at", "archive_deleted_at"]
        ]

    # Merging the provided dataframe with the archive
    merged_df = pd.merge(
        dataframe, archive_df, on=match_on, how="inner", indicator=True
    )

    # Filtering to find changed rows
    changed_rows = merged_df[
        merged_df.filter(regex="_x$")
        .ne(
            merged_df.filter(regex="_y$").rename(
                columns=lambda x: x.rstrip("_y") + "_x"
            )
        )
        .any(axis=1)
    ]
    changed_rows = changed_rows.loc[:, ~changed_rows.columns.str.endswith("_y")]

    get_run_logger().info(
        "data_archive: Found %s changed rows compared to archived dataset at %s",
        len(changed_rows),
        archive_path,
    )
    return changed_rows


@task
def get_dropped_rows(
    dataframe: pd.DataFrame,
    archive_path: str,
    match_on: list = None,
) -> pd.DataFrame:
    """Returns rows which are NOT in dataframe which are in the identified archived dataset
    based on the match_on columns. If match_on is None (default), matches on all columns."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    # Retrieve the archived dataset
    with disable_run_logger():
        archive_bytes = get.fn(archive_path, connection_info=connection_info)
    if not archive_bytes:  # If archive is empty
        get_run_logger().info(
            "data_archive: Found 0 dropped rows compared to empty archive at %s",
            archive_path,
        )
        return pd.DataFrame()

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))
    archive_df = archive_df[
        archive_df["archive_deleted_at"].isna()
    ]  # Consider only non-deleted rows

    # If no columns are specified for matching, use all columns except the archive-specific ones
    if match_on is None:
        match_on = [
            col
            for col in dataframe.columns
            if col not in ["archive_last_updated_at", "archive_deleted_at"]
        ]

    # Merging the archived dataframe with the provided dataframe
    merged_df = pd.merge(archive_df, dataframe, on=match_on, how="left", indicator=True)

    # Filtering to find dropped rows
    dropped_rows = merged_df[merged_df["_merge"] == "left_only"]
    dropped_rows = dropped_rows.loc[:, ~dropped_rows.columns.str.endswith("_y")]

    get_run_logger().info(
        "data_archive: Found %s dropped rows compared to provided dataset at %s",
        len(dropped_rows),
        archive_path,
    )
    return dropped_rows


@task
def update_archive(dataframe: pd.DataFrame, archive_path: str) -> None:
    """Adds any new rows from `dataframe` which do not completely match existing rows in the
    identified archived dataset with an archive_last_updated_at value of now. For rows in the
    archive which do not completely match any rows in `dataframe`, sets their
    archive_deleted_at value to now. In other words, the "current state" of the archive is
    modified to match `dataframe`."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    # Retrieve the archived dataset
    with disable_run_logger():
        archive_bytes = get.fn(archive_path, connection_info=connection_info)
    if archive_bytes:
        archive_df = pd.read_pickle(io.BytesIO(archive_bytes))
    else:
        # Initialize empty archive with same columns as dataframe plus archive-specific columns
        archive_df = pd.DataFrame(
            columns=list(dataframe.columns)
            + ["archive_last_updated_at", "archive_deleted_at"]
        )

    current_time = datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(hours=-7))
    )  # America/Denver timezone

    # Mark rows in the archive that are not present in the dataframe as deleted
    merged_df = pd.merge(archive_df, dataframe, how="left", indicator=True)
    merged_df.loc[
        merged_df["_merge"] == "left_only", "archive_deleted_at"
    ] = current_time

    # Find new rows to add to the archive
    new_rows = pd.merge(dataframe, archive_df, how="left", indicator=True)
    new_rows = new_rows[new_rows["_merge"] == "left_only"].drop(columns="_merge")
    new_rows["archive_last_updated_at"] = current_time
    new_rows["archive_deleted_at"] = pd.NA

    # Combine updated archive rows with new rows
    updated_archive = pd.concat(
        [merged_df[merged_df["_merge"] != "left_only"], new_rows]
    )

    # Store the updated archive
    with disable_run_logger():
        data = io.BytesIO()
        updated_archive.to_pickle(data)
        data.seek(0)
        put.fn(data, archive_path, connection_info)

    get_run_logger().info(
        "data_archive: Updated archive at %s with %s new rows and marked %s rows as deleted",
        archive_path,
        len(new_rows),
        len(merged_df[merged_df["_merge"] == "left_only"]),
    )


#####
# Functions to interact with archives from the Python interpreter
#####


def get_data(archive_path: str, as_of: str = None) -> pd.DataFrame:
    """Returns a snapshot of the archive dataset based on a date or datetime given by `as_of`,
    which should parse to a date/datetime. Then this returns all rows with archive_last_updated_at
    less than as_of and archive_deleted_at greater than as_of or null."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    with disable_run_logger():
        # Retrieve the archived dataset
        archive_bytes = get.fn(archive_path, connection_info=connection_info)

    if not archive_bytes:
        print(f"data_archive: No data found in archive at {archive_path}")
        return pd.DataFrame()

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))

    # Parse the as_of date if provided
    if as_of:
        as_of_datetime = dateutil.parser.parse(as_of)
        # Filter for rows valid as of the specified date
        archive_df = archive_df[
            (archive_df["archive_last_updated_at"] <= as_of_datetime)
            & (
                (archive_df["archive_deleted_at"] > as_of_datetime)
                | (archive_df["archive_deleted_at"].isna())
            )
        ]
        print(
            f"data_archive: Retrieved data as of {as_of} from archive at {archive_path}"
        )
    else:
        print(f"data_archive: Retrieved latest data from archive at {archive_path}")

    return archive_df


def info(archive_path: str) -> str:
    """Returns a string summarizing information about the given archive dataset, such as current
    length, length of the archive, etc."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    with disable_run_logger():
        # Retrieve the archived dataset
        archive_bytes = get.fn(archive_path, connection_info=connection_info)

    if not archive_bytes:
        print(f"data_archive: No data found in archive at {archive_path}")
        return "No data found in the specified archive."

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))

    # Calculate various statistics for the archive
    current_length = len(archive_df[archive_df["archive_deleted_at"].isna()])
    total_length = len(archive_df)
    last_updated = archive_df["archive_last_updated_at"].max()

    info_string = (
        f"Archive Path: {archive_path}\n"
        f"Current Length (excluding deleted): {current_length}\n"
        f"Total Length (including deleted): {total_length}\n"
        f"Last Updated At: {last_updated}"
    )

    print(f"data_archive: Information retrieved for archive at {archive_path}")
    return info_string


def undo_changes(
    archive_path: str, start_at: str = None, end_at: str = None, commit: bool = False
) -> None:
    """Removes rows from the archive which have archive_last_updated_at or archive_deleted_at
    between the given datetimes (which should be given as parsable strings). If commit is False
    (default), then just prints a summary of how many updates and deletes will be rolled back."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    with disable_run_logger():
        # Retrieve the archived dataset
        archive_bytes = get.fn(archive_path, connection_info=connection_info)

    if not archive_bytes:
        print(f"data_archive: No data found in archive at {archive_path}")
        return

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))

    # Parse the start_at and end_at dates
    if start_at:
        start_at_datetime = dateutil.parser.parse(start_at)
    else:
        start_at_datetime = datetime.datetime.min

    if end_at:
        end_at_datetime = dateutil.parser.parse(end_at)
    else:
        end_at_datetime = datetime.datetime.max

    # Filter for rows to be rolled back
    rows_to_undo = archive_df[
        (
            (archive_df["archive_last_updated_at"] >= start_at_datetime)
            & (archive_df["archive_last_updated_at"] <= end_at_datetime)
        )
        | (
            (archive_df["archive_deleted_at"] >= start_at_datetime)
            & (archive_df["archive_deleted_at"] <= end_at_datetime)
        )
    ]

    if commit:
        # Remove the rows to be undone from the archive
        archive_df = archive_df.drop(rows_to_undo.index)

        with disable_run_logger():
            # Update the archive
            data = io.BytesIO()
            archive_df.to_pickle(data)
            data.seek(0)
            put.fn(data, archive_path, connection_info)

        print(
            f"data_archive: Rolled back {len(rows_to_undo)} changes in archive at {archive_path}"
        )
    else:
        print(
            f"data_archive: Found {len(rows_to_undo)} changes to be rolled back in archive at "
            f"{archive_path}"
        )


def init(archive_path: str, overwrite: bool = False) -> None:
    """Creates an empty file at `archive_path`, representing an empty archive whose columns will
    end up matching whatever is the first dataset used to update it. If overwrite is False (default)
    and a file already exists at `archive_path`, then prints the results of `info` for it instead
    of replacing it with an empty file."""

    # Load connection info from JSON block
    connection_info = JSON.load(ARCHIVE_STORAGE_BLOCK).value

    with disable_run_logger():
        # Check if the archive already exists
        archive_bytes = get.fn(archive_path, connection_info=connection_info)

    if archive_bytes and not overwrite:
        # If archive exists and overwrite is False, print info about the existing archive
        existing_info = info(
            archive_path
        )  # Assuming 'info' function is defined in the same module
        print(
            f"data_archive: Archive already exists at {archive_path}. Here is its current info:\n"
            f"{existing_info}"
        )
    else:
        with disable_run_logger():
            put.fn(b"", archive_path, connection_info)
        print(f"data_archive: Initialized empty archive at {archive_path}")
