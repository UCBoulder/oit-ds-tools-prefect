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

import pandas as pd
from prefect import task, get_run_logger
from prefect.logging import disable_run_logger
from prefect.blocks.system import JSON
from dateutil import parser
import pytz

from .object_storage import get, put
from . import util


def archive_storage():
    """Returns the json connection info for the archive storage system"""

    return util.reveal_secrets(JSON.load("ds-prod-storage").value)


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

    archive_df = get_data(archive_path)
    if archive_df.empty:
        get_run_logger().info(
            "data_archive: Found %s new rows compared to empty archive at %s",
            len(dataframe),
            archive_path,
        )
        return dataframe
    match_on = _match_columns(dataframe, archive_df, match_on)

    # Merging the provided dataframe with the archive
    merged_df = pd.merge(
        dataframe, archive_df[match_on], on=match_on, how="left", indicator=True
    )

    # Filtering to get new rows
    new_rows = merged_df[merged_df["_merge"] == "left_only"].drop(columns="_merge")

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
    match_on: list,
) -> pd.DataFrame:
    """Returns rows which are in dataframe which ARE found in the identified archived dataset
    based on the match_on columns, BUT which differ according to any of the other columns. In this
    case, match_on is required."""

    archive_df = get_data(archive_path)
    if archive_df.empty:
        get_run_logger().info(
            "data_archive: Found 0 changed rows compared to empty archive at %s",
            archive_path,
        )
        return dataframe.iloc[:0]
    match_on = _match_columns(dataframe, archive_df, match_on)

    # Merging the provided dataframe with the archive
    merged_df = pd.merge(dataframe, archive_df, on=match_on, how="inner")

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
    changed_rows = changed_rows.drop(
        columns=[i for i in changed_rows.columns if i.endswith("_y")]
    )
    changed_rows = changed_rows.rename(
        columns=lambda x: x[:-2] if x.endswith("_x") else x
    )

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

    archive_df = get_data(archive_path)
    if archive_df.empty:
        get_run_logger().info(
            "data_archive: Found 0 dropped rows compared to empty archive at %s",
            archive_path,
        )
        return dataframe.iloc[:0]
    match_on = _match_columns(dataframe, archive_df, match_on)

    # Merging the archived dataframe with the provided dataframe
    merged_df = pd.merge(
        archive_df, dataframe[match_on], on=match_on, how="left", indicator=True
    )

    # Filtering to find dropped rows
    dropped_rows = merged_df[merged_df["_merge"] == "left_only"].drop(columns="_merge")

    get_run_logger().info(
        "data_archive: Found %s dropped rows compared to provided dataset at %s",
        len(dropped_rows),
        archive_path,
    )
    return dropped_rows


@task
def update_archive(
    dataframe: pd.DataFrame, archive_path: str, dt_override=None
) -> None:
    """Adds any new rows from `dataframe` which do not completely match existing rows in the
    identified archived dataset with an archive_last_updated_at value of now. For rows in the
    archive which do not completely match any rows in `dataframe`, sets their
    archive_deleted_at value to now. In other words, the "current state" of the archive is
    modified to match `dataframe`. `dt_override` is just used for unit testing."""

    # Get full archive
    archive_df = get_data(archive_path, include_timestamps=True, include_deleted=True)
    if archive_df.empty:
        # Initialize empty archive with same columns as dataframe plus archive-specific columns
        archive_df = pd.DataFrame(
            columns=list(dataframe.columns)
            + ["archive_last_updated_at", "archive_deleted_at"]
        )
    # Prep data
    current_archive = archive_df[pd.isnull(archive_df["archive_deleted_at"])]
    columns = _match_columns(
        dataframe,
        current_archive.drop(columns=["archive_last_updated_at", "archive_deleted_at"]),
    )
    history = archive_df[~archive_df["archive_deleted_at"].isna()]
    current_time = util.now() if not dt_override else dt_override

    # Get rows from the current archive and mark them deleted if they aren't in the new dataframe
    deleted_or_unchanged = pd.merge(
        current_archive,
        dataframe,
        how="left",
        on=columns,
        indicator=True,
    )
    deleted_rows = deleted_or_unchanged[
        deleted_or_unchanged["_merge"] == "left_only"
    ].drop(columns="_merge")
    deleted_rows["archive_deleted_at"] = current_time

    unchanged_rows = deleted_or_unchanged[
        deleted_or_unchanged["_merge"] == "both"
    ].drop(columns="_merge")

    # Find new rows to add to the archive
    new_rows = pd.merge(
        current_archive, dataframe, how="right", on=columns, indicator=True
    )
    new_rows = new_rows[new_rows["_merge"] == "right_only"].drop(columns="_merge")
    new_rows["archive_last_updated_at"] = current_time
    new_rows["archive_deleted_at"] = pd.NaT

    # Combine all back into a complete archive
    full_updated_archive = pd.concat(
        [new_rows, unchanged_rows, deleted_rows, history], ignore_index=True
    )

    # Store the updated archive
    with disable_run_logger():
        data = io.BytesIO()
        full_updated_archive.to_pickle(data)
        data.seek(0)
        put.fn(data, archive_path, archive_storage())

    get_run_logger().info(
        "data_archive: Updated archive at %s with %s new rows and marked %s rows as deleted",
        archive_path,
        len(new_rows),
        len(deleted_rows),
    )


#####
# Functions to interact with archives from the Python interpreter
#####


def get_data(
    archive_path: str,
    as_of: str = None,
    include_timestamps: bool = False,
    include_deleted: bool = False,
) -> pd.DataFrame:
    """Returns a snapshot of the archive dataset based on a date or datetime given by `as_of`,
    which should parse to a date/datetime. Then this returns all rows with archive_last_updated_at
    less than as_of and archive_deleted_at greater than as_of or null. If `as_of` is None (default),
    returns the current dataset. If `include_timestamps` is True, the archive_last_updated_at and
    archive_deleted_at columns are included. If `include_deleted` is True, returns all rows of the
    archive, including deleted (`as_of` is ignored in this case)."""

    # Retrieve the archived dataset
    with disable_run_logger():
        archive_bytes = get.fn(archive_path, archive_storage())

    # Treat empty file as an empty archive
    if not archive_bytes:
        if include_timestamps:
            return pd.DataFrame(
                data=[], columns=["archive_last_updated_at", "archive_deleted_at"]
            )
        return pd.DataFrame()

    archive_df = pd.read_pickle(io.BytesIO(archive_bytes))

    if include_timestamps:
        final_columns = archive_df.columns.tolist()
    else:
        final_columns = [
            i
            for i in archive_df.columns
            if i not in ["archive_last_updated_at", "archive_deleted_at"]
        ]

    if include_deleted:
        return archive_df[final_columns]

    if as_of:
        as_of = _parse_to_denver_time(as_of)
        # Filter for rows valid as of the specified date
        archive_df = archive_df[
            (archive_df["archive_last_updated_at"] <= as_of)
            & (
                (archive_df["archive_deleted_at"] >= as_of)
                | (archive_df["archive_deleted_at"].isna())
            )
        ]
        return archive_df[final_columns]

    # Otherwise, filter out deleted rows to get the current dataset
    return archive_df[pd.isnull(archive_df["archive_deleted_at"])][final_columns]


def info(archive_path: str) -> str:
    """Returns a string summarizing information about the given archive dataset, such as current
    length, length of the archive, etc."""

    archive_df = get_data(archive_path, include_timestamps=True, include_deleted=True)

    # Calculate various statistics for the archive
    current_length = len(archive_df[archive_df["archive_deleted_at"].isna()])
    total_length = len(archive_df)
    last_updated = max(
        archive_df["archive_last_updated_at"].max(),
        archive_df["archive_deleted_at"].max(),
    )
    first_updated = min(
        archive_df["archive_last_updated_at"].min(),
        archive_df["archive_deleted_at"].min(),
    )

    info_string = (
        f"Archive Path: {archive_path}\n"
        f"Current Length (excluding deleted): {current_length}\n"
        f"Total Length (including deleted): {total_length}\n"
        f"Last Updated At: {last_updated}\n"
        f"Oldest Record: {first_updated}\n"
    )

    return info_string


def undo_changes(
    archive_path: str, start_at: str = None, end_at: str = None, commit: bool = False
) -> None:
    """Rolls back changes in the archive that occurred between the given timestamps. This means that
    anything updated within this time will be dropped, and anything deleted in this time will have
    its delete timestamp removed while preserved its last updated timestamp. If commit is False
    (default), then just prints a summary of how many updates and deletes will be rolled back."""

    archive_df = get_data(archive_path, include_timestamps=True, include_deleted=True)

    if archive_df.empty:
        print(f"No data found in archive at {archive_path}")
        return

    # TODO: this is still overflowing?
    # Parse the start_at and end_at dates
    start_at = _parse_to_denver_time(
        start_at, default=pd.Timestamp.min.tz_localize("America/Denver")
    )
    end_at = _parse_to_denver_time(
        end_at, default=pd.Timestamp.max.tz_localize("America/Denver")
    )

    # Set up filter for dropping rows added during the window
    undo_adds = (archive_df["archive_last_updated_at"] >= start_at) & (
        archive_df["archive_last_updated_at"] <= end_at
    )
    # Set up filter for removing delete timestamps during the window
    undo_deletes = (
        (archive_df["archive_deleted_at"] >= start_at)
        & (archive_df["archive_deleted_at"] <= end_at)
        # Some rows may have been added AND deleted within the window. For these, just drop them
        # without undoing the delete timestamp: this mainly just matters for accurate counting
        & ~undo_adds
    )

    if commit:
        # Remove delete timestamps from deleted rows
        archive_df.loc[undo_deletes, "archive_deleted_at"] = pd.NaT
        # Remove updated rows
        archive_df = archive_df[~undo_adds]

        with disable_run_logger():
            # Update the archive
            data = io.BytesIO()
            archive_df.to_pickle(data)
            data.seek(0)
            put.fn(data, archive_path, archive_storage())

        print(
            f"Rolled back {sum(undo_adds)} updates and {sum(undo_deletes)} deletes in"
            f" archive at {archive_path}"
        )
    else:
        print(
            f"Found {sum(undo_adds)} updates and {sum(undo_deletes)} deletes to be rolled back in "
            f"archive at {archive_path}. To commit these changes, re-run with `commit=True`."
        )


def init(archive_path: str, overwrite: bool = False) -> None:
    """Creates an empty file at `archive_path`, representing an empty archive whose columns will
    end up matching whatever is the first dataset used to update it. If overwrite is False (default)
    and a file already exists at `archive_path`, then prints the results of `info` for it instead
    of replacing it with an empty file."""

    archive_df = get_data(archive_path, include_deleted=True)

    if not archive_df.empty and not overwrite:
        # If archive exists and overwrite is False, print info about the existing archive
        print(
            f"Archive already exists:\n{info(archive_path)}\n\nTo reset to empty archive, "
            "re-run with `overwrite=True`."
        )
    else:
        with disable_run_logger():
            put.fn(b"", archive_path, archive_storage())
        print(f"Initialized empty archive at {archive_path}")


#####
# Utility functions
#####


def _match_columns(dataframe, archive, match_on=None):
    # If no columns are specified for matching, use all columns except the archive-specific ones
    if match_on is None:
        archive_cols = sorted(archive.columns.tolist())
        new_cols = sorted(dataframe.columns.tolist())
        if archive_cols != new_cols:
            raise ValueError(
                f"Dataframe columns do not match archived columns: Dataframe - {new_cols}"
                f" - Archived: {archive_cols}"
            )
        columns = new_cols
    else:
        columns = match_on
    # After identifying columns, ensure there are no duplicates on those columns
    dataframe_dupes = sum(dataframe[columns].duplicated())
    archive_dupes = sum(archive[columns].duplicated())
    if dataframe_dupes or archive_dupes:
        raise ValueError(
            f"Cannot match current dataset with archive on {columns} due to duplicate values. "
            f"Current dataframe has {dataframe_dupes} duplicate values and archive "
            f"has {archive_dupes}."
        )
    return columns


def _parse_to_denver_time(date_str, default=None):
    if date_str is None:
        return default

    timestamp = parser.parse(date_str)
    denver_tz = pytz.timezone("America/Denver")

    # If the datetime object is timezone aware, convert to America/Denver time
    if (
        timestamp.tzinfo is not None
        and timestamp.tzinfo.utcoffset(timestamp) is not None
    ):
        timestamp = timestamp.astimezone(denver_tz)
    else:
        # If it's naive, assume it's in America/Denver time
        timestamp = denver_tz.localize(timestamp)

    return timestamp
