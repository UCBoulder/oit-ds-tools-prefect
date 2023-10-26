"""Tool for comparing two dataframes for testing and validation"""

import os
import itertools
from typing import Callable, Tuple

import pandas as pd


def _list2str(listlike):
    return ", ".join(list(listlike))


def _subsets(items, max_size=10):
    result = []
    max_size = min(max_size, len(items) - 1)
    for i in range(1, max_size + 1):
        result.extend([list(i) for i in itertools.combinations(items, i)])
    return result


def _standardize(dataframe):
    dataframe = dataframe.copy()
    for col in dataframe.columns.tolist():
        try:
            dataframe[col] = dataframe[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
            continue
        except AttributeError:
            pass
        # Not a datetime, try number
        try:
            dataframe[col] = dataframe[col].round(2).astype("Float64")
            continue
        except (AttributeError, TypeError):
            pass
        # Not a number, continue
    # Now convert everything to standardized strings
    dataframe = dataframe.astype("object").fillna("").astype("string")
    return dataframe.drop_duplicates()


def _changes_in_column(merged_df, column, top_n):
    diff = merged_df[merged_df[f"{column}_x"] != merged_df[f"{column}_y"]]
    if len(diff):
        combos = column + ": " + diff[f"{column}_x"] + " => " + diff[f"{column}_y"]
        data = pd.DataFrame({f"change in {column}": combos, "count": 1})
        return dict(
            data.groupby(f"change in {column}")["count"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .head(top_n)
            .values.tolist()
        )
    return {}


def auto_merge(left: pd.DataFrame, right: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Attempts to merge the two dataframes by auto-identifying distinguishing key columns. Returns
    the merged dataframe and a dict of metrics related to the process. Assumes the two dataframes
    have identical columns."""

    best = 0
    key_columns = right.columns.tolist()
    goal = len(right.drop_duplicates())
    for candidates in _subsets(right.columns.tolist(), max_size=4):
        distinct_count = len(right[candidates].drop_duplicates())
        if distinct_count > best:
            best = distinct_count
            key_columns = candidates
            if distinct_count >= goal:
                break
    merged = left.merge(right, on=key_columns, how="outer", indicator=True)
    left_only = len(merged[merged["_merge"] == "left_only"])
    right_only = len(merged[merged["_merge"] == "right_only"])
    both = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
    metrics = {"merged_rows": len(both), "merge_key_columns": _list2str(key_columns)}
    if left_only:
        metrics["unmerged_left_rows"] = left_only
    if right_only:
        metrics["unmerged_right_rows"] = right_only
    return both, metrics


def reconcile(
    left: pd.DataFrame, right: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Returns both dataframes with only matching columns, duplicates removed, and values
    standardized to comparable strings. Also returns a dict with various metrics gathered
    during this process"""

    metrics = {}
    if right.index.names[0]:
        metrics["right_index_reset"] = _list2str(right.index.names)
        right = right.reset_index()
    if left.index.names[0]:
        metrics["left_index_reset"] = _list2str(left.index.names)
        left = left.reset_index()
    left_only_columns = [
        i for i in left.columns.tolist() if i not in right.columns.tolist()
    ]
    right_only_columns = [
        i for i in right.columns.tolist() if i not in left.columns.tolist()
    ]
    columns = [i for i in left.columns.tolist() if i in right.columns.tolist()]
    if left_only_columns:
        metrics["left_only_columns"] = _list2str(left_only_columns)
    if right_only_columns:
        metrics["right_only_columns"] = _list2str(right_only_columns)
    if not columns:
        metrics["coerced_merge"] = True
        left.columns = right.columns.tolist()
        columns = right.columns.tolist()
    new_left = _standardize(left[columns])
    if len(new_left) < len(left):
        metrics["left_duplicates"] = len(left) - len(new_left)
    new_right = _standardize(right[columns])
    if len(new_right) < len(right):
        metrics["right_duplicates"] = len(right) - len(new_right)
    return new_left, new_right, metrics


def compare_column(left: pd.DataFrame, right: pd.DataFrame, column: str) -> dict:
    """Generates a detailed break down of the differences between two dataframes on a specific
    column. Returns this data as a dict that also includes various metrics collected throughout
    this process"""

    left, right, reconcile_metrics = reconcile(left, right)
    merged, merge_metrics = auto_merge(left, right)
    changes = _changes_in_column(merged, column, 10)
    out = {}
    out.update(reconcile_metrics)
    out.update(merge_metrics)
    out.update(changes)
    return out


def compare(left: pd.DataFrame, right: pd.DataFrame) -> dict:
    """Compares the two dataframes and returns a summary of differences"""

    left, right, metrics = reconcile(left, right)

    # Compare
    merged = left.merge(right, on=right.columns.tolist(), how="outer", indicator=True)
    left_cnt = len(merged[merged["_merge"] == "left_only"])
    right_cnt = len(merged[merged["_merge"] == "right_only"])
    both_cnt = len(merged[merged["_merge"] == "both"])
    metrics["left_only_rows"] = left_cnt
    metrics["right_only_rows"] = right_cnt
    metrics["matched_rows"] = both_cnt

    # If different, merge for further comparison
    if left_cnt or right_cnt:
        merged, merge_metrics = auto_merge(left, right)
        metrics.update(merge_metrics)
    else:
        return metrics

    # Get a detailed breakdown of differences
    if not merged.empty:
        for column in [
            i.removesuffix("_x") for i in merged.columns.tolist() if i.endswith("_x")
        ]:
            metrics.update(_changes_in_column(merged, column, 5))
    return metrics


def bulk_compare(
    left_filepaths: list, right_filepaths: list, df_loader: Callable
) -> pd.DataFrame:
    """Compares left files with right files in order, loading the files into DataFrames using the
    df_loader callable. Returns a DataFrame with columns filename, measure, value giving the metrics
    collected for each pair of files with a matching filename."""

    left_filenames = [os.path.basename(i) for i in left_filepaths]
    right_filenames = [os.path.basename(i) for i in right_filepaths]
    left_only = [i for i in left_filenames if i not in right_filenames]
    right_only = [i for i in right_filenames if i not in left_filenames]
    shared_left_filepaths = [
        p for p, n in zip(left_filepaths, left_filenames) if n in right_filenames
    ]
    shared_right_filepaths = [
        p for p, n in zip(right_filepaths, right_filenames) if n in left_filenames
    ]
    out = pd.DataFrame(data=[], columns=["filename", "measure", "value"])
    if left_only:
        out.loc[len(out.index)] = [None, "left_only_files", _list2str(left_only)]
    if right_only:
        out.loc[len(out.index)] = [None, "right_only_files", _list2str(right_only)]
    for left, right in zip(shared_left_filepaths, shared_right_filepaths):
        metrics = compare(df_loader(left), df_loader(right))
        new_rows = pd.DataFrame(
            {
                "filename": os.path.basename(left),
                "measure": metrics.keys(),
                "value": metrics.values(),
            }
        )
        out = pd.concat([out, new_rows], ignore_index=True)
    return out
