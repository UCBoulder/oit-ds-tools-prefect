"""Functions for validating dataframes and generating schemas for validation.

This uses Pandera, a framework for schematic validation of dataframes. For simpler validations, schemas can be
represented as a YAML file which can be read in at validation time. If custom checks or complex validations are
desired, schemas can be generated as Python scripts and provided to the validation task as objects."""

import pandas as pd
import pandera as pa
from pandera.io import from_yaml
from prefect import task, get_run_logger
from os import PathLike
import json


def generate_schema(df: pd.DataFrame, to_script=False, path: PathLike | None = None):
    """Generates a new schema inferred from a dataframe and saves it as a Python script or YAML file.
    The schemas generated from this function are rough drafts, intended as a starting point for further
    iteration by hand. For schemas that will use more complex checks, it is recommended to save as a script.

    If a path is not provided, the schema will be output as a YAML-formatted string.
    This is not implemented for scripts."""
    schema = pa.infer_schema(df)
    if to_script:
        if not path:
            raise NotImplementedError(
                "Schemas in script form must be written to a file, please specify a path"
            )
        schema.to_script(path)
        return None
    return schema.to_yaml(path)


@task
def validate_dataframe(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema = None,
    yaml_path: PathLike = None,
    warn=False,
):
    """Lazily validates target dataframe using a schema or schema file, and throws an error report if violations are detected.
    If warn=True, will instead write the report as a warning to the run logger."""
    if not schema:
        if not yaml_path:
            raise ValueError(
                "Must provide either a schema or a path to a YAML file containing a schema."
            )
        schema = from_yaml(yaml_path)
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        if warn:
            get_run_logger().warning(
                "WARNING - Dataframe failed validation:\n"
                + json.dumps(exc.message, indent=2)
            )
        else:
            raise exc
