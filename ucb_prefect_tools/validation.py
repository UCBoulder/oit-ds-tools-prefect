import pandas as pd
import pandera as pa
from pandera.io import from_yaml
from prefect import task, get_run_logger
from os import PathLike
import json

def generate_schema(df: pd.DataFrame, path: PathLike | None = None):
    """Generates a new schema inferred from a dataframe and saves it as a YAML.
    The schemas generated from this function are rough drafts, intended as
    a starting point for further iteration by hand.

    If a path is not provided, the schema will be output as a YAML-formatted string."""
    schema = pa.infer_schema(df)
    return schema.to_yaml(path)

@task
def validate_dataframe(df: pd.DataFrame, schema_path: PathLike, warn=False):
    """Lazily validates target dataframe using a schema file, and throws an error report if violations are detected.
    If warn=True, will instead write the report as a warning to the run logger."""
    schema = from_yaml(schema_path)
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        if warn:
            get_run_logger().warning(json.dumps(exc.message, indent=2))
        else:
            raise exc
