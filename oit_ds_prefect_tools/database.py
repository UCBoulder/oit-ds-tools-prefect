"""Tasks for connecting to databases.

Each Prefect task takes a connection_info argument which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported system:
    - "oracle" for cx_Oracle.connect

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For oracle, if your system uses an SID instead of a name, you cannot pass the easy connection
        string as the "dsn" argument, so instead pass "host", "port", and "sid" individually.
"""

import prefect
import cx_Oracle
from prefect import task
from prefect.engine import signals
import pandas as pd
import numpy as np

from . import util

# System-agnostic tasks

@task(name="database.sql_extract")
def sql_extract(sql_query: str,
                connection_info: dict,
                query_params=None,
                lob_columns: list =None,
                parquet_chunks_prefix: str =None,
                chunksize: int =1000) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Column names are accepted and returned in all lowercase.

    :param sql_query: The SELECT statement to get data with
    :param connection_info: Target system info; see this module's docstring
    :param query_params: List or dict specifying the values of bind variables for the query
    :param lob_columns: Names of columns containing LOB-type data that must be read as an
        additional step
    :param parquet_chunks_prefix: If given, saves the query results in chunks as Parquet files
        with the given prefix (including path) to local disk. Files are named "{prefix}_0.parquet",
        etc. Intended for queries too large to load into memory.
    :param chunksize: How many rows to load into memory at a time. If parquet_chunks_prefix is
        given, this also determines rows per Parquet file.
    :return: Either a DataFrame result or the number of Parquet files created
    """

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_sql_extract)
    dataframe = function(sql_query, info, query_params, lob_columns, parquet_chunks_prefix,
                         chunksize)
    return dataframe

@task(name="database.insert")
def insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        kill_and_fill: bool =False) -> pd.DataFrame:
    """Takes a dataframe and table identifier (schema.table) and appends the data into that table.
    If kill_and_fill is true, deletes all rows from thet able before inserting. Dataframe columns
    must match table column names.
    """

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_insert)
    return function(dataframe, table_identifier, info, kill_and_fill)

def _switch(connection_info, **kwargs):
    for key, value in kwargs.items():
        if connection_info['system_type'] == key:
            del connection_info['system_type']
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')

def _sql_error(sql_query, offset):
    line_no = len(sql_query[:offset].split('\n'))
    line = sql_query[:offset].split('\n')[-1] + '█' + sql_query[offset:].split('\n')[0]
    return f'Line {line_no}: {line[:100]}'


# Oracle functions

def _make_oracle_dsn(connection_info):
    if 'sid' in connection_info:
        if 'port' in connection_info:
            port = connection_info['port']
            del connection_info['port']
        else:
            port = 1521
        dsn = cx_Oracle.makedsn(connection_info['host'], port, connection_info['sid'])
        connection_info['dsn'] = dsn
        del connection_info['host']
        del connection_info['sid']

def oracle_sql_extract(sql_query: str,
                       connection_info: dict,
                       query_params=None,
                       lob_columns: list =None,
                       parquet_chunks_prefix: str =None,
                       chunksize: int =1000) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Connection encoding is automatically set to utf-8 if missing."""

    if lob_columns is None:
        lob_columns = []
    else:
        lob_columns = [i.lower() for i in lob_columns]
    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    with cx_Oracle.connect(**connection_info) as conn:
        try:
            host = conn.dsn.split('HOST=')[1].split(')')[0]
        except IndexError:
            host = 'UNKNOWN'
        sql_snip = ' '.join(sql_query.split())[:200] + ' ...'
        log_str = f"Oracle: Reading from {host}: {sql_snip}"
        if query_params:
            log_str += f'\nwith injected params: {query_params}'
        prefect.context.get('logger').info(log_str)

        cursor = conn.cursor()
        cursor.arraysize = chunksize
        try:
            if query_params:
                cursor.execute(sql_query, parameters=query_params)
            else:
                cursor.execute(sql_query)
            columns = [i[0].lower() for i in cursor.description]

            if parquet_chunks_prefix:
                count = 0
                size = 0
                index = 0
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    data = pd.DataFrame(rows, columns=columns)
                    count += len(data.index)
                    for column in lob_columns:
                        data[column] = data[column].map(lambda x: x.read() if x else None)
                    size += sum(data.memory_usage())
                    data.to_parquet(f'{parquet_chunks_prefix}_{index}.parquet')
                    index += 1
            else:
                rows = cursor.fetchall()
                data = pd.DataFrame(rows, columns=columns)
                count = len(data.index)
                for column in lob_columns:
                    prefect.context.get('logger').info(
                        f'Reading data from LOB column {column}')
                    data[column] = data[column].map(lambda x: x.read() if x else None)
                size = sum(data.memory_usage())

            util.record_pull('oracle', host, size)
            prefect.context.get('logger').info(f'Oracle: Read {count} rows')
            if parquet_chunks_prefix:
                return index
            return data

        except cx_Oracle.DatabaseError as exc:
            try:
                offset = exc.args[0].offset
            except (IndexError, AttributeError):
                pass
            else:
                prefect.context.get('logger').error(
                    f'Oracle: Database error - {exc}\n{_sql_error(sql_query, offset)}')
            raise

def oracle_insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        kill_and_fill: bool =False) -> pd.DataFrame:
    """Takes a dataframe and table identifier (schema.table) and inserts the data into that table.
    If kill_and_fill is true, deletes all rows from the table before inserting. Dataframe columns
    must match table column names.
    """

    batch_size = 500
    errors = 0
    insert_sql = (f'INSERT INTO {table_identifier} ({",".join(list(dataframe.columns))}) ' +
                  f'VALUES ({",".join(":" + i for i in dataframe.columns)})')
    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'

    # Replace NA values with None and turn to list of dicts
    records = dataframe.fillna(np.nan).replace([np.nan], [None]).to_dict('records')

    with cx_Oracle.connect(**connection_info) as conn:
        try:
            host = conn.dsn.split('HOST=')[1].split(')')[0]
        except IndexError:
            host = 'UNKNOWN'
        prefect.context.get('logger').info(
            f"Oracle: Inserting into {table_identifier} on {host}")
        with conn.cursor() as cursor:
            if kill_and_fill:
                cursor.execute(f'TRUNCATE TABLE {table_identifier}')

            # Insert records in batches
            for start in range(0, len(records), batch_size):
                to_insert = records[start : start + batch_size]
                cursor.executemany(insert_sql, to_insert, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                for error in batch_errors[:10 - errors]:
                    prefect.context.get('logger').error(
                        f'Oracle: Database error {error.message} while inserting data '
                        f'{to_insert[error.offset]}')
                errors += len(batch_errors)
            conn.commit()

    # Logging
    if errors > 10:
        prefect.context.get('logger').error(
            f'Oracle: {errors - 10} more database errors while inserting not shown')
    prefect.context.get('logger').info(
        f"Oracle: Inserted {len(records) - errors} rows")
    util.record_push('oracle', host, sum(dataframe.memory_usage()))
    if errors:
        raise signals.FAIL(f'Failed to insert {errors} records')
