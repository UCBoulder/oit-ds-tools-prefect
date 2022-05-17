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
import pandas as pd
import numpy as np

from . import util

# System-agnostic tasks

@task(name="database.sql_extract")
def sql_extract(sql_query: str,
                connection_info: dict,
                query_params=None,
                lob_columns: list =None) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database, with query_params specifying the values of bind variables, and lob_columns specifying
    any LOB-type columns which should have their `read` methods called to extract literal data.
    Column names are automatically converted to lowercase."""

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_sql_extract)
    dataframe = function(sql_query, info, query_params, lob_columns)
    dataframe.columns = [i.lower() for i in dataframe.columns]
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
                       lob_columns: list =None) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Connection encoding is automatically set to utf-8 if missing. The query_params
    argument specifies values for bind variables in the query. The lob_columns argument specifies
    LOB-type columns which should have their `read` methods called to extract literal data."""

    if query_params is None:
        query_params = {}
    if lob_columns is None:
        lob_columns = []
    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    with cx_Oracle.connect(**connection_info) as conn:
        try:
            host = conn.dsn.split('HOST=')[1].split(')')[0]
        except IndexError:
            host = 'UNKNOWN'
        sql_snip = ' '.join(sql_query.split())[:100] + ' ...'
        try_again = False
        try:
            data = pd.read_sql_query(sql_query, conn, params=query_params)
        except pd.io.sql.DatabaseError:
            # Get the line number of the error from the Oracle library
            try_again = True
        if try_again:
            try:
                conn.cursor().execute(sql_query, parameters=query_params)
            except cx_Oracle.DatabaseError as exc:
                try:
                    offset = exc.args[0].offset
                except (IndexError, AttributeError):
                    pass
                else:
                    prefect.context.get('logger').error(
                        f'Oracle: Database error - {exc}\n{_sql_error(sql_query, offset)}')
                raise
    log_str = f"Oracle: Read {len(data.index)} rows from {host}: {sql_snip}"
    if query_params:
        log_str += f'\nwith injected params: {query_params}'
    prefect.context.get('logger').info(log_str)
    for column in lob_columns:
        prefect.context.get('logger').info(
            f'Reading data from LOB column {column}')
        data[column] = data[column].apply(lambda x: x.read())
    util.record_source('oracle', host, sum(data.memory_usage()))
    return data

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
        with conn.cursor() as cursor:
            if kill_and_fill:
                cursor.execute('TRUNCATE TABLE :table', [table_identifier])

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
        f"Oracle: Inserted {len(records) - errors} rows into {table_identifier} on {host}")
    util.record_sink('oracle', host, sum(dataframe.memory_usage()))
