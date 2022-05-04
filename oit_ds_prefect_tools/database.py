"""Tasks for connecting to databases"""

import prefect
import cx_Oracle
from prefect import task
import pandas as pd
import numpy as np

from . import util

# System-agnostic tasks

@task
def sql_extract(sql_query: str, connection_info: dict) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Currently only Oracle databases are supported: see oracle_sql_extract for details."""

    return oracle_sql_extract(sql_query, connection_info)

@task
def insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        replace_existing: bool =False) -> pd.DataFrame:
    """Takes a dataframe and table identifier (schema.table) and appends the data into that table.
    If kill_and_fill is true, deletes all rows from thet able before inserting. Dataframe columns
    must match table column names.
    """

    return oracle_insert(dataframe, table_identifier, connection_info, replace_existing)

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

def oracle_sql_extract(sql_query: str, connection_info: dict) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. The KVs of connection_info should match the keyword arguments passed to
    cx_Oracle.connect, with "dsn" being the "easy connection string" (see Oracle docs). Be sure to
    give the password as a separate field, not in the DSN. Or pass "host", "port", and "sid"
    individually. Connection encoding is automatically set to utf-8 if missing."""

    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    with cx_Oracle.connect(**connection_info) as conn:
        try:
            host = conn.dsn.split('HOST=')[1].split(')')[0]
        except IndexError:
            host = 'UNKNOWN'
        sql_snip = ' '.join(sql_query.split())[:50]
        try:
            data = pd.read_sql_query(sql_query, conn)
        except pd.io.sql.DatabaseError:
            # Can't get detailed error information from this, so reproduce with oracle library
            try:
                conn.cursor().execute(sql_query)
            except cx_Oracle.DatabaseError as exc:
                try:
                    offset = exc.args[0].offset
                except (IndexError, AttributeError):
                    pass
                else:
                    prefect.context.get('logger').error(
                        f'Oracle: Database error - {exc}\n{_sql_error(sql_query, offset)}')
                raise
            raise
    prefect.context.get('logger').info(
        f"Oracle: Read {len(data.index)} rows from {host}: {sql_snip}")
    util.record_source('oracle', host, sum(data.memory_usage()))
    return data

def oracle_insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        kill_and_fill: bool =False) -> pd.DataFrame:
    """Takes a dataframe and table identifier (schema.table) and inserts the data into that table.
    If kill_and_fill is true, deletes all rows from thet able before inserting. Dataframe columns
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
