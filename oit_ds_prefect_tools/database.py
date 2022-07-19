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

from . import util

# System-agnostic tasks

@task(name="database.sql_extract")
def sql_extract(sql_query: str,
                connection_info: dict,
                query_params=None,
                lob_columns: list =None,
                chunks_prefix: str =None,
                chunksize: int =1000) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Column names are accepted and returned in all lowercase.

    :param sql_query: The SELECT statement to get data with
    :param connection_info: Target system info; see this module's docstring
    :param query_params: List or dict specifying the values of bind variables for the query
    :param lob_columns: Names of columns containing LOB-type data that must be read as an
        additional step
    :param chunks_prefix: If given, saves the query results in chunks as pickled files
        with the given prefix (including path) to local disk. Files are named "{prefix}_0",
        etc. and can be read with pandas.read_pickle. Intended for queries too large to load into
        memory.
    :param chunksize: How many rows to load into memory at a time. If chunks_prefix is
        given, this also determines rows per Parquet file.
    :return: Either a DataFrame result or a list of pickle filenames
    """
    # pylint:disable=too-many-arguments

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_sql_extract)
    dataframe = function(sql_query, info, query_params, lob_columns, chunks_prefix,
                         chunksize)
    return dataframe

@task(name="database.insert")
def insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        pre_insert_statements: list[str] =None,
        pre_insert_params: list =None,
        max_error_proportion: float =.05,
        next_val_columns: list[str] =None) -> pd.DataFrame:
    """Takes a dataframe and table identifier and appends the data into that table.
    Dataframe columns must match table column names (case insensitive, order irrelevant).

    :param dataframe: The data to insert
    :param table_identifier: The table to insert into (`schema.table`)
    :param connection_info: The database connection info dict (see module docstring)
    :param pre_insert_statements: An optional list of sql statements to execute before inserting;
        for example, to delete rows
    :param pre_insert_params: An optional list of query parameters (lists or dicts) to go along with
        each pre-insert statement (aka bind variables)
    :param max_error_proportion: If the proportion of failed insert rows is greater than this, the
        entire transaction is rolled back (including pre-insert statements) and the task fails
    :param next_val_columns: A list of column names which are not in the dataframe, but which should
        be inserted into the database table using each column's "nextval" function (i.e. filling
        it with incremental values)
    """
    # pylint:disable=too-many-arguments

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_insert)
    return function(dataframe,
                    table_identifier,
                    info,
                    pre_insert_statements,
                    pre_insert_params,
                    max_error_proportion,
                    next_val_columns)

@task(name="database.execute_sql")
def execute_sql(sql_statement: str, connection_info: dict, query_params=None):
    """Executes the given SQL statement, with an optional list or dict specifying the values of
    bind variables for the query."""

    info = connection_info.copy()
    function = _switch(info,
                       oracle=oracle_execute_sql)
    return function(sql_statement, info, query_params)

def _switch(connection_info, **kwargs):
    for key, value in kwargs.items():
        if connection_info['system_type'] == key:
            del connection_info['system_type']
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')

def _log_oracle_error(error, sql_query):
    try:
        offset = error.args[0].offset
    except (IndexError, AttributeError):
        pass
    else:
        line_no = len(sql_query[:offset].split('\n'))
        line = sql_query[:offset].split('\n')[-1] + '█' + sql_query[offset:].split('\n')[0]
        message = f'Line {line_no}: {line[:100]}'
        prefect.context.get('logger').error(
            f'Oracle: Database error - {error}\n{message}')

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

def _oracle_host(dsn_string):
    try:
        # First try it with Net Connect Descriptor String
        return dsn_string.split('HOST')[1].split(')')[0].replace('=', '').strip()
    except IndexError:
        # Now try it using Easy Connect syntax
        return dsn_string.split('/')[0].split(':')[0].strip()

def oracle_sql_extract(sql_query: str,
                       connection_info: dict,
                       query_params=None,
                       lob_columns: list =None,
                       chunks_prefix: str =None,
                       chunksize: int =1000) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. Connection encoding is automatically set to utf-8 if missing."""
    # pylint:disable=too-many-statements
    # pylint:disable=too-many-branches
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments

    if lob_columns is None:
        lob_columns = []
    else:
        lob_columns = [i.lower() for i in lob_columns]
    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    with cx_Oracle.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
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

            if chunks_prefix:
                count = 0
                size = 0
                filenames = []
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    data = pd.DataFrame(rows, columns=columns)
                    count += len(data.index)
                    for column in lob_columns:
                        data[column] = data[column].map(lambda x: x.read() if x else None)
                    size += sum(data.memory_usage())
                    filename = f'{chunks_prefix}_{len(filenames)}'
                    filenames.append(filename)
                    data.to_pickle(filename)
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
            if chunks_prefix:
                return filenames
            return data

        except cx_Oracle.DatabaseError as exc:
            _log_oracle_error(exc, sql_query)
            raise

def oracle_insert(
        dataframe: pd.DataFrame,
        table_identifier: str,
        connection_info: dict,
        pre_insert_statements: list[str] =None,
        pre_insert_params: list =None,
        max_error_proportion: float =.05,
        next_val_columns: list[str] =None) -> pd.DataFrame:
    """Takes a dataframe and table identifier and appends the data into that table.
    Dataframe columns must match table column names (case insensitive, order irrelevant).

    :param dataframe: The data to insert
    :param table_identifier: The table to insert into (`schema.table`)
    :param connection_info: The database connection info dict (see module docstring)
    :param pre_insert_statements: An optional list of sql statements to execute before inserting;
        for example, to delete certain rows
    :param pre_insert_params: An optional list of query parameters (lists or dicts) to go along with
        each pre-insert statement (aka bind variables)
    :param max_error_proportion: If the proportion of failed insert rows is greater than this, the
        entire transaction is rolled back (including pre-insert statements) and the task fails
    :param next_val_columns: A list of column names which are not in the dataframe, but which should
        be inserted into the database table using each column's "nextval" function (i.e. filling
        it with incremental values)
    """
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments

    batch_size = 500
    errors = 0
    insert_sql = (f'INSERT INTO {table_identifier} ({",".join(list(dataframe.columns))}) ' +
                  f'VALUES ({",".join(":" + i for i in dataframe.columns)})')
    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    if pre_insert_statements is None:
        pre_insert_statements = []

    # Replace NA values with None and turn to list of dicts
    records = [{k:None if pd.isnull(v) else v for k, v in i.items()}
               for i in dataframe.to_dict('records')]

    with cx_Oracle.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        cursor = conn.cursor()

        _oracle_execute_statements(conn, cursor, host, pre_insert_statements, pre_insert_params)

        prefect.context.get('logger').info(
            f"Oracle: Inserting into {table_identifier} on {host}")
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
        error_proportion = float(errors) / float(len(records))
        if error_proportion > max_error_proportion:
            prefect.context.get('logger').error(
                f'{error_proportion:.0%} of insert actions failed, exceeding the set maximum '
                f'({max_error_proportion:.0%}); rolling back transaction.')
            conn.rollback()
        else:
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

def oracle_execute_sql(sql_statement, connection_info: dict, query_params=None):
    """Executes the given SQL statement str, with an optional list or dict specifying the values of
    bind variables for the query. Or, sql_statement can be a list of str and query_params a list
    of lists or dicts, in which case the statements will be executed in order as one transaction,
    rolling back if any fail."""

    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    if isinstance(sql_statement, str):
        sql_statement = [sql_statement]
        query_params = [query_params]

    with cx_Oracle.connect(**connection_info) as conn:
        host = _oracle_host(conn.dsn)
        cursor = conn.cursor()
        _oracle_execute_statements(conn, cursor, host, sql_statement, query_params)
        conn.commit()

def _oracle_execute_statements(conn, cursor, host, statements, query_params=None):
    if query_params is None:
        query_params = [None * len(statements)]
    try:
        for sql, params in zip(statements, query_params):
            sql_snip = ' '.join(sql.split())[:200] + ' ...'
            log_str = f"Oracle: Executing on {host}: {sql_snip}"
            if params:
                log_str += f'\nwith injected params: {params}'
            prefect.context.get('logger').info(log_str)
            if params:
                cursor.execute(sql, parameters=params)
            else:
                cursor.execute(sql)
    except cx_Oracle.DatabaseError as exc:
        _log_oracle_error(exc, sql)
        conn.rollback()
        raise
