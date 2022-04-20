"""Tasks for connecting to databases"""

import prefect
import cx_Oracle
from prefect import task
import pandas as pd

# Utility function for handling SIDs with Oracle connections
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

@task
def oracle_extract(sql_query: str, connection_info: dict) -> pd.DataFrame:
    """Returns a DataFrame derived from a SQL SELECT statement executed against the given
    database. The dataset_name provides additional readability for code and logging. The KVs
    of connection_info should match the keyword arguments passed to cx_Oracle.connect, with
    "dsn" being the "easy connection string" (see Oracle docs). Be sure to give the password
    as a separate field, not in the DSN. Or pass "host", "port", and "sid" individually. Connection
    encoding is automatically set to utf-8 if missing."""

    _make_oracle_dsn(connection_info)
    if 'encoding' not in connection_info:
        connection_info['encoding'] = 'UTF-8'
    conn = cx_Oracle.connect(**connection_info)
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
                line_no = len(sql_query[:offset].split('\n'))
                line = sql_query[:offset].split('\n')[-1] + '█' + sql_query[offset:].split('\n')[0]
                prefect.context.get('logger').error(
                    f'Oracle database error! {exc}\nLine {line_no}: {line[:100]}')
            raise
        raise
    prefect.context.get('logger').info(f"Read {len(data.index)} rows from {host}: {sql_snip}")
    return data
