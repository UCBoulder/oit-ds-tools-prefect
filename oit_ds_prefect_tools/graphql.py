"""Tasks for connecting to GraphQL APIs.

Each Prefect task takes a connection_info argument which is a dict identifying the endpoint to
connect to. It should always have an "endpoint" argument identifying the URL to POST to. The
remaining KVs of connection_info should correspond to the HTTP headers to send (e.g. for
authentication). The "content-type" header is automatically set to "application/json".
"""

from pprint import pformat
from typing import Callable

import prefect
from prefect import task
import requests

from . import util

class GraphQLError(Exception):
    """Exception for when GraphQL response lists errors"""

    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors

def _graphql_query(request):
    response = requests.post(**request)
    response.raise_for_status()
    result = response.json()
    size = len(response.content)
    if 'errors' in result:
        errors = result['errors']
        message = f'Response listed {len(errors)} errors:\n'
        message += '\n'.join(pformat(i) for i in errors[:5])
        if len(errors) > 5:
            message += f'\n(Plus {len(errors) - 5} more)'
        message += f'\n\nRequest JSON:\n{pformat(request["json"])}'
        raise GraphQLError(message, errors)
    return result['data'], size

@task(name="graphql.query")
def query(query_str: str,
          connection_info: dict,
          variables: dict =None,
          operation_name: str =None,
          chunk_variable: str =None,
          chunksize: int =100,
          next_variables_getter: Callable =None):
    """POSTs a GraphQL query or mutation and returns the "data" entry of the response.

    If chunk_variable is given, this is the name of a list-like variable which will be split into
    chunks of chunksize, with a separate request sent for each chunk.

    If next_variables_getter is given, this is a function which will take the "data" entry of the
    response. If it returns a dict, then the query will be POSTED again using this dict as the
    new variables param (i.e. to get the next page of data). If it returns None or {}, the task
    ends.

    With either of these options, a list of "data" response entries is returned instead of a
    singular. The two options are mutually exclusive.
    """

    # pylint:disable=too-many-locals
    # pylint:disable=too-many-arguments
    if chunk_variable:
        to_chunk = variables[chunk_variable]
        current_vars = variables.copy()
        current_vars[chunk_variable] = to_chunk[:chunksize]
        def chunk_iter():
            base_vars = variables.copy()
            for i in range(chunksize, len(to_chunk), chunksize):
                base_vars[chunk_variable] = to_chunk[i:i + chunksize]
                yield base_vars
        chunks = chunk_iter()
        def chunk_getter(_):
            try:
                return next(chunks)
            except StopIteration:
                return None
        next_vars = chunk_getter
    elif next_variables_getter:
        current_vars = variables
        next_vars = next_variables_getter
    else:
        current_vars = variables
        next_vars = lambda _: None

    message = f'GraphQL: Reading from {connection_info["endpoint"]}: {query_str[:200]} ...'
    if operation_name:
        message += f'\nusing operation {operation_name}'
    if current_vars:
        message += f'\nwith variables {current_vars}'
    prefect.context.get('logger').info(message)

    request = {'url': connection_info['endpoint']}
    request['headers'] = {k:v for k, v in connection_info.items() if k != 'endpoint'}
    request['json'] = {'query': query_str}
    if operation_name:
        request['json']['operationName'] = operation_name

    result_data = []
    total_size = 0
    while current_vars or not result_data:
        if current_vars:
            request['json']['variables'] = current_vars
        data, size = _graphql_query(request)
        result_data.append(data)
        total_size += size
        current_vars = next_vars(data)

    message = f'GraphQL: Read {size} bytes'
    if len(result_data) > 1:
        message += ' from {len(result_data)} requests'
    else:
        result_data = result_data[0]
    prefect.context.get('logger').info(message)
    util.record_pull('graphql', connection_info['endpoint'], size)
    return result_data
