"""Tasks for connecting to REST APIs.

Each Prefect task takes a connection_info argument which is a dict identifying the endpoint to
connect to. It should always have a "domain" key identifying the domain to send the request to.
For example, to send a request to "https://canvas.colorado.edu/api/v1/users/self", set
`connection_info["domain"] = "https://canvas.colorado.edu"` and pass "/api/v1/users/self" for the
`endpoint` param.

The remaining KVs of connection_info should correspond to the HTTP headers to send (e.g. for
authentication), and/or you can include an "auth" key set to a tuple or list of length 2 to use
Basic Authentication. These values are simply passed to the requests module's function as args.
"""

from typing import Callable

import prefect
from prefect import task
from prefect.engine import signals
import requests
import pandas as pd

from . import util
from .util import sizeof_fmt

@task(name='rest.get')
def get(endpoint: str,
        connection_info: dict,
        params: dict =None,
        next_page_getter: Callable =None,
        to_dataframe: bool =False,
        codes_to_skip: list =None):
    """Sends a GET request to the specified endpoint, along with any params, and returns the
    JSON results.

    For paginated data, supply a function to next_page_getter that takes a requests.response object
    and returns either the str endpoint of the next page of data or a dict of params to update the
    initial params with. The next_page_getter should return None when where are no more pages.
    Paginated data is returned as a list of JSON results: if each result is a list, these are
    concatenated together to form a single list.

    If to_dataframe is true, the JSON results will be used to initialize a pandas.Dataframe to be
    returned instead.

    If any of the codes (from the requests module) included in the codes_to_skip argument are
    returned in the http response, a SKIP signal will be raised instead of an exception.
    """
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-locals

    if not next_page_getter:
        next_page_getter = lambda _: None
    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    prefect.context.get('logger').info(f'REST: Sending GET to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        kwargs['auth'] = auth
    if params:
        kwargs['params'] = params
    size = 0
    data = []
    while True:
        response = requests.get(url, **kwargs)
        if codes_to_skip and response.status_code in codes_to_skip:
            raise signals.SKIP(
                f'Received {response.status_code} response; skipping task: {response.text}')
        response.raise_for_status()
        size += len(response.content)
        result = response.json()
        if isinstance(result, list):
            data += result
        else:
            data.append(result)
        next_page_info = next_page_getter(response)
        if not next_page_info:
            break
        if isinstance(next_page_info, str):
            url = next_page_info
        elif isinstance(next_page_info, dict):
            kwargs['params'].update(next_page_info)
        else:
            raise TypeError(
                f'Param next_page_getter must return a str or dict, not {type(next_page_info)}')

    prefect.context.get('logger').info(f'REST: Received {len(data)} objects')
    util.record_pull('rest', domain, size)
    if to_dataframe:
        return pd.DataFrame(data)
    return data

@task(name='rest.post')
def post(endpoint: str, connection_info: dict, data=None, json=None, files=None):
    """Sends a POST request along with any data and returns the JSON response. See requests.post
    for more details."""

    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    prefect.context.get('logger').info(f'REST: Sending POST to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        kwargs['auth'] = auth
    if data:
        kwargs['data'] = data
    if json:
        kwargs['json'] = json
    if files:
        kwargs['files'] = files
    response = requests.post(url, **kwargs)
    response.raise_for_status()
    size = len(response.request.body)
    prefect.context.get('logger').info(f'REST: Sent {sizeof_fmt(size)} bytes')
    util.record_push('rest', domain, size)
    return response.json()

@task(name='rest.put')
def put(endpoint: str, connection_info: dict, data=None, json=None, files=None):
    """Sends a PUT request along with any data and returns the JSON response. See requests.put
    for more details."""

    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    prefect.context.get('logger').info(f'REST: Sending PUT to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        kwargs['auth'] = auth
    if data:
        kwargs['data'] = data
    if json:
        kwargs['json'] = json
    if files:
        kwargs['files'] = files
    response = requests.put(url, **kwargs)
    response.raise_for_status()
    size = len(response.request.body)
    prefect.context.get('logger').info(f'REST: Sent {sizeof_fmt(size)} bytes')
    util.record_push('rest', domain, size)
    return response.json()

@task(name='rest.delete')
def delete(endpoint: str, connection_info: dict):
    """Sends a DELETE request and returns the JSON response. See requests.delete for more details.
    """

    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    prefect.context.get('logger').info(f'REST: Sending DELETE to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        kwargs['auth'] = auth
    response = requests.delete(url, **kwargs)
    response.raise_for_status()
    return response.json()
