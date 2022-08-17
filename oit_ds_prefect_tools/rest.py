"""Tasks for connecting to REST APIs.

Each Prefect task takes a connection_info argument which is a dict identifying the endpoint to
connect to. It should always have a "domain" key identifying the domain to send the request to.
For example, to send a request to "https://canvas.colorado.edu/api/v1/users/self", set
`connection_info["domain"] = "https://canvas.colorado.edu"` and pass "/api/v1/users/self" for the
`endpoint` param.

The remaining KVs of connection_info should correspond to the HTTP headers to send (e.g. for
authentication), and/or you can include an "auth" key set to a tuple or list of length 2 to use
Basic Authentication. These values are simply passed to the requests module's function as args.

Each tasks attempts to return a Python object from JSON results, but if results can't be JSONified,
then the raw bytes content is returned instead.
"""

from typing import Callable
from json.decoder import JSONDecodeError
from multiprocessing.pool import ThreadPool

import prefect
from prefect import task
import requests
import pandas as pd

from . import util
from .util import sizeof_fmt

@task(name='rest.get')
def get(endpoint: str,
        connection_info: dict,
        params: dict =None,
        next_page_getter: Callable =None,
        to_dataframe: bool =False) -> list:
    """Sends a GET request to the specified endpoint, along with any params, and returns a list
    of JSON results.

    For paginated data, supply a function to next_page_getter that takes a requests.response object
    and returns either the str endpoint of the next page of data or a dict of params to update the
    initial params with. The next_page_getter should return None when where are no more pages.
    Paginated data is returned as a list of JSON results: if each result is a list, these are
    concatenated together to form a single list.

    If to_dataframe is true, the JSON results will be used to initialize a pandas.Dataframe to be
    returned instead.
    """
    # pylint:disable=too-many-arguments

    return _get(endpoint, connection_info, params, next_page_getter, to_dataframe)

@task(name='rest.post')
def post(endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None):
    """Sends a POST request along with any data and returns the JSON response. See requests.post
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(requests.post, endpoint, connection_info, params, data, json, files)

@task(name='rest.put')
def put(endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None):
    """Sends a PUT request along with any data and returns the JSON response. See requests.put
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(requests.put, endpoint, connection_info, params, data, json, files)

@task(name='rest.patch')
def patch(endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None):
    """Sends a PATCH request along with any data and returns the JSON response. See requests.patch
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(
        requests.patch, endpoint, connection_info, params, data, json, files)

@task(name='rest.delete')
def delete(endpoint: str, connection_info: dict, params=None, data=None, json=None, files=None):
    """Sends a DELETE request along with any data and returns the JSON response. See requests.delete
    for more details."""
    # pylint:disable=too-many-arguments

    return _send_modify_request(requests.put, endpoint, connection_info, params, data, json, files)

@task(name='rest.get_many')
def get_many(endpoints: list[str],
        connection_info: dict,
        params_list: list[dict] =None,
        next_page_getter: Callable =None,
        to_dataframe: bool =False,
        codes_to_ignore: list =None,
        num_workers: int =1) -> list:
    """Sends many GET requests defined by a list of endpoints and a corresponding list of params
    dicts. Other params are applied to every GET request. Returns a list of lists (or a list of
    dataframes) comprising the result of each individual GET request. See rest.get task for more
    info.

    codes_to_ignore denotes HTTP response codes (from requests.codes) which will simply be omitted
    from the results instead of being raised as exceptions.

    If num_workers > 1, will use a ThreadPool with this many workers to send the requests. The
    order of results will be preserved. Note that this is superior to using get.map due to
    cleaner logs (Prefect Cloud only supports up to 10,000 log entries per flow).
    """
    # pylint:disable=too-many-arguments

    prefect.context.get('logger').info(
        f'REST: Sending {len(endpoints)} GET requests to {connection_info["domain"]} ...')
    if params_list is None:
        params_list = [None] * len(endpoints)
    if len(endpoints) != len(params_list):
        raise ValueError(f'Got {len(endpoints)} endpoints but {len(params_list)} params dicts')

    # single-threaded
    if num_workers == 1:
        results =  [
            _soft_catch(
                _get(endpoint, connection_info, params, next_page_getter, to_dataframe, log=False))
            for endpoint, params in zip(endpoints, params_list)
            ]

    # multi-threaded
    else:
        kwargs_list = [{'endpoint':endpoint,
                        'connection_info':connection_info,
                        'params':params,
                        'next_page_getter':next_page_getter,
                        'to_dataframe':to_dataframe,
                        'codes_to_ignore':codes_to_ignore,
                        'log':False}
                       for endpoint, params in zip(endpoints, params_list)]
        results = []
        count = 0
        with ThreadPool(num_workers) as pool:
            results_iter = pool.imap(_soft_catch(_get_mappable), kwargs_list)
            while True:
                try:
                    results.append(next(results_iter))
                    count += 1
                    if count % 1000 == 0:
                        prefect.context.get('logger').info(
                            f'Received {count} results out of {len(endpoints)} so far...')
                except StopIteration:
                    break

    # filter and return results
    filled_results = [i for i in results if not i is None]
    ignored = len(results) - len(filled_results)
    prefect.context.get('logger').info(
        f'REST: Received {len(filled_results)} responses, with {ignored} ignored')
    failures = [i for i in filled_results if isinstance(i, BaseException)]
    if failures:
        prefect.context.get('logger').error(
            f'REST: Encountered {len(failures)} exceptions while sending requests. Only the first '
            'will be raised.')
        raise failures[0]
    return filled_results

@task(name='rest.post_many')
def post_many(endpoints: list[str],
        connection_info: dict,
        params_list: list =None,
        data_list: list =None,
        json_list: list =None,
        files_list: list =None,
        num_workers: int =1) -> list:
    """Sends many POST requests along with any data and returns a list of JSON responses. See
    requests.post for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using post.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.post,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers)

@task(name='rest.put_many')
def put_many(endpoints: list[str],
        connection_info: dict,
        params_list: list =None,
        data_list: list =None,
        json_list: list =None,
        files_list: list =None,
        num_workers: int =1) -> list:
    """Sends many PUT requests along with any data and returns a list of JSON responses. See
    requests.put for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using put.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.put,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers)

@task(name='rest.patch_many')
def patch_many(endpoints: list[str],
        connection_info: dict,
        params_list: list =None,
        data_list: list =None,
        json_list: list =None,
        files_list: list =None,
        num_workers: int =1) -> list:
    """Sends many PATCH requests along with any data and returns a list of JSON responses. See
    requests.patch for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using patch.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.patch,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers)

@task(name='rest.delete_many')
def delete_many(endpoints: list[str],
        connection_info: dict,
        params_list: list =None,
        data_list: list =None,
        json_list: list =None,
        files_list: list =None,
        num_workers: int =1) -> list:
    """Sends many DELETE requests along with any data and returns a list of JSON responses. See
    requests.delete for more details.

    If num_workers > 1, will use a threadpool with this many workers to send the requests. the
    order of results will be preserved, but not the order in which requests are sent. note that
    this is superior to using delete.map due to cleaner logs."""
    # pylint:disable=too-many-arguments

    return _send_modify_requests(
        requests.delete,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers)

def _get(endpoint, connection_info, params, next_page_getter, to_dataframe, codes_to_ignore=None,
        log=True):
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-branches

    if not next_page_getter:
        next_page_getter = lambda _: None
    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    if log:
        prefect.context.get('logger').info(f'REST: Sending GET to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        if isinstance(auth, list):
            kwargs['auth'] = tuple(auth)
        else:
            kwargs['auth'] = auth
    if params:
        kwargs['params'] = params
    size = 0
    data = []
    while True:
        response = requests.get(url, **kwargs)
        if codes_to_ignore and response.status_code in codes_to_ignore:
            return None
        response.raise_for_status()
        size += len(response.content)
        try:
            result = response.json()
        except JSONDecodeError:
            result = response.content
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

    if log:
        prefect.context.get('logger').info(f'REST: Received {len(data)} objects')
    util.record_pull('rest', domain, size)
    if to_dataframe:
        return pd.DataFrame(data)
    return data

def _get_mappable(kwargs):
    return _get(**kwargs)

def _send_modify_request(method, endpoint, connection_info, params, data, json, files, log=True):
    # pylint:disable=too-many-arguments
    info = connection_info.copy()
    domain = info.pop('domain')
    url = domain + endpoint
    if log:
        prefect.context.get('logger').info(f'REST: Sending {method.__name__.upper()} to {url} ...')
    auth = info.pop('auth', None)
    kwargs = {'headers': info}
    if auth:
        if isinstance(auth, list):
            kwargs['auth'] = tuple(auth)
        else:
            kwargs['auth'] = auth
    if params:
        kwargs['params'] = params
    if data:
        kwargs['data'] = data
    if json:
        kwargs['json'] = json
    if files:
        kwargs['files'] = files
    response = method(url, **kwargs)
    response.raise_for_status()
    size = len(response.request.body)
    if log:
        prefect.context.get('logger').info(f'REST: Sent {sizeof_fmt(size)} bytes')
    util.record_push('rest', domain, size)
    try:
        return response.json()
    except JSONDecodeError:
        return response.content

def _send_modify_requests(
        method,
        endpoints,
        connection_info,
        params_list,
        data_list,
        json_list,
        files_list,
        num_workers):
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-branches

    method_name = method.__name__.upper()
    prefect.context.get('logger').info(
        f'REST: Sending {len(endpoints)} {method_name} requests to {connection_info["domain"]} ...')
    if params_list is None:
        params_list = [None] * len(endpoints)
    if data_list is None:
        data_list = [None] * len(endpoints)
    if json_list is None:
        json_list = [None] * len(endpoints)
    if files_list is None:
        files_list = [None] * len(endpoints)

    if len(endpoints) != len(params_list):
        raise ValueError(f'Got {len(endpoints)} endpoints but {len(params_list)} params objects')
    if len(endpoints) != len(data_list):
        raise ValueError(f'Got {len(endpoints)} endpoints but {len(data_list)} data objects')
    if len(endpoints) != len(json_list):
        raise ValueError(f'Got {len(endpoints)} endpoints but {len(json_list)} json objects')
    if len(endpoints) != len(files_list):
        raise ValueError(f'Got {len(endpoints)} endpoints but {len(files_list)} files objects')

    # single-threaded
    if num_workers == 1:
        results =  [
            _soft_catch(_send_modify_request)(
                method,
                endpoint,
                connection_info,
                params,
                data,
                json,
                files,
                log=False)
            for endpoint, params, data, json, files
            in zip(endpoints, params_list, data_list, json_list, files_list)
            ]

    # multi-threaded
    else:
        kwargs_list = [{'method':method,
                        'endpoint':endpoint,
                        'connection_info':connection_info,
                        'params':params,
                        'data':data,
                        'json':json,
                        'files':files,
                        'log':False}
                       for endpoint, params, data, json, files
                       in zip(endpoints, params_list, data_list, json_list, files_list)]
        results = []
        count = 0
        with ThreadPool(num_workers) as pool:
            results_iter = pool.imap(_soft_catch(_send_mappable), kwargs_list)
            while True:
                try:
                    results.append(next(results_iter))
                    count += 1
                    if count % 1000 == 0:
                        prefect.context.get('logger').info(
                            f'Received {count} results out of {len(endpoints)} so far...')
                except StopIteration:
                    break

    prefect.context.get('logger').info(f'REST: Received {len(results)} responses')
    failures = [i for i in results if isinstance(i, BaseException)]
    if failures:
        prefect.context.get('logger').error(
            f'REST: Encountered {len(failures)} exceptions while sending requests. Only the first '
            'will be raised.')
        raise failures[0]
    return results

def _send_mappable(kwargs):
    return _send_modify_request(**kwargs)

def _soft_catch(function):
    # Modify the function to return exceptions instead of raising them
    def modified(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as err:
            return err
    return modified
