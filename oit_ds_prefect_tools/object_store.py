"""Tasks and functions for connecting to file or object storage systems.

Each Prefect task takes a connection_info parameter which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported systems:
    - "sftp" for pysftp.Connection
    - "minio" for minio.Minio

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For sftp, the private_key arg should instead contain the key file's contents. You can also
        supply a known_hosts arg indicating a Prefect KV Store key whose value is a list of lines
        from a known_hosts file to use to populate the cnopts argument.
    - For minio, must include an additional "bucket" argument for the bucket name. Also, if
        "secure" is omitted, it defaults to True.
"""

import io
from typing import BinaryIO, Union
import os
import stat

import prefect
from prefect import task
import pandas as pd
import pysftp
from minio import Minio

from . import util

# pylint:disable=not-callable

# General-purpose tasks and functions

@task
def join_path(left: str, right: str) -> str:
    """Task wrapper for os.path.join, useful for getting full paths after calling list_files"""

    return os.path.join(left, right)

def _sizeof_fmt(num):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"

def _switch(connection_info, **kwargs):
    for key, value in kwargs:
        if connection_info['system_type'] == key:
            del connection_info['system_type']
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')

@task
def get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file/object on the identified system."""

    function = _switch(connection_info,
                       sftp=sftp_get,
                       minio=minio_get)
    function(object_name, connection_info)

@task
def put(binary_object: Union[BinaryIO, bytes],
        object_name: str,
        connection_info: dict,
        **kwargs) -> None:
    """Writes a binary file-like object or bytes string with the given name to the identified
    system. Additional kwargs can be specified for metadata, tags, etc. when using object storage.
    """

    if not hasattr(binary_object, 'read'):
        binary_object = io.BytesIO(binary_object)
    function = _switch(connection_info,
                       sftp=sftp_put,
                       minio=minio_put)
    function(binary_object, object_name, connection_info, **kwargs)

@task
def remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified file/object."""

    function = _switch(connection_info,
                       sftp=sftp_remove,
                       minio=minio_remove)
    function(object_name, connection_info)

@task
def list_names(connection_info: dict, prefix_or_dir: str =None) -> list[str]:
    """Returns a list of object or file names in the given folder. Filters by object name prefix or
    directory path depending on the system. Folders are not included; non-recursive."""

    function = _switch(connection_info,
                       sftp=sftp_list,
                       minio=minio_list)
    if prefix_or_dir:
        function(connection_info, prefix_or_dir)
    else:
        function(connection_info)

@task
def store_dataframe(dataframe: pd.DataFrame, object_name: str, connection_info: dict) -> None:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    data = io.BytesIO()
    dataframe.to_parquet(data)
    function = _switch(connection_info,
                       sftp=sftp_put,
                       minio=minio_put)
    function(data, object_name, connection_info)

@task
def retrieve_dataframe(object_name: str, connection_info: dict) -> pd.DataFrame:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    function = _switch(connection_info,
                       sftp=sftp_get,
                       minio=minio_get)
    contents = function(object_name, connection_info)
    data = io.BytesIO(contents)
    return pd.read_parquet(data)


# SFTP functions

def _make_ssh_key(connection_info):
    if 'private_key' in connection_info:
        filename = f"{connection_info['username']}_at_{connection_info['host']}_key"
        with open(filename, 'w', encoding='ascii') as fileobj:
            fileobj.write(connection_info['private_key'])
        connection_info['private_key'] = filename

def _make_known_hosts(connection_info):
    if 'known_hosts' in connection_info:
        known_hosts = util.get_config_value(connection_info['known_hosts'])
        with open("flow_known_hosts", 'w', encoding="ascii") as fileobj:
            fileobj.writelines(known_hosts)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys.load('flow_known_hosts')
        connection_info['cnopts'] = cnopts
        del connection_info['known_hosts']

def sftp_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        out = io.BytesIO()
        sftp.getfo(file_path, out)
    out = out.getvalue()
    prefect.context.get('logger').info(
        f"SFTP: Got file {file_path} ({_sizeof_fmt(len(out))}) from {connection_info['host']}")
    return out.getvalue()

def sftp_put(file_object: BinaryIO, file_path: str, connection_info: dict, **kwargs) -> None:
    """Writes a file-like object or bytes string to the given path on an SFTP server. """

    if kwargs:
        prefect.context.get('logger').warning(
            f'Additional kwargs not supported by SFTP: {kwargs.keys()}')
    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        if not sftp.isdir(os.path.dirname(file_path)):
            sftp.makedirs(os.path.dirname(file_path))
            sftp.putfo(file_object, file_path)
    prefect.context.get('logger').info(
        f"SFTP: Put file {file_path} ({_sizeof_fmt(file_object.tell())}) onto "
        f"{connection_info['host']}")

def sftp_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        sftp.remove(file_path)
    prefect.context.get('logger').info(
        f"SFTP: Removed file {file_path} from {connection_info['host']}")

def sftp_list(connection_info: dict, folder_path="/") -> list[str]:
    """Returns a list of filenames for files in the given folder. Folders are not included."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        out = [i for i in sftp.listdir_attr(folder_path) if stat.S_ISREG(i.st_mode)]
    prefect.context.get('logger').info(
        f"SFTP: Found {len(out)} files at {folder_path} on {connection_info['host']}")


# Minio functions

def minio_get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given object in a Minio bucket."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    minio = Minio(**connection_info)
    try:
        response = minio.get_object(connection_info['bucket'], object_name)
        out = response.read()
        prefect.context.get('logger').info(
            f'Minio: Got object {object_name} ({_sizeof_fmt(len(out))}) from '
            f'bucket {connection_info["bucket"]} on {connection_info["endpoint"]}')
        return out
    finally:
        response.close()
        response.release_conn()

def minio_put(binary_object: BinaryIO,
              object_name: str,
              connection_info: dict,
              **kwargs) -> None:
    """Puts the given BinaryIO object into a Minio bucket. Any additional keyword arguments are
    passed to the Minio.put_object function."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    minio = Minio(**connection_info)
    length = binary_object.getbuffer().nbytes
    minio.put_object(bucket_name=connection_info['bucket'],
                     object_name=object_name,
                     data=binary_object,
                     length=length,
                     **kwargs)
    prefect.context.get('logger').info(
        f'Minio: Put object {object_name} ({_sizeof_fmt(length)}) into '
        f'bucket {connection_info["bucket"]} on {connection_info["endpoint"]}')

def minio_remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified object from a Minio bucket."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    minio = Minio(**connection_info)
    minio.remove(connection_info['bucket'], object_name)
    prefect.context.get('logger').info(
        f'Minio: Removed object {object_name} from '
        f'bucket {connection_info["bucket"]} on {connection_info["endpoint"]}')

def minio_list(connection_info: dict, prefix: str ="") -> list[str]:
    """Returns a list of object names with the given prefix in a Minio bucket; non-recursive."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    minio = Minio(**connection_info)
    out = [i.object_name for i in minio.list_objects(connection_info['bucket'], prefix=prefix)]
    prefect.context.get('logger').info(
        f'Minio: Found {len(out)} files with prefix "{prefix}" in '
        f'bucket {connection_info["bucket"]} on {connection_info["endpoint"]}')
    return out
