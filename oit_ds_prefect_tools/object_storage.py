"""Tasks and functions for connecting to file or object storage systems.

Each Prefect task takes a connection_info parameter which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported systems:
    - "sftp" for paramiko.client.SSHClient.connect
    - "minio" for minio.Minio
    - "s3" for boto3.session.Session

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For sftp, the pkey arg can just contain the key itself as a string (you can also supply a
        pkey_password arg). Will try all supported key types until it finds the one that works.
        You can also supply a known_hosts arg indicating a Prefect KV Store key whose value is a
        list of lines from a known_hosts file to use in invoking the SSHClient.load_host_keys
        function.
    - For minio, must include an additional "bucket" argument for the bucket name. Also, if
        "secure" is omitted, it defaults to True.
"""

import io
from typing import BinaryIO, Union
import os
import stat
import logging
from contextlib import contextmanager

import prefect
from prefect import task
from prefect.engine import signals
import pandas as pd
from paramiko.client import SSHClient
from paramiko.dsskey import DSSKey
from paramiko.rsakey import RSAKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from paramiko.ssh_exception import SSHException, AuthenticationException
from minio import Minio
from minio.error import S3Error
import boto3
import botocore

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
    for key, value in kwargs.items():
        if connection_info['system_type'] == key:
            del connection_info['system_type']
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')

@task(name="object_storage.get")
def get(object_name: str, connection_info: dict, skip_if_missing: bool =False) -> bytes:
    """Returns the bytes content for the given file/object on the identified system. If
    skip_if_missing is True, this task will skip instead of fail if the object is missing."""

    info = connection_info.copy()
    function = _switch(info,
                       sftp=sftp_get,
                       minio=minio_get,
                       s3=s3_get)
    return function(object_name, info, skip_if_missing)

@task(name="object_storage.put")
def put(binary_object: Union[BinaryIO, bytes],
        object_name: str,
        connection_info: dict,
        **kwargs) -> None:
    """Writes a binary file-like object or bytes string with the given name to the identified
    system. Additional kwargs can be specified for metadata, tags, etc. when using object storage.
    """

    info = connection_info.copy()
    if not hasattr(binary_object, 'read'):
        binary_object = io.BytesIO(binary_object)
    binary_object.seek(0)
    function = _switch(info,
                       sftp=sftp_put,
                       minio=minio_put,
                       s3=s3_put)
    function(binary_object, object_name, info, **kwargs)

@task(name="object_storage.remove")
def remove(object_name: str, connection_info: dict, **kwargs) -> None:
    """Removes the identified file/object. Additional kwargs can be specified to, for example,
    remove a particular version on certain systems."""

    info = connection_info.copy()
    function = _switch(info,
                       sftp=sftp_remove,
                       minio=minio_remove,
                       s3=s3_remove)
    function(object_name, info, **kwargs)

@task(name="object_storage.list_names")
def list_names(connection_info: dict, prefix: str =None) -> list[str]:
    """Returns a list of object or file names in the given folder. Filters by object name prefix,
    which includes directory path for file systems. Folders are not included; non-recursive."""

    info = connection_info.copy()
    function = _switch(info,
                       sftp=sftp_list,
                       minio=minio_list,
                       s3=s3_list)
    if prefix:
        return function(info, prefix)
    return function(info)

@task(name="object_storage.store_dataframe")
def store_dataframe(dataframe: pd.DataFrame, object_name: str, connection_info: dict) -> None:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    info = connection_info.copy()
    data = io.BytesIO(dataframe.to_parquet())
    function = _switch(info,
                       sftp=sftp_put,
                       minio=minio_put,
                       s3=s3_put)
    prefect.context.get('logger').info(
        f'Storing dataframe {object_name} with {len(dataframe.index)} rows in Parquet format')
    function(data, f'{object_name}.parquet', info)

@task(name="object_storage.retrieve_dataframe")
def retrieve_dataframe(object_name: str, connection_info: dict) -> pd.DataFrame:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    info = connection_info.copy()
    function = _switch(info,
                       sftp=sftp_get,
                       minio=minio_get,
                       s3=s3_get)
    contents = function(f'{object_name}.parquet', info)
    data = io.BytesIO(contents)
    out = pd.read_parquet(data)
    prefect.context.get('logger').info(
        f'Retrieved dataframe {object_name} with {len(out.index)} rows')
    return out


# SFTP functions

def _make_ssh_key(connection_info):
    if 'pkey' in connection_info:
        if 'pkey_password' in connection_info:
            password = connection_info['pkey_password']
            del connection_info['pkey_password']
        else:
            password = None
        for key_type in [Ed25519Key, ECDSAKey, RSAKey, DSSKey]:
            try:
                pkey = key_type.from_private_key(
                    io.StringIO(connection_info['pkey']),
                    password)
                prefect.context.get('logger').info(
                    f'SFTP: Loaded SSH private key using class {key_type}')
                connection_info['pkey'] = pkey
                return
            except SSHException:
                pass
        raise ValueError('connection_info["pkey"] could not be loaded using any Paramiko private '
                         'key class: Verify this value gives the contents of a valid private key '
                         'file and the password in connection_info["pkey_password"] (if applicable)'
                         ' is correct')

def _load_known_hosts(ssh_client, connection_info):
    if 'known_hosts' in connection_info:
        known_hosts = util.get_config_value(connection_info['known_hosts'])
        with open("flow_known_hosts", 'w', encoding="ascii") as fileobj:
            fileobj.write('\n'.join(known_hosts))
        ssh_client.load_host_keys('flow_known_hosts')
        del connection_info['known_hosts']
        if 'look_for_keys' not in connection_info:
            connection_info['look_for_keys'] = False
    else:
        ssh_client.load_system_host_keys()

def _sftp_chdir(sftp, remote_directory):
    if remote_directory == '/':
        # absolute path so change directory to root
        sftp.chdir('/')
        return
    if remote_directory == '':
        # top-level relative directory must exist
        return
    try:
        sftp.chdir(remote_directory) # sub-directory exists
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _sftp_chdir(sftp, dirname) # make parent directories
        sftp.mkdir(basename) # sub-directory missing, so created it
        sftp.chdir(basename)

@contextmanager
def _sftp_connection(ssh_client, connection_info):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    try:
        logging.getLogger("paramiko").addHandler(handler)
        ssh_client.connect(**connection_info)
        yield ssh_client.open_sftp()
    except AuthenticationException:
        prefect.context.get('logger').error(
            "Paramiko SSH Authentication failed. You may need to specify 'disabled_algorithms'. "
            f'See logs:\n\n{stream.getvalue()}')
    finally:
        logging.getLogger("paramiko").removeHandler(handler)
        if ssh_client:
            ssh_client.close()

def sftp_get(file_path: str, connection_info: dict, skip_if_missing: bool =False) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server."""

    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        out = io.BytesIO()
        try:
            sftp.getfo(file_path, out)
        except IOError as exc:
            if skip_if_missing:
                prefect.context.get('logger').info(
                    f'Exception "{exc}" caught while getting {file_path} from '
                    f'{connection_info["hostname"]}: skipping task instead of raising')
                # pylint: disable=raise-missing-from
                raise signals.SKIP()
            raise
        out = out.getvalue()
    prefect.context.get('logger').info(
        f"SFTP: Got file {file_path} ({_sizeof_fmt(len(out))}) from {connection_info['hostname']}")
    util.record_pull('sftp', connection_info['hostname'], len(out))
    return out

def sftp_put(file_object: BinaryIO, file_path: str, connection_info: dict, **kwargs) -> None:
    """Writes a file-like object or bytes string to the given path on an SFTP server. """

    if kwargs:
        prefect.context.get('logger').warning(
            f'Additional kwargs not supported by SFTP: {kwargs.keys()}')
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        _sftp_chdir(sftp, os.path.dirname(file_path))
        sftp.putfo(file_object, os.path.basename(file_path))
    size = file_object.seek(0, 2)
    prefect.context.get('logger').info(
        f"SFTP: Put file {file_path} ({_sizeof_fmt(size)}) onto "
        f"{connection_info['hostname']}")
    util.record_push('sftp', connection_info['hostname'], size)

def sftp_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file."""

    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        sftp.remove(file_path)
    prefect.context.get('logger').info(
        f"SFTP: Removed file {file_path} from {connection_info['hostname']}")

def sftp_list(connection_info: dict, file_prefix: str=".") -> list[str]:
    """Returns a list of filenames for files in the given folder. Folders are not included."""

    directory = os.path.dirname(file_prefix)
    prefix = os.path.basename(file_prefix)
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        out = [i.filename for i in sftp.listdir_attr(directory)
               if stat.S_ISREG(i.st_mode) and i.filename.startswith(prefix)]
    prefect.context.get('logger').info(
        f"SFTP: Found {len(out)} files at '{directory}' with prefix '{prefix}' "
        f"on {connection_info['hostname']}")
    return out


# Minio functions

def minio_get(object_name: str, connection_info: dict, skip_if_missing: bool =False) -> bytes:
    """Returns the bytes content for the given object in a Minio bucket."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    bucket = connection_info['bucket']
    del connection_info['bucket']
    minio = Minio(**connection_info)
    try:
        response = minio.get_object(bucket, object_name)
        out = response.data
    except S3Error as err:
        if err.code == 'NoSuchKey' and skip_if_missing:
            prefect.context.get('logger').info(
                f'Exception "{err}" caught while getting {object_name} from bucket '
                f'{bucket} on {connection_info["endpoint"]}: skipping task '
                f'instead of raising')
            # pylint: disable=raise-missing-from
            raise signals.SKIP()
        raise
    finally:
        try:
            response.close()
            response.release_conn()
        except NameError:
            # response never got defined
            pass
    prefect.context.get('logger').info(
        f'Minio: Got object {object_name} ({_sizeof_fmt(len(out))}) from '
        f'bucket {bucket} on {connection_info["endpoint"]}')
    util.record_pull(f'minio: {connection_info["endpoint"]}', bucket, len(out))
    return out

def minio_put(binary_object: BinaryIO,
              object_name: str,
              connection_info: dict,
              **kwargs) -> None:
    """Puts the given BinaryIO object into a Minio bucket. Any additional keyword arguments are
    passed to the Minio.put_object function."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    bucket = connection_info['bucket']
    del connection_info['bucket']
    minio = Minio(**connection_info)
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    minio.put_object(bucket_name=bucket,
                     object_name=object_name,
                     data=binary_object,
                     length=size,
                     **kwargs)
    prefect.context.get('logger').info(
        f'Minio: Put object {object_name} ({_sizeof_fmt(size)}) into '
        f'bucket {bucket} on {connection_info["endpoint"]}')
    util.record_push(f'minio: {connection_info["endpoint"]}', bucket, size)

def minio_remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified object from a Minio bucket."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    bucket = connection_info['bucket']
    del connection_info['bucket']
    minio = Minio(**connection_info)
    minio.remove_object(bucket, object_name)
    prefect.context.get('logger').info(
        f'Minio: Removed object {object_name} from '
        f'bucket {bucket} on {connection_info["endpoint"]}')

def minio_list(connection_info: dict, prefix: str ="") -> list[str]:
    """Returns a list of object names with the given prefix in a Minio bucket; non-recursive."""

    if "secure" not in connection_info:
        connection_info['secure'] = True
    bucket = connection_info['bucket']
    del connection_info['bucket']
    minio = Minio(**connection_info)
    out = [i.object_name for i in minio.list_objects(bucket, prefix=prefix)]
    prefect.context.get('logger').info(
        f'Minio: Found {len(out)} files with prefix "{prefix}" in '
        f'bucket {bucket} on {connection_info["endpoint"]}')
    return out


# S3 functions

def s3_get(object_key: str, connection_info: dict, skip_if_missing: bool =False) -> bytes:
    """Returns the bytes content for the given object in an Amazon S3 bucket."""

    bucket = connection_info['bucket']
    del connection_info['bucket']
    session = boto3.session.Session(**connection_info)
    s3res = session.resource('s3')
    obj = s3res.Object(bucket, object_key)
    data = io.BytesIO()
    try:
        obj.download_fileobj(data)
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404' and skip_if_missing:
            prefect.context.get('logger').info(
                f'Exception "{err}" caught while getting {object_key} from bucket '
                f'{bucket} on Amazon S3: skipping task instead of raising')
            # pylint: disable=raise-missing-from
            raise signals.SKIP()
        raise
    out = data.getvalue()
    prefect.context.get('logger').info(
        f'Amazon S3: Got object {object_key} ({_sizeof_fmt(len(out))}) from '
        f'bucket {bucket}')
    util.record_pull('s3', bucket, len(out))
    return out

def s3_put(binary_object: BinaryIO,
           object_key: str,
           connection_info: dict,
           ExtraArgs: dict =None) -> None:
    """Puts the given BinaryIO object into an Amazon S3 bucket. The optional ExtraArgs parameter
    is passed to upload_fileobj if provided."""

    # pylint:disable=invalid-name
    bucket = connection_info['bucket']
    del connection_info['bucket']
    session = boto3.session.Session(**connection_info)
    s3res = session.resource('s3')
    bucket_res = s3res.Bucket(bucket)
    try:
        bucket_res.load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404':
            bucket_res.create()
        else:
            raise
    bucket_res.upload_fileobj(binary_object, key=object_key, ExtraArgs=ExtraArgs)
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    prefect.context.get('logger').info(
        f'Amazon S3: Put object {object_key} ({_sizeof_fmt(size)})'
        f' into bucket {bucket}')
    util.record_push('s3', bucket, size)

def s3_remove(object_key: str, connection_info: dict, VersionId: str =None) -> None:
    """Removes the identified object from an Amazon S3 bucket. The optional VersionId parameter
    is passed to the delete method if provided (otherwise, the null version is deleted."""

    # pylint:disable=invalid-name
    bucket = connection_info['bucket']
    del connection_info['bucket']
    session = boto3.session.Session(**connection_info)
    s3res = session.resource('s3')
    obj = s3res.Object(bucket, object_key)
    obj.delete(VersionId=VersionId)
    prefect.context.get('logger').info(
        f'Amazon S3: Removed object {object_key} from bucket {bucket}')

def s3_list(connection_info: dict, Prefix: str ="") -> list[str]:
    """Returns a list of object names with the given prefix in an Amazon S3 bucket; non-recursive.
    """

    # pylint:disable=invalid-name
    bucket = connection_info['bucket']
    del connection_info['bucket']
    session = boto3.session.Session(**connection_info)
    s3res = session.resource('s3')
    bucket = s3res.Bucket(bucket)
    out = [i.key for i in bucket.objects.filter(Prefix=Prefix)]
    prefect.context.get('logger').info(
        f'Amazon S3: Found {len(out)} files with prefix "{Prefix}" in '
        f'bucket {bucket}')
    return out
