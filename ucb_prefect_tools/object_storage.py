"""Tasks and functions for connecting to file or object storage systems.

Each Prefect task takes a connection_info parameter which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported systems:
    - "sftp" for paramiko.client.SSHClient.connect
    - "minio" for minio.Minio
    - "s3" for boto3.session.Session
    - "smb" for smb.SMBConnection
    - "onedrive" for msal.ConfidentialClientApplication

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For sftp, the pkey arg can just contain the key itself as a string (you can also supply a
        pkey_password arg). Will try all supported key types until it finds the one that works.
        You can also supply a known_hosts arg indicating a Prefect KV Store key whose value is a
        list of lines from a known_hosts file to use in invoking the SSHClient.load_host_keys
        function.
    - For minio, must include an additional "bucket" argument for the bucket name. Also, if
        "secure" is omitted, it defaults to True.
    - For smb, must include a "port" arg. The service name should be specified by the first element
        in the file path, preceded by a "/". IP address and "my_name" are automatically derived.
    - for onedrive, must include "username" and "password" args for authentication, and also must
        contain an "owner" param which specifies the user who owns the OneDrive space being accessed
"""

import io
import uuid
from typing import BinaryIO, Union
import os
import stat
import logging
from contextlib import contextmanager
import socket

from prefect import task, get_run_logger
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
from smb.SMBConnection import SMBConnection
from smb.base import OperationFailure
import msal
import requests

from .util import sizeof_fmt

# pylint:disable=not-callable

# General-purpose tasks and functions


def _switch(info, **kwargs):
    system_type = info.pop("system_type")
    if system_type not in kwargs:
        raise ValueError(f"Unsupported system type: {system_type}")
    return kwargs[system_type]


@task(name="object_storage.get")
def get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file/object on the identified system. Raises
    FileNotFoundError if the file could not be found."""

    info = connection_info.copy()
    function = _switch(
        info,
        sftp=sftp_get,
        minio=minio_get,
        s3=s3_get,
        smb=smb_get,
        onedrive=onedrive_get,
    )
    return function(object_name, info)


@task(name="object_storage.put")
def put(
    binary_object: Union[BinaryIO, bytes], object_name: str, connection_info: dict
) -> None:
    """Writes a binary file-like object or bytes string with the given name to the identified
    system."""

    info = connection_info.copy()
    if not hasattr(binary_object, "read"):
        binary_object = io.BytesIO(binary_object)
    binary_object.seek(0)
    function = _switch(
        info,
        sftp=sftp_put,
        minio=minio_put,
        s3=s3_put,
        smb=smb_put,
        onedrive=onedrive_put,
    )
    function(binary_object, object_name, info)


@task(name="object_storage.remove")
def remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified file/object."""

    info = connection_info.copy()
    function = _switch(
        info,
        sftp=sftp_remove,
        minio=minio_remove,
        s3=s3_remove,
        smb=smb_remove,
        onedrive=onedrive_remove,
    )
    function(object_name, info)


@task(name="object_storage.list_names")
def list_names(
    connection_info: dict, prefix: str = None, attributes: bool = False
) -> list[str | dict]:
    """Returns a list of object or file names in the given folder. Filters by object name prefix,
    which includes directory path for file systems. Folders are not included; non-recursive.

    attributes: bool - if given, will instead return a list of dictionaries containing the
    attributes for each file found.
    """

    info = connection_info.copy()
    function = _switch(
        info,
        sftp=sftp_list,
        minio=minio_list,
        s3=s3_list,
        smb=smb_list,
        onedrive=onedrive_list,
    )
    if prefix:
        return function(info, prefix, attributes=attributes)
    return function(info, attributes=attributes)


@task(name="object_storage.store_dataframe")
def store_dataframe(
    dataframe: pd.DataFrame, object_name: str, connection_info: dict
) -> None:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    info = connection_info.copy()
    data = io.BytesIO()
    dataframe.to_pickle(data)
    data.seek(0)
    function = _switch(
        info,
        sftp=sftp_put,
        minio=minio_put,
        s3=s3_put,
        smb=smb_put,
        onedrive=onedrive_put,
    )
    get_run_logger().info(
        "Storing dataframe %s with %s rows in pickle format",
        object_name,
        len(dataframe.index),
    )
    function(data, object_name, info)


@task(name="object_storage.retrieve_dataframe")
def retrieve_dataframe(object_name: str, connection_info: dict) -> pd.DataFrame:
    """Writes the given dataframe to the identified storage system. The storage method and format
    should be considered opaque; reading the data should only be done with retrieve_dataframe.
    """

    info = connection_info.copy()
    function = _switch(
        info,
        sftp=sftp_get,
        minio=minio_get,
        s3=s3_get,
        smb=smb_get,
        onedrive=onedrive_get,
    )
    contents = function(object_name, info)
    data = io.BytesIO(contents)
    out = pd.read_pickle(data)
    get_run_logger().info(
        "Retrieved dataframe %s with %s rows", object_name, len(out.index)
    )
    return out


# SFTP functions


def _make_ssh_key(connection_info):
    if "pkey" in connection_info:
        if "pkey_password" in connection_info:
            password = connection_info["pkey_password"]
            del connection_info["pkey_password"]
        else:
            password = None
        for key_type in [Ed25519Key, ECDSAKey, RSAKey, DSSKey]:
            try:
                pkey = key_type.from_private_key(
                    io.StringIO(connection_info["pkey"]), password
                )
                get_run_logger().info(
                    "SFTP: Loaded SSH private key using class %s", key_type
                )
                connection_info["pkey"] = pkey
                return
            except SSHException:
                pass
        raise ValueError(
            'connection_info["pkey"] could not be loaded using any Paramiko private '
            "key class: Verify this value gives the contents of a valid private key "
            'file and the password in connection_info["pkey_password"] (if applicable)'
            " is correct"
        )


def _load_known_hosts(ssh_client, connection_info):
    if "known_hosts" in connection_info:
        hosts_filename = os.path.expanduser(
            f"~/.ssh/prefect_known_hosts_{uuid.uuid4()}"
        )
        known_hosts = connection_info["known_hosts"]
        try:
            os.mkdir(os.path.expanduser("~/.ssh/"))
        except FileExistsError:
            pass
        with open(hosts_filename, "w", encoding="ascii") as fileobj:
            fileobj.write("\n".join(known_hosts))
        ssh_client.load_host_keys(hosts_filename)
        os.remove(hosts_filename)
        del connection_info["known_hosts"]
        if "look_for_keys" not in connection_info:
            connection_info["look_for_keys"] = False
    else:
        ssh_client.load_system_host_keys()


def _sftp_chdir(sftp, remote_directory):
    if remote_directory == "/":
        # absolute path so change directory to root
        sftp.chdir("/")
        return
    if remote_directory == "":
        # top-level relative directory must exist
        return

    try:
        sftp.chdir(remote_directory)  # sub-directory exists
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip("/"))

        _sftp_chdir(sftp, dirname)  # make parent directories

        try:
            sftp.mkdir(basename)  # sub-directory missing, so created it
        except OSError:
            pass

        sftp.chdir(basename)


@contextmanager
def _sftp_connection(ssh_client, connection_info):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("paramiko")
    level = logger.level
    try:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        ssh_client.connect(**connection_info)
        yield ssh_client.open_sftp()
    except AuthenticationException:
        get_run_logger().error(
            "Paramiko SSH Authentication failed. You may need to specify 'disabled_algorithms'. "
            "See logs:\n\n%s",
            stream.getvalue(),
        )
        raise
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        if ssh_client:
            ssh_client.close()


def sftp_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server."""

    get_run_logger().info(
        "SFTP: Getting file %s from %s", file_path, connection_info["hostname"]
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        out = io.BytesIO()
        try:
            sftp.getfo(file_path, out)
        except IOError as err:
            raise FileNotFoundError(
                f'File {file_path} not found on {connection_info["hostname"]}'
            ) from err
        out = out.getvalue()
    get_run_logger().info("SFTP: Got %s file", sizeof_fmt(len(out)))
    return out


def sftp_put(file_object: BinaryIO, file_path: str, connection_info: dict) -> None:
    """Writes a file-like object or bytes string to the given path on an SFTP server."""

    size = file_object.seek(0, 2)
    file_object.seek(0)
    get_run_logger().info(
        "SFTP: Putting file %s (%s) onto %s",
        file_path,
        sizeof_fmt(size),
        connection_info["hostname"],
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        _sftp_chdir(sftp, os.path.dirname(file_path))
        # Confirming can throw an error on CUTransfer where files are moved immediately
        sftp.putfo(file_object, os.path.basename(file_path), confirm=False)


def sftp_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file."""

    get_run_logger().info(
        "SFTP: Removing file %s from %s", file_path, connection_info["hostname"]
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        sftp.remove(file_path)


def sftp_list(
    connection_info: dict, file_prefix: str = "./", attributes: bool = False
) -> list[str | dict]:
    """Returns a list of filenames for files with the given path prefix. Only the filenames are
    returned, without folder paths."""

    directory = os.path.dirname(file_prefix)
    prefix = os.path.basename(file_prefix)
    get_run_logger().info(
        "SFTP: Finding files at '%s' with prefix '%s' on %s",
        directory,
        prefix,
        connection_info["hostname"],
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        if attributes:
            sftp_attributes = [
                "st_size",
                "st_uid",
                "st_gid",
                "st_mode",
                "st_atime",
                "st_mtime",
            ]
            out = [
                dict(
                    name=i.filename,
                    **{attr: getattr(i, attr) for attr in sftp_attributes},
                )
                for i in sftp.listdir_attr(directory)
                if stat.S_ISREG(i.st_mode) and i.filename.startswith(prefix)
            ]
        else:
            out = [
                i.filename
                for i in sftp.listdir_attr(directory)
                if stat.S_ISREG(i.st_mode) and i.filename.startswith(prefix)
            ]
    get_run_logger().info("SFTP: Found %s files", len(out))
    return out


# Minio functions


def minio_get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given object in a Minio bucket."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        "Minio: Getting object %s from bucket %s on %s",
        object_name,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    try:
        response = minio.get_object(bucket, object_name)
        out = response.data
    except S3Error as err:
        if err.code == "NoSuchKey":
            raise FileNotFoundError(
                f"Object {object_name} not found on {bucket}"
            ) from err
        raise
    finally:
        try:
            response.close()
            response.release_conn()
        except NameError:
            # response never got defined
            pass
    get_run_logger().info("Minio: Got %s object", sizeof_fmt(len(out)))
    return out


def minio_put(binary_object: BinaryIO, object_name: str, connection_info: dict) -> None:
    """Puts the given BinaryIO object into a Minio bucket. Any additional keyword arguments are
    passed to the Minio.put_object function."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    get_run_logger().info(
        "Minio: Putting object %s (%s) into bucket %s on %s",
        object_name,
        sizeof_fmt(size),
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    minio.put_object(
        bucket_name=bucket, object_name=object_name, data=binary_object, length=size
    )


def minio_remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified object from a Minio bucket."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        "Minio: Removing object %s from bucket %s on %s",
        object_name,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    minio.remove_object(bucket, object_name)


def minio_list(
    connection_info: dict, prefix: str = "", attributes: bool = False
) -> list[str | dict]:
    """Returns a list of object names with the given prefix in a Minio bucket; non-recursive."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        'Minio: Finding files with prefix "%s" in bucket %s on %s',
        prefix,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    if attributes:
        out = [
            dict(name=os.path.basename(i.object_name), **dict(i))
            for i in minio.list_objects(bucket, prefix=prefix)
        ]
    else:
        out = [
            os.path.basename(i.object_name)
            for i in minio.list_objects(bucket, prefix=prefix)
        ]
    get_run_logger().info("Minio: Found %s files", len(out))
    return out


# S3 functions


def s3_get(object_key: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given object in an Amazon S3 bucket."""

    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        "Amazon S3: Getting object %s from bucket %s", object_key, bucket
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    obj = s3res.Object(bucket, object_key)
    data = io.BytesIO()
    try:
        obj.download_fileobj(data)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            raise FileNotFoundError(
                f"Object {object_key} not found in {bucket}"
            ) from err
        raise
    out = data.getvalue()
    get_run_logger().info("Amazon S3: Got %s object", sizeof_fmt(len(out)))
    return out


def s3_put(
    binary_object: BinaryIO,
    object_key: str,
    connection_info: dict,
    ExtraArgs: dict = None,
) -> None:
    """Puts the given BinaryIO object into an Amazon S3 bucket. The optional ExtraArgs parameter
    is passed to upload_fileobj if provided."""

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    get_run_logger().info(
        "Amazon S3: Putting object %s (%s) into bucket %s",
        object_key,
        sizeof_fmt(size),
        bucket,
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    bucket_res = s3res.Bucket(bucket)
    try:
        bucket_res.load()
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            bucket_res.create()
        else:
            raise
    bucket_res.upload_fileobj(binary_object, object_key, ExtraArgs=ExtraArgs)


def s3_remove(object_key: str, connection_info: dict, VersionId: str = None) -> None:
    """Removes the identified object from an Amazon S3 bucket. The optional VersionId parameter
    is passed to the delete method if provided (otherwise, the null version is deleted.
    """

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        "Amazon S3: Removing object %s from bucket %s", object_key, bucket
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    obj = s3res.Object(bucket, object_key)
    obj.delete(VersionId=VersionId)


def s3_list(
    connection_info: dict, Prefix: str = "", attributes: bool = False
) -> list[str | dict]:
    """Returns a list of object names with the given prefix in an Amazon S3 bucket;
    non-recursive."""

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    get_run_logger().info(
        'Amazon S3: Finding files with prefix "%s" in bucket %s', Prefix, bucket
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    bucket = s3res.Bucket(bucket)
    if attributes:
        out = [
            dict(name=os.path.basename(i.key), **dict(i))
            for i in bucket.objects.filter(Prefix=Prefix)
        ]
    else:
        out = [os.path.basename(i.key) for i in bucket.objects.filter(Prefix=Prefix)]
    get_run_logger().info("Amazon S3: Found %s files", len(out))
    return out


# SMB functions


def smb_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file on an SMB server."""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    get_run_logger().info(
        "SMB: Getting file %s from %s on %s",
        file_path,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        out = io.BytesIO()
        try:
            conn.retrieveFile(service_name, file_path, out)
        except OperationFailure as err:
            raise FileNotFoundError(
                f'File {file_path} not found in {service_name} on {connection_info["remote_name"]}'
            ) from err
    out = out.getvalue()
    get_run_logger().info("SMB: Got %s file", sizeof_fmt(len(out)))
    return out


def smb_put(
    file_object: BinaryIO,
    file_path: str,
    connection_info: dict,
) -> None:
    """Writes a file-like object or bytes string tot he given path on an SMB server."""

    size = file_object.seek(0, 2)
    file_object.seek(0)
    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    get_run_logger().info(
        "SMB: Putting file %s (%s) in %s on %s",
        file_path,
        sizeof_fmt(size),
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        try:
            conn.createDirectory(service_name, os.path.dirname(file_path))
        except OperationFailure:
            # directory already exists
            pass
        conn.storeFile(service_name, file_path, file_object)


def smb_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file"""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    get_run_logger().info(
        "SMB: Removing file %s from %s on %s",
        file_path,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        conn.deleteFiles(service_name, file_path)


def smb_list(
    connection_info: dict, prefix: str = "./", attributes: bool = False
) -> list[str | dict]:
    """Returns a list of filenames for files with the given path prefix. Only the filenames are
    returned, without folder paths."""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not prefix.startswith("/"):
        raise ValueError("Prefix must start with '/' followed by the SMB service name")
    service_name = prefix.split("/")[1]
    prefix = prefix.removeprefix(f"/{service_name}")
    get_run_logger().info(
        "SMB: Finding files at '%s' in %s on %s",
        prefix,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f"Unable to connect to {connection_info['remote_name']} ({server_ip}): "
                "Authentication failed"
            )
        if attributes:
            smb_attributes = [
                "create_time",
                "last_access_time",
                "last_write_time",
                "last_attr_change_time",
                "file_size",
                "alloc_size",
                "file_attributes",
                "short_name",
                "filename",
                "file_id",
            ]
            out = [
                dict(
                    name=i.filename,
                    **{attr: getattr(i, attr) for attr in smb_attributes},
                )
                for i in conn.listPath(
                    service_name,
                    path=os.path.dirname(prefix),
                    pattern=f"{os.path.basename(prefix)}*",
                )
            ]
        else:
            out = [
                i.filename
                for i in conn.listPath(
                    service_name,
                    path=os.path.dirname(prefix),
                    pattern=f"{os.path.basename(prefix)}*",
                )
            ]
    get_run_logger().info("SMB: Found %s files", len(out))
    return out


# OneDrive functions


def _get_onedrive_auth(connection_info):
    owner = connection_info.pop("owner", None)
    username = connection_info.pop("username")
    password = connection_info.pop("password")
    app = msal.ConfidentialClientApplication(**connection_info)
    if username:
        app_token = app.acquire_token_by_username_password(
            username=username,
            password=password,
            scopes=["User.Read", "Files.ReadWrite.All"],
        )
    else:
        raise KeyError(
            'Only username/password authentication is supported; "username" key is required'
        )
    if "error" in app_token:
        raise ConnectionError(f"Authentication error: {app_token['error_description']}")
    return owner, f"Bearer {app_token['access_token']}"


def onedrive_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file in a user's OneDrive."""

    owner, auth = _get_onedrive_auth(connection_info)
    get_run_logger().info(
        "OneDrive: Getting file %s from %s's OneDrive",
        file_path,
        owner,
    )

    request = requests.get(
        f"https://graph.microsoft.com/v1.0/users('{owner}')/drive/root:{file_path}:/content",
        headers={"Authorization": auth},
        timeout=300,
    )
    request.raise_for_status()

    get_run_logger().info("SMB: Got %s file", sizeof_fmt(len(request.content)))
    return request.content


def onedrive_put(
    file_object: BinaryIO,
    file_path: str,
    connection_info: dict,
) -> None:
    """Writes a file-like object or bytes string to the given path on a user's OneDrive."""

    size = file_object.seek(0, 2)
    file_object.seek(0)
    owner, auth = _get_onedrive_auth(connection_info)
    get_run_logger().info(
        "OneDrive: Putting file %s (%s) on %s's Onedrive",
        file_path,
        sizeof_fmt(size),
        owner,
    )

    # If the file is smaller than 4MB, we can do a simple upload
    if size <= 4 * 1024 * 1024:
        url = (
            f"https://graph.microsoft.com/v1.0/users('{owner}')/drive"
            f"/root:{file_path}:/content"
        )
        response = requests.put(
            url,
            headers={"Authorization": auth},
            data=file_object.read(),
            timeout=60,
        )
        response.raise_for_status()
    else:
        # For larger files, use the createUploadSession endpoint
        response = requests.post(
            f"https://graph.microsoft.com/v1.0/users('{owner}')/drive/"
            f"root:{file_path}:/createUploadSession",
            headers={"Authorization": auth},
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            timeout=30,
        )
        response.raise_for_status()
        upload_url = response.json()["uploadUrl"]

        # Fragment the file and upload in parts
        chunk_size = 320 * 1024  # 320KB
        start_byte = 0
        while start_byte < size:
            end_byte = min(start_byte + chunk_size, size) - 1
            file_chunk = file_object.read(chunk_size)
            chunk_headers = {
                "Authorization": auth,
                "Content-Range": f"bytes {start_byte}-{end_byte}/{size}",
            }
            chunk_response = requests.put(
                upload_url,
                headers=chunk_headers,
                data=file_chunk,
                timeout=30,
            )
            chunk_response.raise_for_status()
            start_byte += chunk_size


def onedrive_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file from a user's OneDrive."""

    owner, auth = _get_onedrive_auth(connection_info)
    get_run_logger().info(
        "OneDrive: Removing file %s from %s's OneDrive",
        file_path,
        owner,
    )

    response = requests.delete(
        f"https://graph.microsoft.com/v1.0/users('{owner}')/drive/root:{file_path}",
        headers={"Authorization": auth},
        timeout=60,
    )
    response.raise_for_status()


def onedrive_list(
    connection_info: dict, prefix: str = "", attributes: bool = False
) -> list[str | dict]:
    """Returns a list of filenames for files with the given path prefix.
    Only the filenames are returned, without folder paths."""

    owner, auth = _get_onedrive_auth(connection_info)
    get_run_logger().info(
        "OneDrive: Listing files with prefix %s from %s's OneDrive",
        prefix,
        owner,
    )

    filenames = []
    url = (
        f"https://graph.microsoft.com/v1.0/users('{owner}')/drive"
        f"/root:{prefix}:/children?$top=200"
    )

    while url:
        response = requests.get(
            url,
            headers={"Authorization": auth},
            timeout=60,
        )
        response.raise_for_status()

        # Extract filenames from the response
        if attributes:
            filenames += [
                item
                for item in response.json().get("value", [])
                if not item.get("folder")
            ]
        else:
            filenames += [
                item["name"]
                for item in response.json().get("value", [])
                if not item.get("folder")
            ]

        # Check for the nextLink to see if there are more items/pages
        url = response.json().get("@odata.nextLink")

    get_run_logger().info("OneDrive: Found %s files", len(filenames))
    return filenames
