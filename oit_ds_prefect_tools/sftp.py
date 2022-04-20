"""Tasks for connecting to SFTP servers using pysftp. Each task takes a connection_info parameter,
a dict whose KVs should match the keyword arguments passed to pysftp.Connection constructor.
The private_key arg should instead contain the key file's contents. You can also supply a
known_hosts arg with the contents of a known hosts file to use to fill the cnopts argument."""

import io
from typing import BinaryIO, Union
import os
import stat

import prefect
from prefect import task
import pysftp

# Utility functions for handling SSH
def _make_ssh_key(connection_info):
    if 'private_key' in connection_info:
        filename = f"{connection_info['username']}_at_{connection_info['host']}_key"
        with open(filename, 'w', encoding='ascii') as fileobj:
            fileobj.write(connection_info['private_key'])
        connection_info['private_key'] = filename

def _make_known_hosts(connection_info):
    if 'known_hosts' in connection_info:
        with open("flow_known_hosts", 'w', encoding="ascii") as fileobj:
            fileobj.write(connection_info['known_hosts'])
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys.load('flow_known_hosts')
        connection_info['cnopts'] = cnopts
        del connection_info['known_hosts']

@task
def put(file_object: Union[BinaryIO, bytes], file_path: str, connection_info: dict) -> None:
    """Writes a file-like object or bytes string to the given path on an SFTP server. """

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    if not hasattr(file_object, 'read'):
        file_object = io.BytesIO(file_object)
    with pysftp.Connection(**connection_info) as sftp:
        if not sftp.isdir(os.path.dirname(file_path)):
            sftp.makedirs(os.path.dirname(file_path))
            sftp.putfo(file_object, file_path)
    prefect.context.get('logger').info(f"sft.put {file_path} on {connection_info['host']}")

@task
def get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        out = io.BytesIO()
        sftp.getfo(file_path, out)
    prefect.context.get('logger').info(f"sftp.get {file_path} on {connection_info['host']}")
    return out.getvalue()

@task
def remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        sftp.remove(file_path)

@task
def list_files(folder_path: str, connection_info: dict) -> list:
    """Returns a list of filenames for files in the given folder. Folders are not included."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        return [i for i in sftp.listdir_attr(folder_path) if stat.S_ISREG(i.st_mode)]

@task
def join_path(left: str, right: str) -> str:
    """Task wrapper for os.path.join, useful for getting full paths after calling list_files"""

    return os.path.join(left, right)
