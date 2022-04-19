"""Tasks for connecting to SFTP servers"""

import io
from typing import BinaryIO
import os

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

@task()
def sftp_put(file_object: BinaryIO, file_path: str, connection_info: dict) -> None:
    """Writes a file-like object to the given path on an SFTP server. The KVs of connection_info
    should match the keyword arguments passed to pysftp.Connection constructor. The private_key
    arg should instead contain the key file's contents. You can also supply a known_hosts arg
    with the contents of a known hosts file to use to fill the cnopts argument."""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        if not sftp.isdir(os.path.dirname(file_path)):
            sftp.mkdir(os.path.dirname(file_path))
            sftp.putfo(file_object, file_path)
    prefect.context.get('logger').info(f"sftp_put {file_path} on {connection_info['host']}")

@task()
def sftp_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server. The KVs of
    connection_info should match the keyword arguments passed to pysftp.Connection constructor"""

    _make_ssh_key(connection_info)
    _make_known_hosts(connection_info)
    with pysftp.Connection(**connection_info) as sftp:
        out = io.BytesIO()
        sftp.getfo(file_path, out)
    prefect.context.get('logger').info(f"sftp_get {file_path} on {connection_info['host']}")
    return out.getvalue()
