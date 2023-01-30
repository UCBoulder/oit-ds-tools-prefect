"""
Connecting to OneDrive using Microsoft Authentication Library 


Notes:
------
needs to be tested with onedrive parameters that are stored in 
the Prefect instance

looking to utilize existing rest methods in the ucb_prefect_tools

the connection should have:
    authority:
        the endpoint with the tenant ID provided by the AAD App
    client_id:
        the client id provided by the AAD App
    scope:
        scope of the ms graph API
    client_secret:
        secret generated during the app registration 
    endpoint:
        Microsoft Graph API endpoint for Onedrive

Questions:
----------
1. encryption key on prefect blocks

"""

import msal as onedrivesdk
from os import path
from requests import get, put
from prefect import flow, task
from prefect.blocks.system import JSON
from prefect.blocks.system import Secret
from atexit import register
from onedrivesdk import acquire_token_silent, acquire_token_for_client
from onedrivesdk import ConfidentialClientApplication, SerializableTokenCache

# initialize

tenant_id = None
client_id = None
scope = None
client_secret = None
onedrive_endpoint = None

def onedrive_connect(
        tenant_id: str,
        client_id: str,
        scope: str,
        client_secret: str,
        onedrive_endpoint: str
    ) -> dict:

    # obtain secrets from Prefect
    # please change the parameters that reflect the current msal credentials

    tenant_id = JSON.load("onedrive-tenant-id")
    client_id = JSON.load("onedrive-client-id")
    scope = JSON.load("onedrive-scope")
    client_secret = Secret.load("onedrive-client-secret")
    onedrive_endpoint = JSON.load("onedrive-endpoint")

    # token cache
    # currently unsure how long the token will persist
    # hence we are serializing the token

    cache = onedrivesdk.SerializableTokenCache()
    
    # locate and read 

    if os.path.exists("/SecureFolder/tokencache.bin"):
        cache.deserialize(open("/SecureFolder/tokencache.bin","r").read())

    atexit.register(
        lambda: open("/SecureFolder/tokencache.bin","w").write(cache.serialize())
    )

    # connect/authenticate

    onedrive_app = onedrivesdk.ConfidentialClientApplication(
        client_id,
        authority,
        client_secret,
        token_cache = cache
    )

    # obtain the access token
    # the silent method means the python program may not popup 
    # an external browser window for more user interaction

    accToken = onedrive_app.acquire_token_silent(
        onedrive_endpoint,
        account = None
    )

    # my guess is that this will utilize multi-factor authentication 

    if not accToken:
        accToken = onedrive_app.acquire_token_for_client(onedrive_endpoint)

    return accToken

# payload estimator
# calculate the size of file

def payload_estimate(
        filesize: int
    ) -> int:
    size_class = ['B','KB','MB','GB','TB']
    for i in size_class:
        if filesize < 1024.0:
            return "%d" %(filesize, i)
        filesize = filesize/1024.0

def onedrive_get(
        filepath: str,
        connection_info: dict
    ) -> bytes:

    get_run_logger().info(
        "OneDrive: Getting file %s from %s", filepath, connection_info["hostname"]
    )

    try:
        result = onedrive_app()
        get_data = requests.get(
            onedrive_endpoint,
            headers = {
                'Authorization' : 'Bearer' + result['accToken']
            }
        ).json()
    except:
        raise KeyError
    finally:
        return get_data
    

def onedrive_put(
        filepath: str,
        connection_info: dict
    ) -> bytes:
    """
    function to determine the size of the payload and use necessary function accordingly
    OneDrive 
    """
    # compute the file size 
    _size_of_file = os.path.getsize(filepath)
    filesize = payload_estimate(_size_of_file)

    # accordingly use the necessary function
    try:
        if filesize <= 400000:
            _onedrive_put_min()
        elif filesize > 400000:
            _onedrive_put_max()
        else:
            get_run_logger().error(
                "unable to compute the size of the file"
            )
    except CustomException:
        get_run_logger().error(
            "problem with computing the filesize; program will continue to establish a session for filetransfer"
        )
    finally:
        # regardless of the file size , treat it as a large file and upload 
        _onedrive_put_max() 

def _onedrive_put_min(
        filepath: str,
        connection_info: dict
    ) -> bytes:
    """
    onedrive_put_min method is used for files that are less than 4 megabytes in size
    """

    get_run_logger().info(
        "OneDrive: Getting file %s from %s", filepath, connection_info["hostname"]
    )

    try:
        result = onedrive_app()
        get_data = requests.put(
            onedrive_endpoint,
            headers = {
                'Authorization' : 'Bearer' + result['accToken']
            }
        ).json()
    except:
        raise KeyError
    finally:
        return get_data


def _onedrive_put_max(
        filepath: str,
        connection_info: dict
    ) -> bytes:
    """
    onedrive_put_max method is used for files that are more than 4 megabytes in size
    create an upload session. this enables a staging area for the bytecode of the payload.

    """
    UploadSession = None
    get_run_logger().info(
        "OneDrive: Getting file %s from %s", filepath, connection_info["hostname"]
    )

    try:
        result = onedrive_app()
        get_data = requests.put(
            onedrive_endpoint,
            headers = {
                'Authorization' : 'Bearer' + result['accToken']
            }
        ).json()
    except:
        get_run_logger().error(
            "Unable to upload the large file, %s", filepath
        )
    finally:
        return get_data