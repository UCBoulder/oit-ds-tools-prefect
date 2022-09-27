"""General utility functions to make flows easier to implement with Prefect Cloud"""

import importlib
from datetime import datetime
import os
import sys
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

from prefect import task, get_run_logger, context
from prefect.utilities.filesystem import set_default_ignore_file
from prefect.deployments import Deployment
from prefect.filesystems import RemoteFileSystem
from prefect.blocks.system import Secret, JSON
from prefect.infrastructure.docker import DockerContainer
import git
import pytz

DOCKER_REGISTRY = "oit-data-services-docker-local.artifactory.colorado.edu"
LOCAL_FLOW_FOLDER = "flows"
FLOW_STORAGE_CONNECTION_BLOCK = "ds-flow-storage"
TIMEZONE = "America/Denver"


@task
def send_email(
    addressed_to: str,
    subject: str,
    body: str,
    smtp_info: dict,
    attachments: list = None,
):
    """Sends an email.

    :param addressed_to: A list of emails to send to separated by ", "
    :param subject: Email subject
    :param body: Plain text email body
    :param smtp_info: Dict with keys "from" (sender email), "host", and "port"
    :param attachments: List of (bytes, str) tuples giving the file contents and the filename of
        objects to attach
    """

    msg = MIMEMultipart()
    msg["From"] = smtp_info["from"]
    msg["To"] = addressed_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body))

    if attachments:
        for contents, filename in attachments:
            obj = MIMEBase("application", "octet-stream")
            obj.set_payload(contents)
            email.encoders.encode_base64(obj)
            obj.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(obj)

    mailserver = smtplib.SMTP(smtp_info["host"], smtp_info["port"])

    if addressed_to:
        info = f'Sending "{subject}" email to {addressed_to}'
        if attachments:
            info += " with attachments " + ", ".join(
                [f"{i[1]} ({sizeof_fmt(len(i[0]))})" for i in attachments]
            )
        info += f":\n{body[:500]} ..."
        get_run_logger().info(info)
        mailserver.sendmail(
            smtp_info["from"].split(", "), addressed_to.split(", "), msg.as_string()
        )
        mailserver.quit()
    else:
        info = f'No delivery contacts set; "{subject}" email not sent'
        if attachments:
            info += " with attachments " + ", ".join(
                [f"{i[1]} ({sizeof_fmt(len(i[0]))})" for i in attachments]
            )
        info += f":\n{body[:500]} ..."
        get_run_logger().warn(info)


def run_flow_command_line_interface(flow_filename, flow_function_name, args=None):
    """Provides a command line interface for running and deploying a flow. If args is none, will
    use sys.argv"""
    # pylint:disable=too-many-locals

    if args is None:
        args = sys.argv[1:]
    command = args[0]
    options = args[1:]
    if command == "deploy":
        _deploy(flow_filename, flow_function_name, options)
    else:
        raise ValueError(f"Command {command} is not implemented")


def _deploy(flow_filename, flow_function_name, options):
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-branches
    if options and options[0] == "--docker-label":
        docker_label = options[1]
    else:
        docker_label = "main"

    repo = git.Repo()
    cwd = os.getcwd()
    os.chdir(LOCAL_FLOW_FOLDER)

    try:
        flow_filename = os.path.basename(flow_filename)
        if not os.path.exists(flow_filename):
            raise FileNotFoundError(
                f"File {flow_filename} not found in the {LOCAL_FLOW_FOLDER} folder"
            )
        if set_default_ignore_file("."):
            print("Created default .prefectignore file")

        repo_name = os.path.basename(repo.working_dir)
        module_name = os.path.splitext(flow_filename)[0]
        try:
            flow_function = getattr(sys.modules["__main__"], flow_function_name)
        except KeyError:
            module = importlib.import_module(module_name)
            flow_function = getattr(module, flow_function_name)
        branch_name = repo.active_branch.name
        if branch_name == "main":
            label = "main"
            storage_path = f"main/{repo_name}"
            flow_function = flow_function.with_options(
                retries=1, retry_delay_seconds=5 * 60
            )
        else:
            label = "dev"
            storage_path = f"dev/{repo_name}/{branch_name}"

        docker = DockerContainer(
            image=f"{DOCKER_REGISTRY}/{repo_name}:{docker_label}",
            image_pull_policy="ALWAYS",
            auto_remove=True,
        )

        storage_conn = reveal_secrets(JSON.load(FLOW_STORAGE_CONNECTION_BLOCK).value)
        if storage_conn["system_type"] == "minio":
            endpoint_url = storage_conn["endpoint"]
            if not endpoint_url.startswith("https://"):
                endpoint_url = "https://" + endpoint_url
            storage = RemoteFileSystem(
                basepath=f's3://{storage_conn["bucket"]}/{storage_path}',
                settings={
                    "key": storage_conn["access_key"],
                    "secret": storage_conn["secret_key"],
                    "client_kwargs": {"endpoint_url": endpoint_url},
                },
            )
        else:
            raise ValueError(
                f'Flow storage connection system type {storage_conn["system_type"]} not supported'
            )

        deployment = Deployment.build_from_flow(
            flow=flow_function,
            name=f"{module_name}-{label}-{branch_name}",
            tags=[label],
            work_queue_name=label,
            infrastructure=docker,
            storage=storage,
            apply=True,
        )
        print(f"Deployed {deployment.name}")

    finally:
        try:
            os.chdir(cwd)
        # pylint:disable=broad-except
        except Exception:
            pass


def reveal_secrets(json_obj) -> dict:
    """Looks for strings within a JSON-like object that start with '<secret>' and replaces these
    with Prefect Secrets. For example, the value '<secret>EDB_PW' would be replaced with the EDB_PW
    Prefect Secret value."""

    def recursive_reveal(obj):
        if isinstance(obj, dict):
            return {k: recursive_reveal(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [recursive_reveal(i) for i in obj]
        if isinstance(obj, str) and obj.startswith("<secret>"):
            try:
                get_run_logger().info(
                    f"Extracting value for {obj[8:]} from Secret Block"
                )
            except RuntimeError:
                # Called outside flow run: print instead
                print(f"Extracting value for {obj[8:]} from Secret Block")
            return Secret.load(obj[8:]).get()
        return obj

    return recursive_reveal(json_obj)


def now() -> datetime:
    """Returns the current datetime converted to `util.TIMEZONE`"""

    return datetime.now(pytz.timezone(TIMEZONE))


def sizeof_fmt(num: int) -> str:
    """Takes a number of bytes and returns a human-readable representation"""

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"