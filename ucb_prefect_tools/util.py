"""General utility functions to make flows easier to implement with Prefect Cloud"""

import inspect
import re
import argparse
import importlib
from datetime import datetime
import os
import sys
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from contextlib import contextmanager
import asyncio
import uuid

from prefect import task, get_run_logger, tags, context, deployments, settings
from prefect.utilities.filesystem import create_default_ignore_file
from prefect.filesystems import RemoteFileSystem
from prefect.infrastructure import DockerContainer
from prefect.blocks.system import Secret, JSON
from prefect.client.orchestration import get_client
import git
import pytz

# Overrideable settings related to deployments
DOCKER_REGISTRY = "oit-data-services-docker-local.artifactory.colorado.edu"
LOCAL_FLOW_FOLDER = "flows"
FLOW_STORAGE_CONNECTION_BLOCK = "ds-flow-storage"
REPO_PREFIX = "oit-ds-flows-"

# Timezone for `now` function
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
    if "username" in smtp_info:
        mailserver.starttls()
        mailserver.login(smtp_info["username"], smtp_info["password"])

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
        get_run_logger().warning(info)


async def _set_concurrency_limit(tag, limit):
    async with get_client() as client:
        await client.create_concurrency_limit(tag=tag, concurrency_limit=limit)


async def _remove_concurrency_limit(tag):
    async with get_client() as client:
        await client.delete_concurrency_limit_by_tag(tag=tag)


@contextmanager
def limit_concurrency(max_tasks):
    """Context manager for applying a task concurrency limit via Prefect Cloud"""

    tag = str(uuid.uuid4())
    get_run_logger().info("Setting concurrency limit for tag %s to %s", tag, max_tasks)
    asyncio.run(_set_concurrency_limit(tag, max_tasks))
    try:
        with tags(tag):
            yield
    finally:
        for future in context.get_run_context().task_run_futures:
            future.wait()
        asyncio.run(_remove_concurrency_limit(tag))
        get_run_logger().info("Cleared concurrency limit for tag %s", tag)


def run_flow_command_line_interface(flow_filename, flow_function_name, args=None):
    """Provides a command line interface for running and deploying a flow. If args is none, will
    use sys.argv"""
    # pylint:disable=too-many-locals

    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["deploy", "run"])
    parser.add_argument("--image-name", type=str, default="oit-ds-prefect-default")
    parser.add_argument("--image-branch", type=str, default="main")
    parsed_args = parser.parse_args(args)
    if parsed_args.command == "deploy":
        _deploy(
            flow_filename,
            flow_function_name,
            parsed_args.image_name,
            parsed_args.image_branch,
        )
    if parsed_args.command == "run":
        if git.Repo().active_branch.name == "main":
            raise RuntimeError('Command "run" not allowed from main branch')
        deployment_id = _deploy(
            flow_filename,
            flow_function_name,
            parsed_args.image_name,
            parsed_args.image_branch,
        )
        print("Running deployment...")
        flow_run = deployments.run_deployment(deployment_id)
        print(
            f'Flow run "{flow_run.name}" finished with state {flow_run.state_name}: '
            f"{settings.PREFECT_UI_URL.value()}/flow-runs/flow-run/{flow_run.id}"
        )
        asyncio.run(_delete_deployment(deployment_id))
        print("Deleted deployment")


async def _delete_deployment(deployment_id):
    async with get_client() as client:
        await client.delete_deployment(deployment_id)


def _deploy(flow_filename, flow_function_name, image_name, image_branch):
    # pylint: disable=too-many-locals

    # Check file locations
    if create_default_ignore_file(LOCAL_FLOW_FOLDER):
        print("Created default .prefectignore file")
    flow_filename = os.path.basename(flow_filename)
    if not os.path.exists(os.path.join(LOCAL_FLOW_FOLDER, flow_filename)):
        raise FileNotFoundError(
            f"File {flow_filename} not found in the {LOCAL_FLOW_FOLDER} folder"
        )

    # Get repo info then temporarily switch into flows folder to make importing easier
    label, repo_name, branch_name = _get_repo_info()
    with _ChangeDir(LOCAL_FLOW_FOLDER):

        # Import the module and flow
        module_name = os.path.splitext(flow_filename)[0]
        try:
            flow_function = getattr(sys.modules["__main__"], flow_function_name)
        except KeyError:
            module = importlib.import_module(module_name)
            flow_function = getattr(module, flow_function_name)

        # Parse docstring fields and action on them as appropriate
        docstring_fields = parse_docstring_fields(
            flow_function, {"tags": lambda x: all(i.strip() for i in x.split(","))}
        )
        # Additional tags are only included on main flows
        flow_tags = [label]
        additional_tags = [i.strip() for i in docstring_fields["tags"].split(",") if i]
        if label == "main":
            flow_tags.extend(additional_tags)
        elif additional_tags:
            print(f"Additional tags not added to dev deployment: {additional_tags}")

        # Now we can setup infrastructure and deploy the flow
        image_uri = f"{DOCKER_REGISTRY}/{image_name}:{image_branch}"
        flow_function.name = f"{repo_name} | {module_name}"
        deployment = deployments.Deployment.build_from_flow(
            flow=flow_function,
            name=f"{repo_name} | {branch_name} | {module_name}",
            tags=flow_tags,
            work_pool_name=f"{label}-agent",
            work_queue_name=None,
            infrastructure=_get_flow_infrastructure(image_uri),
            storage=_get_flow_storage(),
            # Path must end in slash!
            path=(
                f"main/{repo_name}/"
                if label == "main"
                else f"{label}/{repo_name}/{branch_name}/"
            ),
        )
        deployment_id = deployment.apply(upload=True)
        print(
            f"Deployed {deployment.name}\n\tWork Pool: {deployment.work_pool_name}\n\t"
            f"Docker image: {image_uri}\n\tTags: {flow_tags}\n\tDeployment URL: "
            f"{settings.PREFECT_UI_URL.value()}/deployments/deployment/{deployment_id}"
        )
        return deployment_id


class _ChangeDir:
    """Allows temporary directory change"""

    def __init__(self, path):
        self.path = path
        self.saved_path = None

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.saved_path)


def _get_repo_info():
    repo = git.Repo()
    repo_name = os.path.basename(repo.working_dir)
    repo_short_name = repo_name.removeprefix(REPO_PREFIX)
    branch_name = repo.active_branch.name
    if branch_name == "main":
        # For main, raise error if current commit doesn't match origin commit
        if repo.head.commit != repo.remotes.origin.fetch()[0].commit:
            raise RuntimeError(
                "You are attempting to deploy from `main`, but HEAD is not on the "
                "same commit as remote `origin`. Push or pull changes to continue."
            )
        # For main, also raise error if working tree is dirty (not counting untracked files)
        if repo.is_dirty():
            raise RuntimeError(
                "You are attempting to deploy from `main`, but your working tree is not clean. "
                "Commit or discard changes to continue."
            )
        label = "main"
    else:
        label = "dev"
    return label, repo_short_name, branch_name


def _get_flow_infrastructure(image_uri):
    return DockerContainer(
        image=image_uri,
        image_pull_policy="ALWAYS",
        auto_remove=True,
    )


def _get_flow_storage():
    storage_conn = reveal_secrets(JSON.load(FLOW_STORAGE_CONNECTION_BLOCK).value)
    if storage_conn["system_type"] == "minio":
        endpoint_url = storage_conn["endpoint"]
        if not endpoint_url.startswith("https://"):
            endpoint_url = "https://" + endpoint_url
        return RemoteFileSystem(
            basepath=f's3://{storage_conn["bucket"]}/',
            settings={
                "key": storage_conn["access_key"],
                "secret": storage_conn["secret_key"],
                "client_kwargs": {"endpoint_url": endpoint_url},
            },
        )
    raise ValueError(
        f'Flow storage connection system type {storage_conn["system_type"]} not supported'
    )


def parse_docstring_fields(function, fields):
    """Takes a function and a dictionary of field names mapped to functions that take field values
    and return False if the field values are not valid. Looks at the function docstring and extracts
    any Sphinx-style field labels (like `:tags: crm-ops`) from the docstring. If any key in `fields`
    is not present in the docstring, its value is set to ''. Otherwise, values (like 'crm-ops')
    are passed to the corresponding function given in the `fields` dict to ensure they are valid.
    Finally, we return a dictionary mapping field keys to values from the actual docstring."""

    docstring = inspect.getdoc(function)
    result = {i: "" for i in fields.keys()}
    if docstring:
        for field, validator in fields.items():
            pattern = re.compile(r"^\s*:tags:\s*(.*)", re.MULTILINE)
            matches = pattern.findall(docstring)
            # Check just one value found and that it is a valid value
            if len(matches) == 1:
                value = matches[0].strip()
                if not validator(value):
                    raise ValueError(
                        f"Value '{value}' of field '{field}' in flow docstring is not allowed"
                    )
                result[field] = value
            elif len(matches) > 1:
                raise ValueError(
                    f"Found {len(matches)} entries for '{field}' in flow docstring; "
                    "must have at most 1"
                )
    return result


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
                    "Extracting value for %s from Secret Block", obj[8:]
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
