"""General utility functions to make flows easier to implement with Prefect Cloud"""

import inspect
import json
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

from prefect import task, get_run_logger, tags, context, deployments, settings, flow
from prefect.utilities.filesystem import create_default_ignore_file
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.runner.storage import GitRepository
from prefect.server.schemas.schedules import CronSchedule
from prefect_dask.task_runners import DaskTaskRunner
from prefect_aws import S3Bucket
import git
import pytz


# Overrideable settings related to deployments
DOCKER_REGISTRY = "oit-data-services-docker-local.artifactory.colorado.edu"
GITHUB_ORG = "UCBoulder"
LOCAL_FLOW_FOLDER = "flows"
REPO_PREFIX = "oit-ds-flows-"
GITHUB_SECRET_BLOCK = "github-pat"
RESULT_STORAGE_BLOCK = "flow-result-storage"
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


def deployable(flow_obj):
    """Decorator that modified a Prefect flow to set some standard settings. This decorator
    should be placed ABOVE the @flow decorator."""

    # Only set options if they weren't set previously
    if flow_obj.timeout_seconds is None:
        flow_obj.timeout_seconds = 8 * 3600
    if flow_obj.result_storage is None:
        # Terminal slash in the path is probably non-optional
        flow_obj.result_storage = S3Bucket.load(RESULT_STORAGE_BLOCK)
    if flow_obj.task_runner is None:
        flow_obj.task_runner = (
            DaskTaskRunner(cluster_kwargs={"n_workers": 1, "threads_per_worker": 10}),
        )
    # Auto-generate the flow name based on the repo name (aka the cwd) and the flow file name
    flow_obj.name = (
        f"{os.path.basename(os.path.dirname(os.getcwd())).removeprefix(REPO_PREFIX)}"
        f" | {os.path.splitext(os.path.basename(inspect.getfile(flow_obj.fn)))[0]}"
    )
    return flow_obj


def run_flow_command_line_interface(
    flow_filename: str, flow_function_name: str, args: list = None
):
    """Provides a command line interface for running and deploying a flow. If args is none, will
    use sys.argv.

    The first command-line arg must be either `deploy` or `run`. Deploy will deploy the flow
    based on a combination of any CL options and the flow's docstring fields. Run will deploy the
    flow, run it via Prefect Cloud, then delete the deployment afterward; as such, it is useful
    for testing.

    Your git repo must be "clean" in order to deploy, meaning you have no uncommitted changes and
    you are up to date with your remote branch. This is because Prefect will pull your code from
    Github when running the flow.

    Command-line options:

        `--image-name`: Change what image to use for deployment from what is specified in the flow
            docstring

        `--image-branch`: Change the image branch, aka image label, to use. By default, when you are
            on the `main` git branch, this will be `main`, and when you are on any other branch,
            this will be the same as the branch name. See the oit-ds-tools-prefect-images repo for
            information about building images.

        `--label`: This determines what work pool will be used for the deployment. Options are
            `main` or `dev`, and the default is based on the current git branch. If you deploy a
            main flow with the dev label, it will act like a dev deployment with no schedule, error
            notifications, etc. If you deploy a dev flow with a main label, it will run just like
            a dev deployment except using the main namespace in Kubernetes. You could also create
            a new work pool/namespace and point the deployment at it using this option.

    When deploying a flow, the following Sphinx-style docstring fields are used to define certain
    parameters (see oit-ds-flows-template for an example):

        main_params: A json-style dict of parameter overrides to use for flow runs when deployed
            from the main branch. At the very least, the env param should be overridden to "prod"
            from it's usual default of "dev".
        schedule: A cron string giving the schedule to be applied to the main-branch deployment.
            This will use the common America/Denver timezone used by ucb_prefect_tools. If no
            schedule is desired, write `None` here. For new deployments, the schedule will be
            active by default. But if someone manually disables a schedule for an existing
            deployment, future redeploys will not reactivate that schedule by design: it must be
            manually reactivated.
        image_name: The image to use for this flow, usually just oit-ds-prefect-default.
        tags: (Optional) Any additional tags to apply to main-branch deployments of this flow. At
            least list the autotest tag for flows which can safely be run with default (i.e. dev)
            parameters as part of automated testing.
        source_systems: An informal list of systems from which this flow pulls data. Make it helpful
            to non-engineers: e.g. instead of "CUTransfer", say "Bookstore (CUTransfer)"
        sink_systems: Same as source systems, but for places where the data is being sent.
        customer_contact: A comma-separated list of email addresses of people or teams to reach out
            to if we have questions about the flow (typically on the "sink" side). Feel free to
            include names or other info when needed for clarity.

    """
    # pylint:disable=too-many-locals

    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["deploy", "run"])
    parser.add_argument("--image-name", type=str, default="infer")
    parser.add_argument("--image-branch", type=str, default="main")
    parser.add_argument("--label", type=str, default="infer")
    parsed_args = parser.parse_args(args)
    if parsed_args.command == "deploy":
        _deploy(
            flow_filename,
            flow_function_name,
            parsed_args.image_name,
            parsed_args.image_branch,
            parsed_args.label,
        )
    if parsed_args.command == "run":
        if git.Repo().active_branch.name == "main" and parsed_args.label == "infer":
            raise RuntimeError(
                "Command `run` not allowed from main branch unless you specify an alternate "
                "`--label`"
            )
        deployment_id = _deploy(
            flow_filename,
            flow_function_name,
            parsed_args.image_name,
            parsed_args.image_branch,
            parsed_args.label,
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


def _deploy(flow_filename, flow_function_name, image_name, image_branch, label="infer"):
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements

    # Check file locations
    if create_default_ignore_file(LOCAL_FLOW_FOLDER):
        print("Created default .prefectignore file")
    flow_filename = os.path.basename(flow_filename)
    if not os.path.exists(os.path.join(LOCAL_FLOW_FOLDER, flow_filename)):
        raise FileNotFoundError(
            f"File {flow_filename} not found in the {LOCAL_FLOW_FOLDER} folder"
        )

    # Get repo info
    inferred_label, repo_name, branch_name = _get_repo_info()
    module_name = os.path.splitext(flow_filename)[0]
    # Set label and deployment name based on label parameter
    if label == "infer":
        label = inferred_label
        deployment_name = f"{repo_name} | {branch_name} | {module_name}"
    else:
        deployment_name = f"{repo_name} | {branch_name} (as {label}) | {module_name}"

    # Temporarily change into the flows folder
    with _ChangeDir(LOCAL_FLOW_FOLDER):

        # Import the module and flow
        try:
            flow_func = getattr(sys.modules["__main__"], flow_function_name)
        except KeyError:
            module = importlib.import_module(module_name)
            flow_func = getattr(module, flow_function_name)

        # Parse docstring fields and action on them as appropriate
        docstring_fields = parse_docstring_fields(inspect.getdoc(flow_func))

        # Validate and apply docstring fields
        flow_tags = [label]
        if docstring_fields["tags"]:
            additional_tags = [
                i.strip() for i in docstring_fields["tags"].split(",") if i.strip()
            ]
        else:
            additional_tags = []
        if image_name == "infer":
            image_name = docstring_fields["image_name"]
        image_uri = f"{DOCKER_REGISTRY}/{image_name}:{image_branch}"
        try:
            main_params = json.loads(docstring_fields["main_params"])
        except json.decoder.JSONDecodeError as err:
            raise ValueError(":main_params: label must provide valid JSON") from err

        # Deploy
        work_pool_name = f"k8s-{label}"
        flow_obj = flow.from_source(
            source=GitRepository(
                url=f"https://github.com/{GITHUB_ORG}/{REPO_PREFIX}{repo_name}.git",
                branch=branch_name,
                credentials={"access_token": Secret.load(GITHUB_SECRET_BLOCK)},
            ),
            entrypoint=f"{LOCAL_FLOW_FOLDER}/{module_name}.py:{flow_function_name}",
        )
        if inferred_label == "main" and label == "main":
            flow_tags.extend(additional_tags)
            if docstring_fields["schedule"] == "None":
                schedule = None
            else:
                schedule = CronSchedule(
                    cron=docstring_fields["schedule"], timezone=TIMEZONE
                )
            deployment_id = flow_obj.deploy(
                name=deployment_name,
                work_pool_name=work_pool_name,
                image=image_uri,
                tags=flow_tags,
                schedule=schedule,
                parameters=main_params,
                build=False,
                print_next_steps=False,
            )
            print(
                f"Deployed {deployment_name}\n\tWork Pool: {work_pool_name}\n\t"
                f"Docker image: {image_uri}\n\tTags: {flow_tags}\n\t"
                f"Schedule: {docstring_fields['schedule']}\n\tDeployment URL: "
                f"{settings.PREFECT_UI_URL.value()}/deployments/deployment/{deployment_id}"
            )
        else:
            deployment_id = flow_obj.deploy(
                name=deployment_name,
                work_pool_name=work_pool_name,
                image=image_uri,
                tags=flow_tags,
                build=False,
                print_next_steps=False,
            )
            print(
                f"Deployed {deployment_name}\n\tWork Pool: {work_pool_name}\n\t"
                f"Docker image: {image_uri}\n\tTags: {flow_tags}\n\tDeployment URL: "
                f"{settings.PREFECT_UI_URL.value()}/deployments/deployment/{deployment_id}"
            )
            if docstring_fields["schedule"] != "None":
                print(
                    f"\tSchedule not applied to dev deployment: {docstring_fields['schedule']}"
                )
            if additional_tags:
                print(
                    f"\tAdditional tags not added to dev deployment: {additional_tags}"
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
    # Raise error if current commit doesn't match origin commit
    if repo.head.commit != repo.remotes.origin.fetch()[0].commit:
        raise RuntimeError(
            "You are attempting to deploy using Github, but HEAD is not on the "
            "same commit as remote `origin`. Push or pull changes to continue."
        )
    # Also raise error if working tree is dirty (not counting untracked files)
    if repo.is_dirty():
        raise RuntimeError(
            "You are attempting to deploy using Github, but your working tree is not clean. "
            "Commit or discard changes to continue."
        )
    if branch_name == "main":
        label = "main"
    else:
        label = "dev"
    return label, repo_short_name, branch_name


# Use immutable tuples as default values to avoid dangerous-default-arg
def parse_docstring_fields(
    docstring: str,
    required_fields: tuple[str] = ("schedule", "image_name", "main_params"),
    optional_fields: tuple[str] = (
        "tags",
        "source_systems",
        "sink_systems",
        "customer_contact",
    ),
):
    """Looks at the given function docstring and extracts any Sphinx-style field labels (like
    `:tags: crm-ops`) from the docstring that match the expected fields. If field is not
    present in the docstring, its value is set to None. Finally, return a dictionary mapping field
    keys to values from the actual docstring.

    Note that field names cannot contain whitespace, though their values can, including line breaks.
    Line breaks and associated extra whitespace are reduced to a single space each.

    Raises ValueError if a field name (e.g. `:tags:`) appears more than once in the docstring.
    Raises ValueError if a field name (e.g. `:bad_label:`) is in the docstring but not in either
        list of fields.
    Raises ValueError if a required field is not in the docstring.
    """

    all_fields = list(set(optional_fields + required_fields))
    result = {}
    if docstring:
        # Bare regex isn't quite strong enough to allow for multi-line values for each label
        # So we have to work line-by-line through the docstring
        lines = docstring.split("\n")

        # Find field labels and their corresponding values
        field = None
        value = []
        for line in lines:
            # See if this line is the start of a new field
            match = re.match(r"^\s*:(\w+):\s*(.*)", line)
            if match:
                # Yes, we found a new label and thus we will start capturing a new field
                if field:
                    # First, if we were previously capturing a field-value, tie that off now
                    result[field] = " ".join(value).strip()
                # Now validate the new field
                field, val = match.groups()
                if field not in all_fields:
                    raise ValueError(
                        f"Label '{field}' found in docstring is not valid; "
                        f"must be one of: {all_fields}"
                    )
                if field in result:
                    raise ValueError(
                        f"Found multiple entries for '{field}' in flow docstring; "
                        "must have at most 1"
                    )
                # And begin capturing the new value
                value = [val.strip()]
            else:
                if field:
                    # This line isn't a new field, but we already started capturing a previous one
                    # So add this line to that field's value
                    value.append(line.strip())

        # We've reached the end of the document, so tie off the last field we were capturing
        if field:
            result[field] = " ".join(value).strip()
            print(result[field])

    # Check that all required fields were found
    for field in required_fields:
        if field not in result:
            raise ValueError(f"Missing required field :{field}: in docstring")
    # Set default values for fields not present in the docstring
    for field in optional_fields:
        if field not in result:
            result[field] = None
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
