"""General utility functions to make flows easier to implement with Prefect Cloud"""

import importlib
import os
import sys
import smtplib
import traceback
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from datetime import datetime

import prefect
from prefect import task
from prefect.deployments import Deployment
from prefect.filesystems import RemoteFileSystem
from prefect.blocks.system import JSON
from prefect.blocks.system import Secret
from prefect.infrastructure.docker import DockerContainer
import git

DOCKER_REGISTRY = 'oit-data-services-docker-local.artifactory.colorado.edu/'

@task
def send_email(addressed_to: str,
               subject: str,
               body: str,
               smtp_info: dict,
               attachments: list =None):
    """Sends an email.

    :param addressed_to: A list of emails to send to separated by ", "
    :param subject: Email subject
    :param body: Plain text email body
    :param smtp_info: Dict with keys "from" (sender email), "host", and "port"
    :param attachments: List of (bytes, str) tuples giving the file contents and the filename of
        objects to attach
    """

    msg = MIMEMultipart()
    msg["From"] = smtp_info['from']
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

    mailserver = smtplib.SMTP(smtp_info['host'], smtp_info['port'])

    if addressed_to:
        info = f'Sending "{subject}" email to {addressed_to}'
        if attachments:
            info += ' with attachments ' + ', '.join([i[1] for i in attachments])
        info += f":\n{body[:500]} ..."
        prefect.context.get('logger').info(info)
        mailserver.sendmail(smtp_info['from'].split(", "),
                            addressed_to.split(", "),
                            msg.as_string())
        mailserver.quit()
        if attachments:
            record_push('smtp', smtp_info['host'], sum(len(i[0]) for i in attachments))
    else:
        info = f'No delivery contacts set; "{subject}" email not sent'
        if attachments:
            info += ' with attachments ' + ', '.join([i[1] for i in attachments])
        info += f":\n{body[:500]} ..."
        prefect.context.get('logger').warn(info)

def run_flow_command_line_interface(flow_filename, flow_function_name, args=None):
    """Provides a command line interface for running and deploying a flow. If args is none, will
    use sys.argv"""

    repo = git.Repo()
    if args is None:
        args = sys.argv[1:]
    command = args[0]
    options = args[1:]
    if command == 'deploy':
        if repo.active_branch.name == "main":
            label = 'main'
        else:
            label = 'dev'
        if options and options[0] == '--docker-label':
            docker_label = options[1]
        else:
            docker_label = 'main'
        repo_name = os.path.basename(repo.working_dir)
        module_name = os.path.splitext(os.path.basename(flow_filename))[0]
        module = importlib.import_module(module_name)
        flow_function = getattr(module, flow_function_name)
        if label == 'main':
            flow_function = flow_function.with_options(
                timeout_seconds=12*60*60,
                retries=2,
                retry_delay_seconds=60)

        docker = DockerContainer(
            image=f'{DOCKER_REGISTRY}/{repo_name}:{docker_label}',
            image_pull_policy='ALWAYS',
            auto_remove=True)
        file_system = RemoteFileSystem.load('flow-storage')
        deployment = Deployment.build_from_flow(
            flow=flow_function,
            name=f'{module_name}_{label}',
            tags=[label],
            work_queue_name=label,
            infrastructure=docker,
            storage=file_system,
            apply=True)
    else:
        raise ValueError(f'Command {command} is not implemented')





def reveal_secrets(json_obj) -> dict:
    """Looks for strings within a JSON-like object that start with '<secret>' and replaces these
    with Prefect Secrets. For example, the value '<secret>EDB_PW' would be replaced with the EDB_PW
    Prefect Secret value."""

    def recursive_reveal(obj):
        if isinstance(obj, dict):
            return {k:recursive_reveal(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [recursive_reveal(i) for i in obj]
        if isinstance(obj, str) and obj.startswith('<secret>'):
            prefect.context.get('logger').info(
                f'Extracting value for {obj[8:]} from Prefect Secrets')
            return Secret.load(obj[8:]).value
        return obj

    return recursive_reveal(json_obj)

def record_pull(source_type, source_name, num_bytes):
    """Makes a record in Prefect Cloud that data was pulled from a source system within a Flow
    context. Be sure to call this function if you ever write your own extraction task outside this
    package. Does nothing if the flow's "env" param is not "prod"."""

    _make_record('pull', str(source_type), str(source_name), int(num_bytes))

def record_push(sink_type, sink_name, num_bytes):
    """Makes a record in Prefect Cloud that data was pushed to a sink system within a Flow
    context. Be sure to call this function if you ever write your own extraction task outside this
    package. Does nothing if the flow's "env" param is not "prod"."""

    _make_record('push', str(sink_type), str(sink_name), int(num_bytes))

def _make_record(record_type, source_type, source_name, num_bytes):
    # pylint:disable=broad-except
    if prefect.context.get('parameters')['env'] == 'prod':
        try:
            # Convert the BoxList to a native list to avoid JSON dump issues
            records = list(JSON.load('pull-push-records').value)
        except ValueError:
            records = JSON(value=[])
        # Combine identical records within the same hour
        time = datetime.utcnow().strftime("%Y-%m-%dT%H:00:00")
        base_record = [record_type, prefect.context.get('flow_name'), source_type, source_name,
                       time]
        try:
            existing_record = next(i for i in records if i[:5] == base_record)
            existing_record[5] += num_bytes
        except StopIteration:
            records.append(base_record + [num_bytes])
        try:
            JSON(value=records).save('pull-push-records')
        except ValueError:
            # Not connection to cloud; just do nothing
            pass
        except Exception:
            prefect.context.get('logger').warn(
                f'Exception while recording sink data {record_type}:\n{traceback.format_exc()}')

def sizeof_fmt(num):
    """Takes a number of bytes and returns a human-readable representation"""

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"
