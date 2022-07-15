"""General utility functions to make flows easier to implement with Prefect Cloud"""

import os
import smtplib
import traceback
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from datetime import datetime

import prefect
from prefect import task
from prefect.client import Secret
from prefect import storage
from prefect.backend import kv_store
from prefect.run_configs.docker import DockerRun
from prefect.exceptions import ClientError
import git

@task
def send_email(addressed_to: str, subject: str, body: str, attachments: list, smtp_info: dict):
    """Sends an email.

    :param addressed_to: A list of emails to send to separated by ", "
    :param subject: Email subject
    :param body: Plain text email body
    :param attachments: List of (bytes, str) tuples giving the file contents and the filename of
        objects to attach
    :param smtp_info: Dict with keys "from" (sender email), "host", and "port"
    """

    msg = MIMEMultipart()
    msg["From"] = smtp_info['from']
    msg["To"] = addressed_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body))

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

def run_local(flow, mode='run', argv=None, **kwargs):
    """Runs the flow locally, or if mode="visualize", does that instead. Keyword arguments
    translate into the flow parameters. Or pass a list for argv and these will be parsed as
    parameters with the format `{name}={value}`."""

    flow.storage = None
    flow.run_config = None
    if mode == 'visualize':
        flow.visualize()
    else:
        if argv:
            for entry in argv:
                name, value = entry.split('=')
                kwargs[name] = value
        flow.run(**kwargs)

def get_github_storage(flow_filename):
    """Returns a GitHub storage object based on the current Git branch"""

    repo = git.Repo()
    return storage.GitHub(repo=f'UCBoulder/{os.path.basename(repo.working_dir)}',
                          ref=repo.active_branch.name,
                          path=f'flows/{flow_filename}',
                          access_token_secret='GITHUB_ACCESS_TOKEN')

def get_docker_run(image):
    """Returns a DockerRun object pointing to the given registry image with either a "main" or
    "dev" label applied depending on whether the active Git branch is "main" or not."""

    repo = git.Repo()
    if repo.active_branch.name == "main":
        return DockerRun(image=image, labels=['main'])
    return DockerRun(image=image, labels=['dev'])

def get_config_value(key: str):
    """Retrieves the value associated with the given key in the KV Store; but this can be
    overridden by the Flow's runtime context."""

    try:
        value = prefect.context[key]
        prefect.context.get('logger').info(f'Extracted value for "{key}" from Flow Context')
        return value
    except KeyError:
        prefect.context.get('logger').info(
            f'Extracting value for "{key}" from KV Store since it is not set in Flow Context')
        return kv_store.get_key_value(key)

def reveal_secrets(config) -> dict:
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
            return Secret(obj[8:]).get()
        return obj

    return recursive_reveal(config)

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
            records = kv_store.get_key_value('pull_push_records').to_list()
        except ValueError:
            records = []
        except ClientError:
            # Not connected to Cloud: just do nothing
            return
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
            kv_store.set_key_value('pull_push_records', records)
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
