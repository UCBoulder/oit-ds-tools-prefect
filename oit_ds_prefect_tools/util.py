"""General utility functions to make flows easier to implement with Prefect Cloud"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Callable

import prefect
from prefect.client import Secret
from prefect import storage
from prefect import backend
from prefect.run_configs.docker import DockerRun
from prefect.exceptions import ClientError
import git

def failure_notifier(smtp_info_keyname: str, contacts_param: str) -> Callable:
    """Returns a function which can be passed to a Flow's on_failure argument in order to send
    a failure notification email. SMTP configuration info is extracted from the flow context using
    get_config_value. SMTP server info must include "host", "port", and "from" keys. Contacts is
    a string of email addresses separated by ", " and is extracted from the flow parameter
    named by contacts_param."""

    def send_notification(flow, state):
        def message(ptask):
            result = state.result.get(ptask)
            if result:
                return result.message
            return "not run"
        task_results = "\n".join([f'{i.name}: {message(i)}' for i in flow.sorted_tasks()])
        body = f'this prefect flow has failed. task result messages:\n\n{task_results}'

        smtp_info = get_config_value(smtp_info_keyname)
        contacts = prefect.context.get("parameters")[contacts_param]

        msg = MIMEMultipart()
        msg["from"] = smtp_info['from']
        msg["to"] = contacts
        msg["subject"] = (f'[{prefect.context.get("parameters")["env"].upper()}] ' +
                          f'{flow.name}: FAILURE')
        msg.attach(MIMEText(body))

        prefect.context.get('logger').info(
            f'Emailing failure notification to {contacts}:\nSUBJECT: {msg["subject"]}\n\n{body}')

        mailserver = smtplib.SMTP(smtp_info['host'], smtp_info['port'])
        mailserver.sendmail(smtp_info['from'].split(', '), contacts.split(', '), msg.as_string())
        mailserver.quit()
    return send_notification

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
        return backend.get_key_value(key)

def reveal_secrets(config: dict) -> dict:
    """Looks for <secret> values and replaces these with Secret values from Prefect"""

    out = {}
    for key, value in config.items():
        if isinstance(value, str) and value.startswith('<secret>'):
            prefect.context.get('logger').info(
                f'Extracting value for {value[8:]} from Prefect Secrets')
            out[key] = Secret(value[8:]).get()
        elif isinstance(value, dict):
            out[key] = {}
            for skey, sval in value.items():
                if isinstance(sval, str) and sval.startswith('<secret>'):
                    prefect.context.get('logger').info(
                        f'Extracting value for {sval[8:]} from Prefect Secrets')
                    out[key][skey] = Secret(sval[8:]).get()
                else:
                    out[key][skey] = sval
        else:
            out[key] = value
    return out

def _get_flow_record(source_sink_records):
    flow_name = prefect.context.get('flow_name')
    if flow_name not in source_sink_records:
        source_sink_records[flow_name] = {}
    run_id = prefect.context.get('flow_run_id')
    if run_id not in source_sink_records[flow_name]:
        source_sink_records[flow_name][run_id] = {'sources':[], 'sinks': []}
    return source_sink_records[flow_name][run_id]

def record_source(source_type, source_name, num_bytes):
    """Makes a record in Prefect Cloud that data was pulled from a source system within a Flow
    context. Be sure to call this function if you ever write your own extraction task outside this
    package."""

    try:
        records = backend.get_key_value('source_sink_records')
    except ValueError:
        records = {}
    except ClientError:
        # Not connected to Cloud: just do nothing
        return
    flow_record = _get_flow_record(records)
    flow_record['sources'].append(
        {'type': source_type, 'name': source_name, 'bytes': int(num_bytes)})
    backend.set_key_value('source_sink_records', records)

def record_sink(sink_type, sink_name, num_bytes):
    """Makes a record in Prefect Cloud that data was pulled from a source system within a Flow
    context. Be sure to call this function if you ever write your own extraction task outside this
    package."""

    try:
        records = backend.get_key_value('source_sink_records')
    except ValueError:
        records = {}
    except ClientError:
        # Not connected to Cloud: just do nothing
        return
    flow_record = _get_flow_record(records)
    flow_record['sinks'].append(
        {'type': sink_type, 'name': sink_name, 'bytes': int(num_bytes)})
    backend.set_key_value('source_sink_records', records)
