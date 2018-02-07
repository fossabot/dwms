import click
import os
import yaml
import requests
import logging

from enum import IntEnum, Enum
from elasticsearch import Elasticsearch
from collections import defaultdict
from datetime import datetime
from operator import itemgetter
from itertools import groupby


__version__ = '0.1'


# Please just shut up
logging.getLogger('elasticsearch').setLevel(100)


class Status(IntEnum):

    # Lower the better, like golf
    OKAY = 0
    IN_PROGRESS = 1
    PARTIAL = 2
    BAD_HEALTH = 3
    TIMED_OUT = 4
    MISSING = 5
    FAILED = 6

    def __str__(self):
        return self.name.replace('_', ' ')

    def get_color(self):
        return Color[self.name]


class Color(Enum):

    # Slack colors
    GOOD = 'good'
    WARNING = 'warning'
    BAD = 'danger'

    # Status -> colors
    OKAY = 'good'
    IN_PROGRESS = 'warning'
    PARTIAL = 'warning'
    BAD_HEALTH = 'danger'
    TIMED_OUT = 'danger'
    MISSING = 'danger'
    FAILED = 'danger'


def get_snapshots(cluster_config, repository):
    """
    Get list of snapshots.

    Args:
        cluster_config: cluster specific config
        repository: repository name

    Returns:
        Returns list of snapshots from the given cluster:repo
    """
    es = cluster_config['es']
    snapshots = es.cat.snapshots(
        repository=repository,
        format='json',
        request_timeout=300
    )
    return snapshots


def check_snapshots(cluster_config):
    """
    Check if given patterns for a cluster exist.

    Args:
        cluster_config: cluster specific config

    Returns:
        Dictionary of each cluster with corresponding snapshots grouped by
        where they are categorized.
    """
    results = defaultdict(lambda: defaultdict(dict))

    # For each repository, grab the required snapshots
    for repository, repository_config in cluster_config['repositories'].items():

        # Setup our results, patterns and snapshots
        repo_results = results[repository]
        patterns = repository_config['patterns']
        try:
            snapshots = get_snapshots(cluster_config, repository)
        except:
            repo_results['timed_out'] = True
            continue
        snapshots = {s['id']: s['status'] for s in snapshots}

        # Pop off the patterns that exist in the snapshots
        for s, v in snapshots.items():
            if s in patterns:
                repo_results['found'][s] = v
                patterns.remove(s)

        # Patterns leftover are missing!
        repo_results['missing'] = patterns

        # Check the patterns that exist for 'in progress' states
        repo_results['progress'] = {
            s: v for s, v in repo_results['found'].items()
            if v == 'IN_PROGRESS'
        }

        # Check for partials, partials may indicate failed shards 
        # and a larger problem
        repo_results['partial'] = {
            s: v for s, v in repo_results['found'].items()
            if v == 'PARTIAL'
        }

        # Failed snapshots
        repo_results['failed'] = {
            s: v for s, v in repo_results['found'].items()
            if v == 'FAILED'
        }

    return results


def evaluate_snapshots(statuses):
    """
    Takes the results of check_snapshots for a cluster and returns a value
    based on the severity of the situation.

    Severity ranges from 0-3; "Okay" to "Shit is broken, yo".

    Args:
        statuses: status dict for a cluster (from :func:`check_snapshots`)

    Returns:
        Maximum status level from the cluster
    """
    cluster_statuses = []

    for repo, repo_statuses in statuses.items():

        if repo_statuses.get('timed_out', False):
            repo_status = Status.TIMED_OUT
            cluster_statuses.append(repo_status)
            continue

        # Evaluate each type
        is_progress = len(repo_statuses['progress']) > 0
        is_partial = len(repo_statuses['partial']) > 0
        is_missing = len(repo_statuses['missing']) > 0
        is_failed = len(repo_statuses['failed']) > 0

        # Which is the highest?
        repo_status = Status.OKAY
        repo_status = Status.IN_PROGRESS if is_progress else repo_status
        repo_status = Status.PARTIAL if is_partial else repo_status
        repo_status = Status.MISSING if is_missing else repo_status
        repo_status = Status.FAILED if is_failed else repo_status

        cluster_statuses.append(repo_status)

    status = max(cluster_statuses)

    return status


def check_credentials(cluster_config):
    """
    Check if the credentials are even correct.

    Args:
        cluster_config: cluster specific config

    Returns:
        True or false if it receives a successful health check
    """
    try:
        cluster_config['es'].cluster.health(request_timeout=300)
    except Exception as e:
        return False

    return True


def build_patterns(config, date=datetime.now()):
    """
    Format date strings for patterns. Takes in global config.

    Args:
        config: global settings config
        date: date str or object to override current datetime with

    Returns:
        The global settings config but with updated per cluster pattern sets
    """
    cluster_settings = config['clusters']

    # Set date override
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')

    # Fill in patterns
    for cluster in cluster_settings:
        for repo, repo_config in cluster['repositories'].items():
            repo_config['patterns'] = list(
                map(
                    lambda x: datetime.strftime(date, x),
                    repo_config['patterns']
                )
            )
            repo_config['patterns'] = list(set(repo_config['patterns']))

    return config


def build_cluster_info(config):
    """
    Construct our cluster settings. Dumps global settings config to each cluster
    settings, however, does not overwrite local cluster settings. Cluster config
    takes precedence over global. Use global config for generic info to be
    applied by default.

    Note:
        However, as in the docstring for :func:`send_to_notifiers`, the local
        settings are not used for anything other than user/pass for getting
        snapshot information. There's not a need to send notifications to
        multiple areas at the time of writing. I doubt this will change in the
        future.

    Args:
        config: global settings config

    Returns:
        Global settings config, but with each cluster config is updated with
        global settings.
    """
    global_settings = config['settings']
    cluster_settings = config['clusters']

    for cluster in cluster_settings:

        # Set global as settings if not present
        if 'settings' not in cluster:
            cluster['settings'] = global_settings
            continue

        # Only add/update keys not present in cluster
        for k, v in global_settings.items():
            if k not in cluster['settings']:
                cluster['settings'][k] = v

    return config


def load_config(config):
    """
    Load config, expand env vars, and load yaml. Data is read in to avoid a very
    odd problem with load_safe.

    Args:
        config: config filename

    Returns:
        Dictionary loaded from yaml file
    """
    with open(config) as f:
        data = f.read()
    data = os.path.expandvars(data)
    data = yaml.safe_load(data)
    return data


def send_to_zabbix(results, config):
    """
    Send results to zabbix with prefixes and whatnot.

    Args:
        results: results from the complete evaluation of snapshots statuses
        config: global settings config
    """
    raise NotImplementedError


def send_to_hipchat(results, config):
    """
    Send results to Hipchat URL (room, person, etc).

    Args:
        results: results from the complete evaluation of snapshots statuses
        config: global settings config
    """

    # TODO: This doesn't work anymore, but since we no longer use HipChat
    # it's not worth looking at anymore. If I get around to it I'll swap it
    # to use Status & Color appropriately
    color = {
        0: 'green',
        1: 'yellow',
        2: 'red',
        3: 'red'
    }

    message = []
    max_level = max(results.values())[0]

    # generate our table of stuff
    message.append('<table>')
    for k, v in results.items():
        level, status = v
        message.append(
            '<tr>'
            f'<td><a href="https://{k}">{k}</a></td>'
            f'<td>{status}</td>'
            '</tr>'
        )
    message.append('</table>')

    if max_level == 0:
        message.append('<br>')
        message.append('<strong>All good! 👌😎👌</strong>')

    message = '\n'.join(message)

    payload = {
        'color': color[max_level],
        'message': message,
        'message_format': 'html',
        'notify': False,
        'from': "Dude, where's my snapshots?"
    }

    headers = {
        'Authorization': 'Bearer ' + config['notifiers']['hipchat']['token']
    }

    rv = requests.post(
        config['notifiers']['hipchat']['url'],
        json=payload,
        headers=headers
    )


def send_to_slack(results, config):
    """
    Send to slack.

    Args:
        results: results from the complete evaluation of snapshots statuses
        config: global settings config
    """
    slack_url = config['notifiers']['slack']['url']
    attachments = []
    grouped = groupby(sorted(results.items(), key=itemgetter(1)), key=itemgetter(1))

    for key, group in grouped:
        items = [v[0] for v in list(group)]
        total = len(items)
        attachments.append({
            'color': key.get_color().value,
            'title': f'{str(key).title()} ({total})',
            'text': '\n'.join(items)
        })

    payload = {'attachments': attachments}

    requests.post(
        slack_url,
        json=payload,
        timeout=10
    )



def send_to_stdout(results):
    """
    Dump results to stdout, just as it would be to Hipchat/Slack (soon).

    Note:
        This looks a bit garbage because it has to put together a clean
        table, didn't want to import some dependency to do it for me since
        it's such a one-off kind of thing that's only useful when run as
        an ad-hoc job with debug on.

    Args:
        results: results from the complete evaluation of snapshots statuses
    """
    color = {
        0: 'green',
        1: 'yellow',
        2: 'yellow',
        3: 'red',
        4: 'red',
        5: 'red',
        6: 'red'
    }

    raw_messages = []

    for k, v in results.items():
        status = str(v)
        level = v.value
        status = click.style(status, fg=color[level])
        raw_messages.append((f'{k}:', status, level))

    max_len = max(
        raw_messages,
        key=lambda m: len(m[0]) + len(m[1])
    )
    max_len = len(''.join(max_len[:2]))

    # TODO: Cleanup, this is a bit hard to follow
    for message in raw_messages:
        name, status, level = message

        status_len = len(''.join(message[:2]))
        if status_len == max_len:
            status_len = max_len - status_len - 2
            dot_line = '.' * status_len
            status_line = f'{name}{dot_line} {status}'
        else:
            status_len = max_len - len(''.join(message[:2])) - 1
            dot_line = '.' * status_len
            status_line = f'{name} {dot_line} {status}'

        click.secho(status_line, err=True if level > 0 else False)


def send_to_notifiers(results, config):
    """
    Whip through each notifier and do the thing.

    Note:
        This could be changed to iterate and submit information for each cluster
        utilizing the cluster specific settings, but there's no practical need
        for this behavior.

        Right now the only cluster specific information that we would possibly
        need is the login information for retrieving snapshot info.

    Args:
        results: results from the complete evaluation of snapshots statuses
        config: global settings config (used for some notifiers)
    """

    if 'notifiers' in config and len(config['notifiers']) > 0:
        notifiers = config['notifiers']

        if 'zabbix' in notifiers:
            send_to_zabbix(results, config)

        if 'hipchat' in notifiers:
            send_to_hipchat(results, config)

        if 'slack' in notifiers:
            send_to_slack(results, config)

        if 'stdout' in notifiers and notifiers['stdout']:
            send_to_stdout(results)

    else:
        # You dummy, you didn't set any outputs
        click.echo('You have no notifiers set, dumping to stdout instead')
        send_to_stdout(results)


def create_clients(config):
    """
    Create Elasticsearch clients (do not test if they work, that's later).

    Args:
        config: global settings config
    """
    for cluster_config in config['clusters']:
        auth = (
            cluster_config['settings']['username'],
            cluster_config['settings']['password']
        )
        cluster_config['es'] = Elasticsearch(
            cluster_config['endpoint'],
            port=cluster_config['port'],
            use_ssl=True if cluster_config['protocol'] == 'https' else False,
            verify_certs=True,
            http_auth=auth
        )

    return config


@click.command()
@click.version_option(version=__version__)
@click.argument(
    'config',
    default='config.yaml',
    type=click.Path(exists=True, readable=True)
)
@click.option(
    '--date',
    type=str,
    help="Override date, use format YYYY-MM-DD"
)
@click.option(
    '-d',
    '--debug',
    is_flag=True,
    help="Don't send info, show everything"
)
def main(config, date, debug):
    """
    Check each cluster:repo(s) pair for the patterns specified in the config
    for the current day's snapshot(s).
    """
    # Build settings
    config = load_config(config)
    config = build_cluster_info(config)
    config = build_patterns(config, date or datetime.now())
    config = create_clients(config)

    # Check if each cluster is accessible before checking
    for cluster_config in config['clusters']:
        endpoint = cluster_config['endpoint']
        health = check_credentials(cluster_config)

        if not health:
            click.secho(f'Cluster "{endpoint}" health check failed! Skipping!', err=True, fg='red')
            cluster_config['ignore'] = True

    # Do snapshot checks for each cluster
    # TODO: needs catch for timeout, if timeout, status should read TIMEOUT
    results = {}
    for cluster_config in config['clusters']:
        if cluster_config.get('ignore', False):
            results[cluster_config['endpoint']] = Status.BAD_HEALTH
            continue
        statuses = check_snapshots(cluster_config)
        statuses = evaluate_snapshots(statuses)
        results[cluster_config['endpoint']] = statuses

    # Send results to zabbix, hipchat, whatever, or stdout only if debug
    # TODO: debug should still use the notifiers, but dump their output instead
    if not debug:
        send_to_notifiers(results, config)
    else:
        send_to_stdout(results)


if __name__ == '__main__':
    # TODO: implement logging via structlog to catch unhandled exceptions
    main()
