import logging

import click
import click_log
from configparser import NoOptionError

from .configure import _load_config, CFG_FILE, PROFILE
from .create_job_cluster import create_job_library
from .update_databricks_library import update_databricks

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def _resolve_input(variable, variable_name, config_key, config):
    """
    Resolve input entered as option values with config values

    If option values are provided (passed in as `variable`), then they are
     returned unchanged. If `variable` is None, then we first look for a config
     value to use.
    If no config value is found, then raise an error.

    Parameters
    ----------
    variable: string or numeric
        value passed in as input by the user
    variable_name: string
        name of the variable, for clarity in the error message
    config_key: string
        key in the config whose value could be used to fill in the variable
    config: ConfigParser
        contains keys/values in .storkcfg
    """
    if variable is None:
        try:
            variable = config.get(PROFILE, config_key)
        except NoOptionError:
            raise ValueError((
                'no {} found - either provide a command line argument or '
                'set up a default by running `stork configure`'
            ).format(variable_name))
    return variable


@click.command(short_help='upload an egg or jar')
@click.option(
    '-p',
    '--path',
    help=('path to egg or jar file with name as output from setuptools '
          '(e.g. dist/new_library-1.0.0-py3.6.egg '
          'or libs/new_library-1.0.0.jar)'),
    required=True
)
@click.option(
    '-t',
    '--token',
    help=('Databricks API key - '
          'optional, read from `.storkcfg` if not provided'),
)
@click.option(
    '-f',
    '--folder',
    type=str,
    help=('Databricks folder to upload to '
          '(e.g. `/Users/my_email@fake_organization.com`) '
          '- optional, read from `.storkcfg` if not provided'),
)
@click_log.simple_verbosity_option(logger)
def upload(path, token, folder):
    """
    The egg that the provided path points to will be uploaded to Databricks.
    """
    config = _load_config(CFG_FILE)
    token = _resolve_input(token, 'token', 'token', config)
    folder = _resolve_input(folder, 'folder', 'prod_folder', config)

    update_databricks(
        logger,
        path,
        token,
        folder,
        update_jobs=False,
        cleanup=False
    )


@click.command(short_help='upload an egg and update jobs')
@click.option(
    '-p',
    '--path',
    help=('path to egg file with name as output from setuptools '
          '(e.g. dist/new_library-1.0.0-py3.6.egg)'),
    required=True,
)
@click.option(
    '-t',
    '--token',
    help=('Databricks API key with admin permissions on all jobs using library'
          ' - optional, read from `.storkcfg` if not provided'),
)
@click.option(
    '--cleanup/--no-cleanup',
    help=('if cleanup, remove outdated files from production folder; '
          'if no-cleanup, remove nothing'),
    default=True,
    show_default=True,
)
@click_log.simple_verbosity_option(logger)
def upload_and_update(path, token, cleanup):
    """
    The egg that the provided path points to will be uploaded to Databricks.
     All jobs which use the same major version of the library will be updated
     to use the new version, and all version of this library in the production
     folder with the same major version and a lower minor version will
     be deleted.

    Unlike `upload`, `upload_and_update` does not ask for a folder because it
     relies on the production folder specified in the config. This is to
     protect against accidentally updating jobs to versions of a library still
     in testing/development.

    All egg names already in Databricks must be properly formatted
     with versions of the form <name>-0.0.0.
    """
    config = _load_config(CFG_FILE)
    token = _resolve_input(token, 'token', 'token', config)
    folder = _resolve_input(None, 'folder', 'prod_folder', config)

    update_databricks(
        logger,
        path,
        token,
        folder,
        update_jobs=True,
        cleanup=cleanup
    )


@click.command(short_help='create a cluster based on a job_id')
@click.option(
    '-j',
    '--job_id',
    help='job id of job you want to debug',
    required=True
)
@click.option(
    '-c',
    '--cluster_name',
    default=None,
    help=('Cluster Name- '
          'optional, use default value if not provided'),
)
@click.option(
    '-t',
    '--token',
    help=('Databricks API key - '
          'optional, read from `.storkcfg` if not provided'),
)
@click_log.simple_verbosity_option(logger)
def create_cluster(job_id, cluster_name, token):
    """
    Create a cluster based on a job id
    """
    config = _load_config(CFG_FILE)
    token = _resolve_input(token, 'token', 'token', config)

    create_job_library(
        logger,
        job_id,
        cluster_name,
        token
    )
