import click
from configparser import ConfigParser
from os.path import expanduser, join


CFG_FILE = join(expanduser('~'), '.storkcfg')
PROFILE = 'DEFAULT'


def _load_config(filename):
    """
    Reads in existing config

    Returns
    -------
    config class with values read from existing file
    """
    config = ConfigParser()
    config.read(filename)
    return config


def _update_value(config, key, instruction, is_sensitive):
    """
    creates (if needed)  and updates the value of the key in the config with a
     value entered by the user

    Parameters
    ----------
    config: ConfigParser object
        existing configuration
    key: string
        key to update
    instruction: string
        text to show in the prompt
    is_sensitive: bool
        if true, require confirmation and do not show typed characters

    Notes
    -----
    sets key in config passed in
    """
    if config.has_option(PROFILE, key):
        current_value = config.get(PROFILE, key)
    else:
        current_value = None

    proposed = click.prompt(
        instruction,
        default=current_value,
        hide_input=is_sensitive,
        confirmation_prompt=is_sensitive,
    )

    if key == 'host' or key == 'prod_folder':
        if proposed[-1] == '/':
            proposed = proposed[:-1]

    if key == 'prod_folder':
        if proposed[0] != '/':
            proposed = '/' + proposed

    if key == 'host':
        if 'http' != proposed[:4]:
            proposed = click.prompt(
                ("looks like there's an issue - "
                 'make sure the host name starts with http'),
                default=current_value,
                hide_input=is_sensitive,
                confirmation_prompt=is_sensitive,
            )
    config.set(PROFILE, key, proposed)


@click.command(short_help='configure Databricks connection information')
def configure():
    """
    Configure information about Databricks account and default behavior.

    Configuration is stored in a `.storkcfg` file. A config file must exist
     before this package can be used, and can be supplied either directly as a
     text file or generated using this configuration tool.
    """
    config = _load_config(CFG_FILE)

    _update_value(
        config,
        'host',
        'Databricks host (e.g. https://my-organization.cloud.databricks.com)',
        is_sensitive=False,
    )
    _update_value(
        config,
        'token',
        'Databricks API token',
        is_sensitive=True,
    )
    _update_value(
        config,
        'prod_folder',
        'Databricks folder for production libraries',
        is_sensitive=False,
    )

    with open(CFG_FILE, 'w+') as f:
        config.write(f)
