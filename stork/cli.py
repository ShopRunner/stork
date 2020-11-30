import click

from . import __version__
from .cli_commands import create_cluster, upload, upload_and_update
from .configure import configure


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('Version {}'.format(__version__))
    ctx.exit()


@click.group()
@click.option('--version', '-v', is_flag=True, callback=print_version,
              help=__version__)
def cli(version):
    pass


cli.add_command(configure)
cli.add_command(create_cluster)
cli.add_command(upload)
cli.add_command(upload_and_update)
