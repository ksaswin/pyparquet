import click

import src.click_cli.cmd as click_cmd


@click.group()
@click.version_option()
def cli():
    pass

cli.add_command(click_cmd.cat)
cli.add_command(click_cmd.head)
cli.add_command(click_cmd.tail)
cli.add_command(click_cmd.csv)
