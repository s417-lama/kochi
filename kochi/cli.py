import click

from . import stats

@click.group()
def cli():
    pass

# show
# -----------------------------------------------------------------------------

@cli.group()
def show():
    pass

@show.command()
def queues():
    stats.show_queues()

@show.command()
def workers():
    stats.show_workers()

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@log.command()
@click.argument("idx", required=True)
def worker(idx):
    stats.show_worker_log(idx)
