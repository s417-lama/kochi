import click

from . import stats
from . import worker
from . import job_queue

@click.group()
def cli():
    pass

# enqueue
# -----------------------------------------------------------------------------

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
@click.option("-q", "--queue", required=True)
def enqueue_cmd(commands, queue):
    """
    Enqueues a job that runs commands COMMANDS.
    """
    job = job_queue.Job(name="", dependencies="", environment="", commands=list(commands))
    job_queue.push(queue, job)

# work
# -----------------------------------------------------------------------------

@cli.command(name="work")
@click.argument("queue", required=True)
def work_cmd(queue):
    """
    Start a new worker that works on queue QUEUE.
    """
    worker.start(queue)

# show
# -----------------------------------------------------------------------------

@cli.group()
def show():
    pass

@show.command(name="queues")
def queues_cmd():
    stats.show_queues()

@show.command(name="workers")
def workers_cmd():
    stats.show_workers()

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@log.command(name="worker")
@click.argument("worker_id", required=True, type=int)
def worker_cmd(worker_id):
    """
    Show a log file of the worker whose ID is WORKER_ID.
    """
    stats.show_worker_log(worker_id)
