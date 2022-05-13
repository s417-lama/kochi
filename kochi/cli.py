import click
import subprocess

from . import settings
from . import stats
from . import worker
from . import job_queue

@click.group()
def cli():
    settings.ensure_init()

# alloc_interact
# -----------------------------------------------------------------------------

@cli.command(name="alloc_interact")
@click.argument("machine", required=True)
@click.option("-n", "--nodes", metavar="NODES_SPEC", help="Specification of nodes to be allocated on machine MACHINE")
def alloc_interact_cmd(machine, nodes):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    config = settings.machine_config(machine)
    login_host = config["login_host"]
    commands_on_login_node = config["alloc_interact"]
    if "work_dir" in config:
        commands_on_login_node = "cd {} && {}".format(config["work_dir"], commands_on_login_node)
    subprocess.run("ssh -o LogLevel=QUIET -t {} '{}'".format(login_host, commands_on_login_node), shell=True)

# enqueue
# -----------------------------------------------------------------------------

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to enqueue a job")
def enqueue_cmd(commands, queue):
    """
    Enqueues a job that runs commands COMMANDS to queue QUEUE.
    """
    job = job_queue.Job(name="", dependencies="", environment="", commands=list(commands))
    job_queue.push(queue, job)

# work
# -----------------------------------------------------------------------------

@cli.command(name="work")
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-b", "--blocking", is_flag=True, default=False, help="Whether to block to wait for job arrival")
def work_cmd(queue, blocking):
    """
    Start a new worker that works on queue QUEUE.
    """
    worker.start(queue, blocking)

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
