import click

from . import util
from . import settings
from . import stats
from . import worker
from . import job_queue
from . import context
from . import project

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
    util.run_command_ssh_interactive(login_host, commands_on_login_node)

# enqueue
# -----------------------------------------------------------------------------

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@click.argument("machine", required=True, nargs=1)
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to enqueue a job")
@click.option("-c", "--with-context", is_flag=True, default=False, help="Whether to create context of the current git repository")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
def enqueue_cmd(machine, commands, queue, with_context, git_remote):
    """
    Enqueues a job that runs commands COMMANDS to queue QUEUE on machine MACHINE.
    """
    if with_context and not util.is_inside_git_dir():
        raise click.UsageError("--with-context (-c) option must be used inside a git directory.")
    login_host = settings.machine_config(machine)["login_host"] if machine != "local" else None
    if with_context and not git_remote:
        project.sync(login_host)
    ctx = context.create(git_remote) if with_context else None
    job = job_queue.Job(name="", dependencies="", context=ctx, commands=list(commands))
    if machine == "local":
        job_queue.push(queue, job)
    else:
        job_str = util.serialize(job)
        util.run_command_ssh(login_host, "kochi enqueue_aux {} -q {} {}".format(machine, queue, job_str))

@cli.command(name="enqueue_aux", hidden=True)
@click.argument("machine", required=True)
@click.argument("job_string", required=True)
@click.option("-q", "--queue", required=True)
def enqueue_raw_cmd(machine, job_string, queue):
    """
    For internal use only.
    """
    job = util.deserialize(job_string)
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
def show_queues_cmd():
    stats.show_queues()

@show.command(name="workers")
def show_workers_cmd():
    stats.show_workers()

@show.command(name="jobs")
def show_jobs_cmd():
    stats.show_jobs()

@show.command(name="projects")
def show_projects_cmd():
    stats.show_projects()

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@log.command(name="worker")
@click.argument("worker_id", required=True, type=int)
def show_log_worker_cmd(worker_id):
    """
    Show a log file of worker WORKER_ID.
    """
    stats.show_worker_log(worker_id)

@log.command(name="job")
@click.argument("job_id", required=True, type=int)
def show_log_job_cmd(job_id):
    """
    Show a log file of job JOB_ID.
    """
    stats.show_job_log(job_id)

# show path
# -----------------------------------------------------------------------------

@show.group()
def path():
    pass

@path.command(name="project")
@click.argument("project_name", required=True, type=str)
def show_path_project_cmd(project_name):
    """
    Show a path to PROJECT_NAME.
    """
    print(settings.project_git_dirpath(project_name))
