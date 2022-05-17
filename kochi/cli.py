from collections import namedtuple
import os
import subprocess
import sys
import string
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
@click.option("-m", "--machine", metavar="MACHINE", required=True, help="Machine to run a job on")
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
def alloc_interact_cmd(machine, nodes):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    config = settings.machine_config(machine)
    login_host = settings.machine_config(machine)["login_host"]
    env_dict = dict(KOCHI_ALLOC_NODE_SPEC=nodes)
    commands_on_login_node = string.Template(config["alloc_interact"]).substitute(env_dict)
    util.run_command_ssh_interactive(login_host, commands_on_login_node, cwd=config.get("work_dir"))

# alloc
# -----------------------------------------------------------------------------

AllocArgs = namedtuple("AllocArgs", ["machine", "queue", "nodes", "duplicates", "commands"])

@cli.command(name="alloc")
@click.option("-m", "--machine", metavar="MACHINE", required=True, help="Machine to allocate")
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
@click.option("-d", "--duplicates", metavar="DUPLICATES", type=int, default=1, help="Number of workers to be created")
def alloc_cmd(machine, queue, nodes, duplicates):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    config = settings.machine_config(machine)
    login_host = config["login_host"]
    args = AllocArgs(machine, queue, nodes, duplicates, config["alloc"])
    util.run_command_ssh_interactive(login_host, "kochi alloc_aux {}".format(util.serialize(args)), cwd=config.get("work_dir"))

@cli.command(name="alloc_aux", hidden=True)
@click.argument("args_serialized", required=True)
def alloc_aux_cmd(args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    for i in range(args.duplicates):
        worker_id = worker.get_worker_id()
        env_dict = dict(
            KOCHI_ALLOC_NODE_SPEC=args.nodes,
            KOCHI_WORKER_LAUNCH_CMD="kochi work -q {} -i {}".format(args.queue, worker_id),
        )
        commands = string.Template(args.commands).substitute(env_dict)
        try:
            subprocess.run(commands, shell=True, check=True)
            click.secho("Worker {} submitted.".format(worker_id), fg="green")
        except subprocess.CalledProcessError:
            click.secho("Submission of a system job for worker {} failed.".format(worker_id), fg="red", file=sys.stderr)
            exit(1)

# enqueue
# -----------------------------------------------------------------------------

EnqueueArgs = namedtuple("EnqueueArgs", ["machine", "queue", "job"])

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Machine to run a job on")
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to enqueue a job")
@click.option("-c", "--with-context", is_flag=True, default=False, help="Whether to create context of the current git repository")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
def enqueue_cmd(machine, queue, with_context, git_remote, commands):
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
        args = EnqueueArgs(machine, queue, job)
        util.run_command_ssh_interactive(login_host, "kochi enqueue_aux {}".format(util.serialize(args)))

@cli.command(name="enqueue_aux", hidden=True)
@click.argument("args_serialized", required=True)
def enqueue_aux_cmd(args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    job_queue.push(args.queue, args.job)

# work
# -----------------------------------------------------------------------------

@cli.command(name="work")
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-b", "--blocking", is_flag=True, default=False, help="Whether to block to wait for job arrival")
@click.option("-i", "--worker-id", type=int, default=-1, hidden=True, help="For internal use only")
def work_cmd(queue, blocking, worker_id):
    """
    Start a new worker that works on queue QUEUE.
    """
    worker.start(queue, blocking, worker_id)

# install
# -----------------------------------------------------------------------------

InstallArgs = namedtuple("InstallArgs", ["machine", "project_name", "dependency", "install_name", "context", "commands"])

@cli.command(name="install")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Machine on which project DEP will run")
@click.option("-d", "--dependency", metavar="DEP", required=True, help="Project name to be installed")
@click.option("-n", "--install-name", metavar="NAME", required=True, help="Installation name specified in the config")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.pass_context
def install_cmd(click_ctx, machine, dependency, install_name, git_remote):
    """
    Install project DEP of name NAME that is depended on by this repository on machine MACHINE.
    """
    project_name = project.project_name_of_cwd()
    login_host = settings.machine_config(machine)["login_host"] if machine != "local" else None
    local_dep_path = os.path.join(util.toplevel_git_dirpath(), settings.project_dep_config(dependency)["local_path"])
    dep_config = settings.project_dep_install_config(dependency, install_name)
    with util.cwd(local_dep_path):
        if not git_remote:
            project.sync(login_host)
        ctx = context.create_with_project_config(dep_config, git_remote)
    commands = "\n".join(dep_config["commands"]) if isinstance(dep_config["commands"], list) else dep_config["commands"]
    args = InstallArgs(machine, project_name, dependency, install_name, ctx, commands)
    if machine == "local":
        click_ctx.invoke(install_aux_cmd, args_serialized=util.serialize(args))
    else:
        util.run_command_ssh_interactive(login_host, "kochi install_aux {}".format(util.serialize(args)))

@cli.command(name="install_aux", hidden=True)
@click.argument("args_serialized", required=True)
def install_aux_cmd(args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    prefix = settings.project_dep_install_dirpath(args.project_name, args.dependency, args.install_name, args.machine)
    os.makedirs(prefix, exist_ok=True)
    with util.tmpdir(settings.project_dep_install_tmp_dirpath(args.project_name, args.dependency, args.install_name, args.machine)):
        with context.context(args.context):
            env_dict = dict(KOCHI_INSTALL_PREFIX=prefix)
            commands = string.Template(args.commands).substitute(env_dict)
            subprocess.run(commands, shell=True, check=True)

# show
# -----------------------------------------------------------------------------

@cli.group()
def show():
    pass

@show.command(name="queues")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
def show_queues_cmd(machine):
    if machine == "local":
        stats.show_queues()
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show queues")

@show.command(name="workers")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
def show_workers_cmd(machine):
    if machine == "local":
        stats.show_workers()
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show workers")

@show.command(name="jobs")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
def show_jobs_cmd(machine):
    if machine == "local":
        stats.show_jobs()
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show jobs")

@show.command(name="projects")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
def show_projects_cmd(machine):
    if machine == "local":
        stats.show_projects()
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show projects")

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@log.command(name="worker")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
@click.argument("worker_id", required=True, type=int)
def show_log_worker_cmd(machine, worker_id):
    """
    Show a log file of worker WORKER_ID on machine MACHINE.
    """
    if machine == "local":
        with open(settings.worker_log_filepath(worker_id)) as f:
            click.echo_via_pager(f)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show log worker {}".format(worker_id))

@log.command(name="job")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
@click.argument("job_id", required=True, type=int)
def show_log_job_cmd(machine, job_id):
    """
    Show a log file of job JOB_ID on machine MACHINE.
    """
    if machine == "local":
        with open(settings.job_log_filepath(job_id)) as f:
            click.echo_via_pager(f)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show log job {}".format(job_id))

# show path
# -----------------------------------------------------------------------------

@show.group()
def path():
    pass

@path.command(name="project")
@click.option("-m", "--machine", metavar="MACHINE", default="local", help="Target machine")
@click.option("-f", "--force", is_flag=True, default=False, help="Force to show the project path even if the project does not exist")
@click.argument("project_name", required=True, type=str)
def show_path_project_cmd(machine, force, project_name):
    """
    Show a path to PROJECT_NAME on machine MACHINE.
    """
    if machine == "local":
        project_path = settings.project_git_dirpath(project_name)
        if force or os.path.isdir(project_path):
            print(project_path)
        else:
            print("Project '{}' does not exist.".format(project_name), file=sys.stderr)
            sys.exit(1)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show path project -f {} {}".format(force, project_name))
