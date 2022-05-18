from collections import namedtuple
import os
import subprocess
import sys
import click

from . import util
from . import settings
from . import stats
from . import worker
from . import job_queue
from . import context
from . import project

machine_option = click.option("-m", "--machine", metavar="MACHINE", default="local", help="Machine name",
                              callback=lambda _c, _p, v: (settings.ensure_init_machine(v), v)[-1])

on_machine_option = click.option("--on-machine", is_flag=True, default=False, hidden=True)

@click.group()
def cli():
    settings.ensure_init()

# alloc_interact
# -----------------------------------------------------------------------------

@cli.command(name="alloc_interact")
@machine_option
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
def alloc_interact_cmd(machine, nodes):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    config = settings.machine_config(machine)
    login_host = settings.machine_config(machine)["login_host"]
    env_dict = dict(KOCHI_ALLOC_NODE_SPEC=nodes)
    util.run_command_ssh_interactive(login_host, config["alloc_interact"], cwd=config.get("work_dir"), env=env_dict)

# alloc
# -----------------------------------------------------------------------------

AllocArgs = namedtuple("AllocArgs", ["queue", "nodes", "duplicates", "commands"])

@cli.command(name="alloc")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
@click.option("-d", "--duplicates", metavar="DUPLICATES", type=int, default=1, help="Number of workers to be created")
def alloc_cmd(machine, queue, nodes, duplicates):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    config = settings.machine_config(machine)
    login_host = config["login_host"]
    args = AllocArgs(queue, nodes, duplicates, config["alloc"])
    util.run_command_ssh_interactive(login_host, "kochi alloc_aux -m {} {}".format(machine, util.serialize(args)),
                                     cwd=config.get("work_dir"))

@cli.command(name="alloc_aux", hidden=True)
@machine_option
@click.argument("args_serialized", required=True)
def alloc_aux_cmd(machine, args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    for i in range(args.duplicates):
        worker_id = worker.get_worker_id(machine)
        env_dict = dict(
            KOCHI_ALLOC_NODE_SPEC=args.nodes,
            KOCHI_WORKER_LAUNCH_CMD="kochi work -m {} -q {} -i {}".format(machine, args.queue, worker_id),
        )
        try:
            subprocess.run(util.decorate_command(args.commands, env=env_dict), shell=True, check=True)
            click.secho("Worker {} submitted on machine {}.".format(worker_id, machine), fg="green")
        except subprocess.CalledProcessError:
            click.secho("Submission of a system job for worker {} failed on machine {}.".format(worker_id, machine), fg="red", file=sys.stderr)
            exit(1)

# enqueue
# -----------------------------------------------------------------------------

def parse_dependencies(deps):
    ret = []
    for d in deps:
        ds = d.split(":")
        if len(ds) != 2:
            raise click.UsageError("Dependency must be specified as '--dependency (-d) DEPENDENCY_NAME:RECIPE_NAME'")
        ret.append((ds[0], ds[1]))
    return ret

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to enqueue a job")
@click.option("-c", "--with-context", is_flag=True, default=False, help="Whether to create context of the current git repository")
@click.option("-d", "--dependency", multiple=True, help="Project dependencies in the format DEPENDENCY_NAME:RECIPE_NAME")
@click.option("-n", "--name", metavar="JOB_NAME", default="ANON", help="Job name")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
def enqueue_cmd(machine, queue, with_context, dependency, name, git_remote, commands):
    """
    Enqueues a job that runs commands COMMANDS to queue QUEUE on machine MACHINE.
    """
    if with_context and not util.is_inside_git_dir():
        raise click.UsageError("--with-context (-c) option must be used inside a git directory.")
    login_host = settings.machine_config(machine)["login_host"] if machine != "local" else None
    if with_context and not git_remote:
        project.sync(login_host)
    ctx = context.create(git_remote) if with_context else None
    deps = parse_dependencies(dependency)
    job = job_queue.Job(name=name, machine=machine, queue=queue, dependencies=deps, context=ctx, commands=list(commands))
    if machine == "local":
        job_queue.push(job)
    else:
        util.run_command_ssh_interactive(login_host, "kochi enqueue_aux -m {} {}".format(machine, util.serialize(job)))

@cli.command(name="enqueue_aux", hidden=True)
@machine_option
@click.argument("job_serialized", required=True)
def enqueue_aux_cmd(machine, job_serialized):
    """
    For internal use only.
    """
    job = util.deserialize(job_serialized)
    job_queue.push(job)

# inspect
# -----------------------------------------------------------------------------

@cli.command(name="inspect")
@machine_option
@on_machine_option
@click.argument("worker_id", type=int, required=True)
def inspect_cmd(machine, on_machine, worker_id):
    """
    Inspect worker of WORKER_ID by connecting to the machine where the worker is running.
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    if on_machine:
        keypath = settings.sshd_clientkey_filepath()
        port = settings.sshd_port()
        util.ssh_to_machine(keypath, port, machine)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi inspect -m {} --on-machine {}".format(machine, worker_id))

# work
# -----------------------------------------------------------------------------

@cli.command(name="work")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-b", "--blocking", is_flag=True, default=False, help="Whether to block to wait for job arrival")
@click.option("-i", "--worker-id", type=int, default=-1, hidden=True, help="For internal use only")
def work_cmd(machine, queue, blocking, worker_id):
    """
    Start a new worker that works on queue QUEUE.
    Assume that this command is invoked on machine MACHINE.
    """
    worker.start(queue, blocking, worker_id, machine)

# install
# -----------------------------------------------------------------------------

InstallArgs = namedtuple("InstallArgs", ["project_name", "dependency", "recipe_name", "context", "commands"])

@cli.command(name="install")
@machine_option
@click.option("-d", "--dependency", metavar="DEP", required=True, help="Project dependency name to be installed")
@click.option("-r", "--recipe-name", metavar="RECIPE", required=True, help="Recipe name specified in the config")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.pass_context
def install_cmd(click_ctx, machine, dependency, recipe_name, git_remote):
    """
    Install project DEP with recipe RECIPE that is depended on by this repository on machine MACHINE.
    """
    project_name = project.project_name_of_cwd()
    login_host = settings.machine_config(machine)["login_host"] if machine != "local" else None
    local_dep_path = os.path.join(util.toplevel_git_dirpath(), settings.project_dep_config(dependency)["local_path"])
    dep_config = settings.project_dep_recipe_config(dependency, recipe_name)
    with util.cwd(local_dep_path):
        if not git_remote:
            project.sync(login_host)
        ctx = context.create_with_project_config(dep_config, git_remote)
    commands = "\n".join(dep_config["commands"]) if isinstance(dep_config["commands"], list) else dep_config["commands"]
    args = InstallArgs(project_name, dependency, recipe_name, ctx, commands)
    if machine == "local":
        click_ctx.invoke(install_aux_cmd, args_serialized=util.serialize(args))
    else:
        util.run_command_ssh_interactive(login_host, "kochi install_aux -m {} {}".format(machine, util.serialize(args)))

@cli.command(name="install_aux", hidden=True)
@machine_option
@click.argument("args_serialized", required=True)
def install_aux_cmd(machine, args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    prefix = settings.project_dep_install_dirpath(args.project_name, machine, args.dependency, args.recipe_name)
    os.makedirs(prefix, exist_ok=True)
    with util.tmpdir(settings.project_dep_install_tmp_dirpath(args.project_name, machine, args.dependency, args.recipe_name)):
        with context.context(args.context):
            env_dict = dict(KOCHI_INSTALL_PREFIX=prefix)
            subprocess.run(util.decorate_command(args.commands, env=env_dict), shell=True, check=True)

# show
# -----------------------------------------------------------------------------

@cli.group()
def show():
    pass

@show.command(name="queues")
@machine_option
@on_machine_option
def show_queues_cmd(machine, on_machine):
    if machine == "local" or on_machine:
        stats.show_queues(machine)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show queues -m {} --on-machine".format(machine))

@show.command(name="workers")
@machine_option
@on_machine_option
def show_workers_cmd(machine, on_machine):
    if machine == "local" or on_machine:
        stats.show_workers(machine)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show workers -m {} --on-machine".format(machine))

@show.command(name="jobs")
@machine_option
@on_machine_option
def show_jobs_cmd(machine, on_machine):
    if machine == "local" or on_machine:
        stats.show_jobs(machine)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show jobs -m {} --on-machine".format(machine))

@show.command(name="projects")
@machine_option
@on_machine_option
def show_projects_cmd(machine, on_machine):
    if machine == "local" or on_machine:
        stats.show_projects()
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show projects -m {} --on-machine".format(machine))

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@log.command(name="worker")
@machine_option
@on_machine_option
@click.argument("worker_id", required=True, type=int)
def show_log_worker_cmd(machine, on_machine, worker_id):
    """
    Show a log file of worker WORKER_ID on machine MACHINE.
    """
    if machine == "local" or on_machine:
        with open(settings.worker_log_filepath(machine, worker_id)) as f:
            click.echo_via_pager(f)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show log worker {} -m {} --on-machine".format(worker_id, machine))

@log.command(name="job")
@machine_option
@on_machine_option
@click.argument("job_id", required=True, type=int)
def show_log_job_cmd(machine, on_machine, job_id):
    """
    Show a log file of job JOB_ID on machine MACHINE.
    """
    if machine == "local" or on_machine:
        with open(settings.job_log_filepath(machine, job_id)) as f:
            click.echo_via_pager(f)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show log job {} -m {} --on-machine".format(job_id, machine))

# show path
# -----------------------------------------------------------------------------

@show.group()
def path():
    pass

@path.command(name="project")
@machine_option
@on_machine_option
@click.option("-f", "--force", is_flag=True, default=False, help="Force to show the project path even if the project does not exist")
@click.argument("project_name", required=True, type=str)
def show_path_project_cmd(machine, on_machine, force, project_name):
    """
    Show a path to PROJECT_NAME on machine MACHINE.
    """
    if machine == "local" or on_machine:
        project_path = settings.project_git_dirpath(project_name)
        if force or os.path.isdir(project_path):
            print(project_path)
        else:
            print("Project '{}' does not exist.".format(project_name), file=sys.stderr)
            sys.exit(1)
    else:
        login_host = settings.machine_config(machine)["login_host"]
        util.run_command_ssh_interactive(login_host, "kochi show path project -m {} --on-machine -f {} {}".format(machine, force, project_name))
