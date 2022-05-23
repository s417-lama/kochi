from collections import namedtuple
import os
import subprocess
import sys
import functools
import click

from . import util
from . import settings
from . import stats
from . import worker
from . import job_queue
from . import context
from . import project
from . import sshd

machine_option = click.option("-m", "--machine", metavar="MACHINE", default="local", help="Machine name",
                              callback=lambda _c, _p, v: (settings.ensure_init_machine(v), v)[-1])

on_machine_option = click.option("--on-machine", is_flag=True, default=False, hidden=True)

@click.group()
def cli():
    settings.ensure_init()

@cli.command(name="on_machine_aux", hidden=True)
@machine_option
@click.argument("cmd_serialized")
@click.pass_context
def on_machine_aux_cmd(click_ctx, machine, cmd_serialized):
    cmd = util.deserialize(cmd_serialized)
    click_ctx.invoke(globals()[cmd["funcname"]], machine=machine, on_machine=True, **cmd["kwargs"])

def on_machine_cmd(group, name):
    def decorator(f):
        @group.command(name=name)
        @machine_option
        @on_machine_option
        @functools.wraps(f)
        def wrapper(machine, on_machine, *args, **kwargs):
            if machine == "local" or on_machine:
                f(machine, *args, **kwargs)
            else:
                args_serialized = util.serialize(dict(funcname=f.__name__, kwargs=kwargs))
                cmd_on_machine = "kochi on_machine_aux -m {} {}".format(machine, args_serialized)
                login_host = settings.machine_config(machine)["login_host"]
                util.run_command_ssh_interactive(login_host, cmd_on_machine)
        return wrapper
    return decorator

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

AllocArgs = namedtuple("AllocArgs", ["queue", "nodes", "duplicates", "time_limit", "commands"])

@cli.command(name="alloc")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
@click.option("-d", "--duplicates", metavar="DUPLICATES", type=int, default=1, help="Number of workers to be created")
@click.option("-t", "--time-limit", metavar="TIME_LIMIT", help="Time limit for the system job")
def alloc_cmd(machine, queue, nodes, duplicates, time_limit):
    """
    Allocates nodes of NODES_SPEC on machine MACHINE as an interactive job
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    config = settings.machine_config(machine)
    login_host = config["login_host"]
    args = AllocArgs(queue, nodes, duplicates, time_limit, config["alloc"])
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
        worker_id = worker.init(machine, args.queue)
        env_dict = dict(
            KOCHI_ALLOC_NODE_SPEC=args.nodes,
            KOCHI_ALLOC_TIME_LIMIT=args.time_limit,
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

dependency_option = click.option("-d", "--dependency", metavar="DEPENDENCY_NAME:RECIPE_NAME", multiple=True, help="Project dependencies specified in the config")

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
@dependency_option
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
        job_enqueued = job_queue.push(job)
        click.secho("Job {} submitted on machine {}.".format(job_enqueued.id, machine), fg="blue")
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
    job_enqueued = job_queue.push(job)
    click.secho("Job {} submitted on machine {}.".format(job_enqueued.id, machine), fg="blue")

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
        sshd.login_to_machine(machine, worker_id)
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
    worker_id = worker.init(machine, queue, worker_id)
    worker.start(queue, blocking, worker_id, machine)

# install
# -----------------------------------------------------------------------------

InstallArgs = namedtuple("InstallArgs", ["project_name", "dependency", "recipe", "context", "envs", "commands"])

def get_install_context(dep_config, recipe_config, login_host, git_remote):
    if dep_config.get("local_path") and dep_config.get("git_remote"):
        raise click.UsageError("'local_path' and 'git_remote' cannot be simultaneously specified in dependency config.")
    if dep_config.get("local_path"):
        local_dep_path = os.path.join(util.toplevel_git_dirpath(), dep_config["local_path"])
        with util.cwd(local_dep_path):
            if not git_remote:
                project.sync(login_host)
            return context.create_with_project_config(recipe_config, git_remote)
    elif dep_config.get("git_remote"):
        if recipe_config.get("current_status"):
            raise click.UsageError("'current_status' cannot be used in 'git_remote' dependency.")
        return context.create_with_project_config(recipe_config, dep_config["git_remote"])
    else:
        return None

@cli.command(name="install")
@machine_option
@dependency_option
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.pass_context
def install_cmd(click_ctx, machine, dependency, git_remote):
    """
    Install projects that are depended on by this repository on machine MACHINE.
    """
    project_name = project.project_name_of_cwd()
    login_host = settings.machine_config(machine)["login_host"] if machine != "local" else None
    for d, r in parse_dependencies(dependency):
        dep_config = settings.project_dep_config(d)
        recipe_config = settings.project_dep_recipe_config(d, r)
        ctx = get_install_context(dep_config, recipe_config, login_host, git_remote)
        args = InstallArgs(project_name, d, r, ctx, recipe_config.get("envs", dict()), recipe_config["commands"])
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
    prefix = settings.project_dep_install_dirpath(args.project_name, machine, args.dependency, args.recipe)
    os.makedirs(prefix, exist_ok=True)
    with util.tmpdir(settings.project_dep_install_tmp_dirpath(args.project_name, machine, args.dependency, args.recipe)):
        with context.context(args.context):
            with util.tee(settings.project_dep_install_log_filepath(args.project_name, machine, args.dependency, args.recipe)) as tee:
                color = "magenta"
                print(click.style("Kochi installation for {}:{} started on machine {}.".format(args.dependency, args.recipe, machine), fg=color), file=tee.stdin, flush=True)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)
                env = os.environ.copy()
                env["KOCHI_MACHINE"] = machine
                env["KOCHI_INSTALL_PREFIX"] = prefix
                for k, v in args.envs.items():
                    env[k] = v
                try:
                    subprocess.run(util.decorate_command(args.commands), env=env, shell=True, check=True, stdout=tee.stdin, stderr=tee.stdin)
                except KeyboardInterrupt:
                    print(click.style("Kochi installation for {}:{} interrupted.".format(args.dependency, args.recipe), fg="red"), file=tee.stdin, flush=True)
                except BaseException as e:
                    print(click.style("Kochi installation for {}:{} failed: {}".format(args.dependency, args.recipe, str(e)), fg="red"), file=tee.stdin, flush=True)
                print(click.style("*" * 80, fg=color), file=tee.stdin, flush=True)

# show
# -----------------------------------------------------------------------------

@cli.group()
def show():
    pass

@on_machine_cmd(show, "queues")
def show_queues_cmd(machine):
    stats.show_queues(machine)

@on_machine_cmd(show, "workers")
@click.option("-a", "--all", is_flag=True, default=False, help="Show all workers including terminated ones.")
@click.option("-q", "--queue", multiple=True, help="Queues on which workers work on. Defaults to all queues.")
def show_workers_cmd(machine, all, queue):
    stats.show_workers(machine, all, queue)

@on_machine_cmd(show, "jobs")
@click.option("-a", "--all", is_flag=True, default=False, help="Show all jobs including terminated ones.")
@click.option("-q", "--queue", multiple=True, help="Queues for which jobs were submitted. Defaults to all queues.")
@click.option("-n", "--name", multiple=True, help="Job names to be queried. Defaults to all names.")
def show_jobs_cmd(machine, all, queue, name):
    stats.show_jobs(machine, all, queue, name)

@on_machine_cmd(show, "projects")
def show_projects_cmd(machine):
    stats.show_projects()

# show log
# -----------------------------------------------------------------------------

@show.group()
def log():
    pass

@on_machine_cmd(log, "worker")
@click.argument("worker_id", required=True, type=int)
def show_log_worker_cmd(machine, worker_id):
    """
    Show a log file of worker WORKER_ID on machine MACHINE.
    """
    with open(settings.worker_log_filepath(machine, worker_id)) as f:
        click.echo_via_pager(f)

@on_machine_cmd(log, "job")
@click.argument("job_id", required=True, type=int)
def show_log_job_cmd(machine, job_id):
    """
    Show a log file of job JOB_ID on machine MACHINE.
    """
    with open(settings.job_log_filepath(machine, job_id)) as f:
        click.echo_via_pager(f)

@on_machine_cmd(log, "install")
@click.option("-p", "--project", help="Target (base) project. Defaults to the project of the current directory.",
              callback=lambda _c, _p, v: project.project_name_of_cwd() if not v else v)
@dependency_option
def show_log_install_cmd(machine, project, dependency):
    """
    Show log files of installation of specified dependency recipes.
    """
    for d, r in parse_dependencies(dependency):
        with open(settings.project_dep_install_log_filepath(project, machine, d, r)) as f:
            click.echo_via_pager(f)

# show path
# -----------------------------------------------------------------------------

@show.group()
def path():
    pass

@on_machine_cmd(path, "project")
@click.option("-f", "--force", is_flag=True, default=False, help="Force to show the project path even if the project does not exist")
@click.argument("project_name", required=True, type=str)
def show_path_project_cmd(machine, force, project_name):
    """
    Show a path to PROJECT_NAME on machine MACHINE.
    """
    project_path = settings.project_git_dirpath(project_name)
    if force or os.path.isdir(project_path):
        print(project_path)
    else:
        print("Project '{}' does not exist.".format(project_name), file=sys.stderr)
        sys.exit(1)
