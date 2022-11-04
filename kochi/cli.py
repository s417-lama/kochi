from collections import namedtuple
import os
import subprocess
import sys
import string
import functools
import collections
import pathlib
import click

from . import util
from . import settings
from . import config
from . import stats
from . import worker
from . import job_queue
from . import job_manager
from . import context
from . import project
from . import sshd
from . import installer
from . import reverse_shell
from . import job_config
from . import artifact
from . import ssh_forward

def ensure_init():
    sshd.ensure_init()

def ensure_init_machine(machine):
    worker.ensure_init(machine)
    job_manager.ensure_init(machine)
    job_queue.ensure_init(machine)

def run_on_login_node(machine, script, **opts):
    util.run_command_ssh_interactive(config.login_host(machine), config.load_env_login_script(machine) + [script],
                                     cwd=config.work_dir(machine), env=opts.get("env"))

machine_option = click.option("-m", "--machine", metavar="MACHINE", required=True, help="Machine name", envvar="KOCHI_DEFAULT_MACHINE",
                              callback=lambda _c, _p, v: (ensure_init_machine(v), v)[-1])

on_machine_option = click.option("--on-machine", is_flag=True, default=False, hidden=True)

@click.group()
def cli():
    ensure_init()

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
                run_on_login_node(machine, "kochi on_machine_aux -m {} {}".format(machine, args_serialized))
        return wrapper
    return decorator

# alloc_interact
# -----------------------------------------------------------------------------

@cli.command(name="alloc_interact")
@machine_option
@click.option("-n", "--nodes", metavar="NODES_SPEC", default="1", help="Specification of nodes to be allocated on machine MACHINE")
@click.option("-q", "--queue", metavar="QUEUE", help="Spawns a worker on an allocated node, which works on the specified queue")
def alloc_interact_cmd(machine, nodes, queue):
    """
    Allocates nodes of NODES_SPEC on MACHINE as an interactive job
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    env_dict = dict(KOCHI_ALLOC_NODE_SPEC=nodes)
    on_login_node_scripts = config.load_env_login_script(machine) + config.alloc_interact_script(machine)
    on_compute_node_scripts = config.load_env_machine_script(machine)
    if queue:
      on_compute_node_scripts.append("kochi work -m {} -q {} -b".format(machine, queue))
    util.run_command_ssh_expect(config.login_host(machine), on_login_node_scripts,
                                on_compute_node_scripts, cwd=config.work_dir(machine), env=env_dict)

# alloc
# -----------------------------------------------------------------------------

AllocArgs = namedtuple("AllocArgs", ["queue", "nodes", "duplicates", "time_limit", "follow", "blocking", "load_env_script", "alloc_script"])

@cli.command(name="alloc")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on. '${nodes}' in the queue name will be substituted with NODES_SPEC")
@click.option("-n", "--nodes", metavar="NODES_SPEC", multiple=True, help="Specification of nodes to be allocated on machine MACHINE")
@click.option("-d", "--duplicates", metavar="DUPLICATES", type=int, default=1, help="Number of workers to be created for each queue")
@click.option("-t", "--time-limit", metavar="TIME_LIMIT", help="Time limit for the system job")
@click.option("-f", "--follow", is_flag=True, default=False, help="Wait for worker allocation and output log as grows")
@click.option("-b", "--blocking", is_flag=True, default=False, help="Block to wait for new job arrival")
def alloc_cmd(machine, queue, nodes, duplicates, time_limit, follow, blocking):
    """
    Allocates nodes of NODES_SPEC on MACHINE as an interactive job
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    if len(nodes) == 0:
        raise click.UsageError("Please specify NODES_SPEC with --nodes (-n).")
    args = AllocArgs(queue, list(nodes), duplicates, time_limit, follow, blocking,
                     config.load_env_machine_script(machine), config.alloc_script(machine))
    run_on_login_node(machine, "kochi alloc_aux -m {} {}".format(machine, util.serialize(args)))

@cli.command(name="alloc_aux", hidden=True)
@machine_option
@click.argument("args_serialized", required=True)
def alloc_aux_cmd(machine, args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    worker_ids = []
    for n in args.nodes:
        q = string.Template(args.queue).substitute(nodes=n)
        for i in range(args.duplicates):
            worker_id = worker.init(machine, q, -1)
            work_cmd = "kochi work -m {} -q {} -i {}".format(machine, q, worker_id)
            if args.blocking:
                work_cmd += " -b"
            cmds = args.load_env_script + [work_cmd]
            env = os.environ.copy()
            env["KOCHI_WORKER_LAUNCH_CMD"] = "\n".join(cmds)
            env["KOCHI_ALLOC_NODE_SPEC"] = n
            if args.time_limit:
                env["KOCHI_ALLOC_TIME_LIMIT"] = args.time_limit
            try:
                subprocess.run("\n".join(args.alloc_script), env=env, shell=True, executable="/bin/bash", check=True)
            except subprocess.CalledProcessError:
                click.secho("Submission of a system job for worker {} failed on machine {}.".format(worker_id, machine), fg="red", file=sys.stderr)
                exit(1)
            click.secho("Worker {} for queue '{}' was requested on machine '{}'.".format(worker_id, q, machine), fg="green")
            worker_ids.append(worker_id)
    if args.follow:
        worker.watch(machine, worker_ids)

# enqueue
# -----------------------------------------------------------------------------

dependency_option = click.option("-d", "--dependency", metavar="NAME:RECIPE", multiple=True, help="Project dependencies specified in the config")

def parse_dependencies(dep_strs):
    deps = collections.OrderedDict()
    for d in dep_strs:
        ds = d.split(":")
        if len(ds) != 2:
            raise click.UsageError("Dependency must be specified as '--dependency (-d) NAME:RECIPE'")
        deps[ds[0]] = ds[1]
    return deps

def get_dependencies_recursively_aux(deps, machine, deps_acc):
    for d, r in deps.items():
        if d in deps_acc and deps_acc[d] != r:
            print("Dependency conflict: {0}:{1} vs {0}:{2}".format(d, r, deps_acc[d]), file=sys.stderr)
            exit(1)
        if not d in deps_acc:
            get_dependencies_recursively_aux(config.recipe_dependencies(d, r, machine), machine, deps_acc)
            deps_acc[d] = r

def get_dependencies_recursively(deps, machine):
    deps_acc = collections.OrderedDict()
    get_dependencies_recursively_aux(deps, machine, deps_acc)
    return deps_acc

def create_job(machine, queue, with_context, dependency, name, git_remote, commands):
    if len(commands) > 0 and pathlib.Path(commands[0]).suffix in [".yaml", ".yml"]:
        with_context = True
        build_conf = job_config.build(commands[0])
        run_conf = job_config.run(commands[0])
        deps = job_config.default_dependencies(commands[0], machine)
        deps.update(parse_dependencies(dependency))
        params = util.param_substitute(job_manager.parse_params(commands, machine))
        if not name:
            name = string.Template(job_config.default_name(commands[0], machine)).substitute(params)
        if not queue:
            queue = string.Template(job_config.default_queue(commands[0], machine)).substitute(params)
    else:
        deps = parse_dependencies(dependency)
        params = dict()
        build_conf = dict()
        run_conf = dict(script=[subprocess.list2cmdline(list(commands))] if len(commands) > 0 else [])

    if not name:
        name = "ANON"
    if not queue:
        raise click.UsageError("Please specify --queue (-q) option.")

    if with_context and not util.is_inside_git_dir():
        raise click.UsageError("--with-context (-c) option must be used inside a git directory.")
    if with_context and not git_remote:
        project.sync(machine)
    ctx = context.create(git_remote) if with_context else None

    rec_deps = get_dependencies_recursively(deps, machine)
    activate_script = sum([config.recipe_activate_script(d, r) for d, r in rec_deps.items()], [])

    return job_queue.Job(name, machine, queue, rec_deps, ctx, params,
                         [], activate_script, build_conf, run_conf)

@cli.command(name="enqueue", context_settings=dict(ignore_unknown_options=True))
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", help="Queue to enqueue a job")
@click.option("-c", "--with-context", is_flag=True, default=False, help="Whether to create context of the current git repository")
@dependency_option
@click.option("-n", "--name", metavar="JOB_NAME", help="Job name")
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def enqueue_cmd(click_ctx, machine, queue, with_context, dependency, name, git_remote, commands):
    """
    Enqueues a job that runs COMMANDS to QUEUE on MACHINE.

    COMMANDS := <commandline shell script>
              | job_config.yaml param1=value1 param2=value2 ...
    """
    job = create_job(machine, queue, with_context, dependency, name, git_remote, commands)
    if machine == "local":
        click_ctx.invoke(enqueue_aux_cmd, machine=machine, job_serialized=util.serialize(job))
    else:
        run_on_login_node(machine, "kochi enqueue_aux -m {} {}".format(machine, util.serialize(job)))

@cli.command(name="enqueue_aux", hidden=True)
@machine_option
@click.argument("job_serialized", required=True)
def enqueue_aux_cmd(machine, job_serialized):
    """
    For internal use only.
    """
    job = util.deserialize(job_serialized)
    job_enqueued = job_queue.push(job)
    click.secho("Job {} was submitted to queue '{}' on machine '{}'.".format(job_enqueued.id, job.queue, machine), fg="blue")

# interact
# -----------------------------------------------------------------------------

InteractArgs = namedtuple("InteractArgs", ["job", "forward_remote_port", "forward_target_port"])

@cli.command(name="interact")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to enqueue a job")
@click.option("-c", "--with-context", is_flag=True, default=False, help="Whether to create context of the current git repository")
@dependency_option
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.option("-p", "--forward-port", type=int, default=8080, help="Remote port to be forwarded from local via ssh. $KOCHI_FORWARD_PORT env is set.")
@click.argument("commands", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def interact_cmd(click_ctx, machine, queue, with_context, dependency, git_remote, forward_port, commands):
    """
    Enqueues a job to launch an interactive shell on a worker.
    COMMANDS are automatically executed. See also `enqueue` command.
    """
    job = create_job(machine, queue, with_context, dependency, "interact", git_remote, commands)
    if machine == "local":
        args = InteractArgs(job, None, None)
        click_ctx.invoke(interact_aux_cmd, machine=machine, args_serialized=util.serialize(args))
    else:
        with ssh_forward.reverse_forward(config.login_host(machine)) as remote_port:
            args = InteractArgs(job, remote_port, forward_port)
            run_on_login_node(machine, "kochi interact_aux -m {} {}".format(machine, util.serialize(args)))

@cli.command(name="interact_aux", hidden=True)
@machine_option
@click.argument("args_serialized", required=True)
def interact_aux_cmd(machine, args_serialized):
    """
    For internal use only.
    """
    args = util.deserialize(args_serialized)
    job = args.job
    def on_listen(host, port, token):
        commands = []
        ip_address_candidates = ["127.0.0.1"] if machine == "local" else util.get_ip_address_candidates()
        for ip in ip_address_candidates:
            commands.append("timeout 0.3 nc -z {0} {1} &&" \
                            "echo 'Connecting to {0}:{1} to forward the control of an interactive shell...' &&" \
                            "kochi launch_reverse_shell {0} {1} {2} &&" \
                            "echo 'Connection lost.' &&" \
                            "exit 0".format(ip, port, token))
        activate_script = job.activate_script + ["export KOCHI_FORWARD_PORT={}".format(args.forward_target_port)]
        job_interact = job_queue.Job(job.name, job.machine, job.queue, job.dependencies, job.context, job.params,
                                     job.artifacts_conf, activate_script, job.build_conf, dict(job.run_conf, script=commands))
        job_enqueued = job_queue.push(job_interact)
        click.secho("Job {} submitted on machine {} (listening on {}:{}).".format(job_enqueued.id, machine, host, port), fg="blue")
    def on_accept(remote_host, remote_port):
        if args.forward_target_port:
            ssh_forward.invoke_reverse_forward(args.forward_remote_port, remote_host, args.forward_target_port)
    reverse_shell.wait_to_connect("0.0.0.0", 0, on_listen_hook=on_listen, on_accept_hook=on_accept, script=job.run_conf.get("script", []))

@cli.command(name="launch_reverse_shell", hidden=True)
@click.argument("host", required=True)
@click.argument("port", type=int, required=True)
@click.argument("token", required=True)
def launch_reverse_shell_cmd(host, port, token):
    """
    For internal use only.
    """
    reverse_shell.launch_shell(host, port, token)

# batch
# -----------------------------------------------------------------------------

@cli.command(name="batch")
@machine_option
@click.argument("job_config_file", required=True)
@click.argument("batch_name", required=True)
@click.option("-g", "--git-remote", help="URL or path to remote git repository. By default, a remote repository is created on the remote machine via ssh.")
@click.pass_context
def batch_cmd(click_ctx, machine, job_config_file, batch_name, git_remote):
    """
    Enqueues jobs specified as BATCH_NAME in JOB_CONFIG_FILE on MACHINE.
    """
    build_conf = job_config.batch_build(job_config_file, batch_name, machine)
    run_conf = job_config.batch_run(job_config_file, batch_name, machine)

    job_name_template = string.Template(job_config.batch_job_name(job_config_file, batch_name, machine))
    queue_name_template = string.Template(job_config.batch_queue(job_config_file, batch_name, machine))
    duplicates = job_config.batch_duplicates(job_config_file, batch_name, machine)
    params = job_config.batch_params(job_config_file, batch_name, machine)
    params.update(batch_name=batch_name)
    artifacts_conf = job_config.batch_artifacts(job_config_file, batch_name, machine)

    deps = job_config.batch_dependencies(job_config_file, batch_name, machine)
    rec_deps = get_dependencies_recursively(deps, machine)
    activate_script = sum([config.recipe_activate_script(d, r) for d, r in rec_deps.items()], [])

    artifact.ensure_init_machine(machine)

    if not util.is_inside_git_dir():
        raise click.UsageError("--with-context (-c) option must be used inside a git directory.")
    if not git_remote:
        project.sync(machine)
    ctx = context.create(git_remote)

    for p in util.param_product(params):
        p = util.param_substitute(p)
        for dup in range(duplicates):
            p.update(duplicate=dup)
            queue = queue_name_template.substitute(p)
            job_name = job_name_template.substitute(p)
            job = job_queue.Job(job_name, machine, queue, rec_deps, ctx, p,
                                artifacts_conf, activate_script, build_conf, run_conf)
            if machine == "local":
                click_ctx.invoke(enqueue_aux_cmd, machine=machine, job_serialized=util.serialize(job))
            else:
                run_on_login_node(machine, "kochi enqueue_aux -m {} {}".format(machine, util.serialize(job)))

# cacnel
# -----------------------------------------------------------------------------

@on_machine_cmd(cli, "cancel")
@click.option("-a", "--all", is_flag=True, default=False, help="Cancel all jobs.")
@click.argument("job_ids", type=int, nargs=-1)
def cancel_cmd(machine, all, job_ids):
    """
    Cancel jobs of JOB_IDS on MACHINE.
    """
    if all:
        job_states = stats.get_all_active_job_states(machine)
    else:
        job_states = []
        for job_id in job_ids:
            state = job_manager.get_state(machine, job_id)
            job_states.append((job_id, state))
    if job_states:
        for job_id, state in job_states:
            if state.running_state == job_manager.RunningState.WAITING or \
               state.running_state == job_manager.RunningState.RUNNING:
                job_manager.cancel(machine, job_id)
                print("Job {} was canceled.".format(job_id))
            else:
                print("Job {} has already finished (state: {}).".format(job_id, str(state.running_state)))
    else:
        print("No job was canceled.")

# inspect
# -----------------------------------------------------------------------------

@cli.command(name="inspect")
@machine_option
@on_machine_option
@click.argument("worker_id", type=int, required=True)
def inspect_cmd(machine, on_machine, worker_id):
    """
    Inspect worker of WORKER_ID by connecting to MACHINE where the worker is running.
    """
    if machine == "local":
        raise click.UsageError("MACHINE cannot be 'local'.")
    if on_machine:
        sshd.login_to_machine(machine, worker_id)
    else:
        run_on_login_node(machine, "kochi inspect -m {} --on-machine {}".format(machine, worker_id))

# run
# -----------------------------------------------------------------------------

@cli.command(name="run")
@machine_option
@dependency_option
@click.argument("commands", required=True, nargs=-1, type=click.UNPROCESSED)
def run_cmd(machine, dependency, commands):
    """
    Run COMMANDS here (within this directory) with specified dependencies.
    Mainly for generating compilation artifacts such as compile_commands.json.
    """
    project_name = project.project_name_of_cwd()
    deps = get_dependencies_recursively(parse_dependencies(dependency), machine)
    activate_script = sum([config.recipe_activate_script(d, r) for d, r in deps.items()], [])
    dep_envs = installer.deps_env(project_name, machine, deps)
    scripts = activate_script + [subprocess.list2cmdline(list(commands))]
    env = os.environ.copy()
    env["KOCHI_MACHINE"] = machine
    env.update(dep_envs)
    subprocess.run("\n".join(scripts), env=env, shell=True, executable="/bin/bash")

# work
# -----------------------------------------------------------------------------

@cli.command(name="work")
@machine_option
@click.option("-q", "--queue", metavar="QUEUE", required=True, help="Queue to work on")
@click.option("-b", "--blocking", is_flag=True, default=False, help="Whether to block to wait for job arrival")
@click.option("-i", "--worker-id", type=int, default=-1, hidden=True, help="For internal use only")
def work_cmd(machine, queue, blocking, worker_id):
    """
    Start a new worker that works on QUEUE.
    Assume that this command is invoked on MACHINE.
    """
    worker_id = worker.init(machine, queue, worker_id) if worker_id == -1 else worker_id
    worker.start(queue, blocking, worker_id, machine)

# install
# -----------------------------------------------------------------------------

@cli.command(name="install")
@machine_option
@dependency_option
@click.option("-q", "--queue", metavar="QUEUE", help="Queue to which installation jobs are submitted (only when on_machine = true)")
@click.pass_context
def install_cmd(click_ctx, machine, dependency, queue):
    """
    Install projects that are depended on by this repository on MACHINE.
    """
    project_name = project.project_name_of_cwd()
    for dep, recipe in parse_dependencies(dependency).items():
        ctx = installer.get_install_context(machine, dep, recipe)
        recipe_deps = config.recipe_dependencies(dep, recipe, machine)
        on_machine = config.recipe_on_machine(dep, recipe)
        rec_deps = get_dependencies_recursively(recipe_deps, machine)
        activate_script = sum([config.recipe_activate_script(d, r) for d, r in rec_deps.items()], [])
        args = installer.InstallConf(project_name, dep, recipe, on_machine, rec_deps, ctx,
                                     config.recipe_envs(dep, recipe), activate_script, config.recipe_script(dep, recipe))
        if machine == "local":
            click_ctx.invoke(install_aux_cmd, machine=machine, args_serialized=util.serialize(args))
        else:
            install_cmd = "kochi install_aux -m {} {}".format(machine, util.serialize(args))
            if on_machine:
                if not queue:
                    raise click.UsageError("Please specify --queue (-q) option to install {}:{} (on_machine = true)".format(dep, recipe))
                job = job_queue.Job("KOCHI-INSTALL", machine, queue, rec_deps, ctx, dict(),
                                    [], activate_script, dict(), dict(script=[install_cmd]))
                run_on_login_node(machine, "kochi enqueue_aux -m {} {}".format(machine, util.serialize(job)))
            else:
                run_on_login_node(machine, install_cmd)

@cli.command(name="install_aux", hidden=True)
@machine_option
@click.argument("args_serialized", required=True)
def install_aux_cmd(machine, args_serialized):
    """
    For internal use only.
    """
    installer.install(util.deserialize(args_serialized), machine)

# artifact
# -----------------------------------------------------------------------------

@cli.group(name="artifact")
def artifact_group():
    pass

@artifact_group.command(name="init")
@click.argument("git_worktree_path", required=True)
def artifact_init_cmd(git_worktree_path):
    """
    Creates a git worktree at GIT_WORKTREE_PATH that operates on kochi's artifacts branch.
    """
    artifact.init(git_worktree_path)

@artifact_group.command(name="sync")
@machine_option
def artifact_sync_cmd(machine):
    """
    Gets (pulls) job artifacts from MACHINE and saves them in the artifact worktree.
    """
    artifact.sync(machine)

@artifact_group.command(name="discard")
@machine_option
def artifact_discard_cmd(machine):
    """
    Deletes an artifact branch for MACHINE and removes all artifacts stored on MACHINE.
    """
    artifact.discard(machine)

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

@on_machine_cmd(show, "job")
@click.argument("job_id", required=True, type=int)
def show_job_cmd(machine, job_id):
    stats.show_job_detail(machine, job_id)

@on_machine_cmd(show, "installs")
@click.option("--project", hidden=True, callback=lambda _c, _p, v: project.project_name_of_cwd() if not v else v)
@click.option("--recipes", hidden=True, callback=lambda _c, _p, v: project.get_all_recipes() if not v else v)
def show_installs_cmd(machine, project, recipes):
    stats.show_installs(machine, project, recipes);

@on_machine_cmd(show, "install")
@click.option("--project", hidden=True, callback=lambda _c, _p, v: project.project_name_of_cwd() if not v else v)
@dependency_option
def show_install_cmd(machine, project, dependency):
    for d, r in parse_dependencies(dependency).items():
        stats.show_install_detail(machine, project, d, r)

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
    Show a log file of worker WORKER_ID on MACHINE.
    """
    with open(settings.worker_log_filepath(machine, worker_id)) as f:
        click.echo_via_pager(f)

@on_machine_cmd(log, "job")
@click.argument("job_id", required=True, type=int)
def show_log_job_cmd(machine, job_id):
    """
    Show a log file of job JOB_ID on MACHINE.
    """
    with open(settings.job_log_filepath(machine, job_id)) as f:
        click.echo_via_pager(f)

@on_machine_cmd(log, "install")
@click.option("--project", hidden=True, callback=lambda _c, _p, v: project.project_name_of_cwd() if not v else v)
@dependency_option
def show_log_install_cmd(machine, project, dependency):
    """
    Show log files of installation of specified dependency recipes.
    """
    for d, r in parse_dependencies(dependency).items():
        with open(settings.project_dep_install_log_filepath(project, machine, d, r)) as f:
            click.echo_via_pager(f)

# show path
# -----------------------------------------------------------------------------

@show.group()
def path():
    pass

@on_machine_cmd(path, "project")
@click.option("-f", "--force", is_flag=True, default=False, help="Force to show the project path even if the project does not exist")
@click.option("--is-artifact", is_flag=True, default=False, help="Get an artifact git path")
@click.argument("project_name", required=True, type=str)
def show_path_project_cmd(machine, force, project_name, is_artifact):
    """
    Show a path to PROJECT_NAME on MACHINE.
    """
    project_path = settings.project_artifact_git_dirpath(project_name) if is_artifact else settings.project_git_dirpath(project_name)
    if force or os.path.isdir(project_path):
        print(project_path)
    else:
        print("Project '{}' does not exist.".format(project_name), file=sys.stderr)
        sys.exit(1)
