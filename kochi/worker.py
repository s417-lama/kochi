import os
import subprocess
import time
import click

from . import util
from . import settings
from . import job_queue
from . import atomic_counter
from . import context
from . import sshd
from . import heartbeat

def get_worker_id(machine):
    idx = atomic_counter.fetch_and_add(settings.worker_counter_filepath(machine), 1)
    heartbeat.init(machine, idx)
    return idx

def run_job(job, machine, stdout):
    color = "blue"
    print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=stdout, flush=True)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)
    with context.context(job.context):
        with subprocess.Popen(["tee", settings.job_log_filepath(machine, job.id)], stdin=subprocess.PIPE, stdout=stdout, encoding="utf-8", start_new_session=True) as tee:
            env = os.environ.copy()
            env["KOCHI_JOB_ID"] = str(job.id)
            env["KOCHI_JOB_NAME"] = job.name
            for dep, recipe in job.dependencies:
                env["KOCHI_DEP_" + dep.upper()] = settings.project_dep_install_dirpath(job.context.project, machine, dep, recipe)
            try:
                subprocess.run(job.commands, env=env, shell=not isinstance(job.commands, list), stdout=tee.stdin, stderr=tee.stdin, check=True)
            except KeyboardInterrupt:
                print(click.style("Kochi job {} (ID={}) interrupted.".format(job.name, job.id), fg="red"), file=tee.stdin, flush=True)
            except BaseException as e:
                print(click.style("Kochi job {} (ID={}) failed: {}".format(job.name, job.id, str(e)), fg="red"), file=tee.stdin, flush=True)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)

def worker_loop(queue_name, blocking, machine, stdout):
    while True:
        job = job_queue.pop(machine, queue_name)
        if job:
            run_job(job, machine, stdout)
        elif blocking:
            time.sleep(0.1) # TODO: monitor filesystem events?
        else:
            return

def start(queue_name, blocking, worker_id, machine):
    idx = get_worker_id(machine) if worker_id == -1 else worker_id
    workspace = settings.worker_workspace_dirpath(machine, idx)
    with util.tmpdir(workspace):
        with heartbeat.heartbeat(machine, idx):
            with sshd.sshd(machine, idx):
                with subprocess.Popen(["tee", settings.worker_log_filepath(machine, idx)], stdin=subprocess.PIPE, encoding="utf-8", start_new_session=True) as tee:
                    color = "green"
                    print(click.style("Kochi worker {} started on machine {}.".format(idx, machine), fg=color), file=tee.stdin, flush=True)
                    print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)
                    try:
                        worker_loop(queue_name, blocking, machine, tee.stdin)
                    except KeyboardInterrupt:
                        print(click.style("Kochi worker {} interrupted.".format(idx), fg="red"), file=tee.stdin, flush=True)
                    except BaseException as e:
                        print(click.style("Kochi worker {} failed: {}".format(idx, str(e)), fg="red"), file=tee.stdin, flush=True)
                    print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)

RunningState = heartbeat.RunningState
State = heartbeat.State

def get_state(machine, worker_id):
    return heartbeat.get_state(machine, worker_id)

if __name__ == "__main__":
    """
    machine1$ python3 -m kochi.worker
    machine2$ python3 -m kochi.worker
    """
    import pathlib
    queue_name = "test"
    test_filename = "test.txt"
    repo_path = pathlib.Path(__file__).parent.parent.absolute()
    print(repo_path)
    with util.cwd(repo_path):
        for i in range(100):
            with open(test_filename, "w+") as f:
                print("File for job {}".format(i), file=f)
            ctx = context.create(repo_path)
            job_queue.push(job_queue.Job("test_job_{}".format(i), "local", queue_name, [], ctx, "sleep 0.1; cat {}".format(test_filename)))
    os.remove(test_filename)
    start(queue_name, False, -1, "local")
