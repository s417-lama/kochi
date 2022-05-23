from collections import namedtuple
import os
import subprocess
import time
import click

from . import util
from . import settings
from . import context
from . import heartbeat

RunningState = heartbeat.RunningState
state_fields = ["running_state", "name", "queue", "worker_id", "init_time", "start_time", "latest_time"]
State = namedtuple("State", state_fields)

def update_state(state, **kwargs):
    d = dict()
    for i, f in enumerate(state_fields):
        d[f] = kwargs.get(f, state[i])
    return State(**d)

def current_timestamp():
    return int(time.time())

def on_start_job(job, worker_id, machine):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        state = util.deserialize(f.read())
        next_state = update_state(state, running_state=RunningState.RUNNING, worker_id=worker_id, start_time=current_timestamp())
        f.seek(0)
        f.write(util.serialize(next_state))
        f.truncate()

def on_finish_job(job, worker_id, machine):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        state = util.deserialize(f.read())
        next_state = update_state(state, running_state=RunningState.TERMINATED, latest_time=current_timestamp())
        f.seek(0)
        f.write(util.serialize(next_state))
        f.truncate()

def run_job(job, worker_id, machine, queue_name, stdout):
    color = "blue"
    print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=stdout, flush=True)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)
    with context.context(job.context):
        with subprocess.Popen(["tee", settings.job_log_filepath(machine, job.id)], stdin=subprocess.PIPE, stdout=stdout, encoding="utf-8", start_new_session=True) as tee:
            on_start_job(job, worker_id, machine)
            env = os.environ.copy()
            env["KOCHI_MACHINE"] = machine
            env["KOCHI_WORKER_ID"] = str(worker_id)
            env["KOCHI_QUEUE"] = queue_name
            env["KOCHI_JOB_ID"] = str(job.id)
            env["KOCHI_JOB_NAME"] = job.name
            for dep, recipe in job.dependencies:
                env["KOCHI_INSTALL_PREFIX_" + dep.upper()] = settings.project_dep_install_dirpath(job.context.project, machine, dep, recipe)
                env["KOCHI_RECIPE_" + dep.upper()] = recipe
            try:
                subprocess.run(job.commands, env=env, shell=not isinstance(job.commands, list), stdout=tee.stdin, stderr=tee.stdin, check=True)
            except KeyboardInterrupt:
                print(click.style("Kochi job {} (ID={}) interrupted.".format(job.name, job.id), fg="red"), file=tee.stdin, flush=True)
            except BaseException as e:
                print(click.style("Kochi job {} (ID={}) failed: {}".format(job.name, job.id, str(e)), fg="red"), file=tee.stdin, flush=True)
            finally:
                on_finish_job(job, worker_id, machine)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)

def get_state(machine, job_id):
    try:
        with open(settings.job_state_filepath(machine, job_id), "r") as f:
            state = util.deserialize(f.read())
        if state.running_state == RunningState.RUNNING:
            worker_state = heartbeat.get_state(settings.worker_heartbeat_filepath(machine, state.worker_id))
            latest_time = current_timestamp() if worker_state.running_state == heartbeat.RunningState.RUNNING else worker_state.latest_time
            return update_state(state, running_state=worker_state.running_state, latest_time=latest_time)
        else:
            return state
    except:
        return State(RunningState.INVALID, None, None, None, None, None, None)

def init(job, machine, queue_name):
    with open(settings.job_state_filepath(machine, job.id), "w") as f:
        state = State(RunningState.WAITING, job.name, queue_name, None, current_timestamp(), None, None)
        f.write(util.serialize(state))
