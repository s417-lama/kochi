from collections import namedtuple
import os
import subprocess
import enum
import time
import click

from . import util
from . import settings
from . import context
from . import heartbeat
from . import installer
from . import atomic_counter

class RunningState(enum.IntEnum):
    def __str__(self):
        if self.value == self.WAITING:
            return "waiting"
        elif self.value == self.RUNNING:
            return "running"
        elif self.value == self.TERMINATED:
            return "terminated"
        elif self.value == self.ABORTED:
            return "aborted"
        elif self.value == self.KILLED:
            return "killed"
        else:
            return "invalid"
    INVALID = 0
    WAITING = 1
    RUNNING = 2
    TERMINATED = 3
    ABORTED = 4
    KILLED = 5

state_fields = ["running_state", "name", "queue", "worker_id", "context", "dependency_states", "activate_script", "script", "init_time", "start_time", "latest_time"]
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

def on_finish_job(job, worker_id, machine, running_state):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        state = util.deserialize(f.read())
        next_state = update_state(state, running_state=running_state, latest_time=current_timestamp())
        f.seek(0)
        f.write(util.serialize(next_state))
        f.truncate()

def run_job(job, worker_id, machine, queue_name, stdout):
    dep_envs = installer.check_dependencies(job.context.project, machine, job.dependencies)
    with context.context(job.context):
        with util.tee(settings.job_log_filepath(machine, job.id), stdout=stdout) as tee:
            color = "blue"
            print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=tee.stdin, flush=True)
            print(click.style("-" * 80, fg=color), file=tee.stdin, flush=True)
            on_start_job(job, worker_id, machine)
            env = os.environ.copy()
            env["KOCHI_MACHINE"] = machine
            env["KOCHI_WORKER_ID"] = str(worker_id)
            env["KOCHI_QUEUE"] = queue_name
            env["KOCHI_JOB_ID"] = str(job.id)
            env["KOCHI_JOB_NAME"] = job.name
            env.update(dep_envs)
            try:
                subprocess.run("\n".join(job.activate_script + job.script), env=env, shell=True, stdout=tee.stdin, stderr=tee.stdin, check=True)
            except KeyboardInterrupt:
                print(click.style("Kochi job {} (ID={}) interrupted.".format(job.name, job.id), fg="red"), file=tee.stdin, flush=True)
                on_finish_job(job, worker_id, machine, RunningState.ABORTED)
            except BaseException as e:
                print(click.style("Kochi job {} (ID={}) failed: {}".format(job.name, job.id, str(e)), fg="red"), file=tee.stdin, flush=True)
                on_finish_job(job, worker_id, machine, RunningState.ABORTED)
            else:
                on_finish_job(job, worker_id, machine, RunningState.TERMINATED)
            print(click.style("-" * 80, fg=color), file=tee.stdin, flush=True)

def get_state(machine, job_id):
    try:
        with open(settings.job_state_filepath(machine, job_id), "r") as f:
            state = util.deserialize(f.read())
        if state.running_state == RunningState.RUNNING:
            worker_state = heartbeat.get_state(settings.worker_heartbeat_filepath(machine, state.worker_id))
            if worker_state.running_state == heartbeat.RunningState.RUNNING:
                return update_state(state, latest_time=current_timestamp())
            else:
                return update_state(state, running_state=RunningState.KILLED, latest_time=worker_state.latest_time)
        else:
            return state
    except:
        return State(RunningState.INVALID, None, None, None, None, None, None, None, None, None)

def init(job, machine, queue_name):
    with open(settings.job_state_filepath(machine, job.id), "w") as f:
        dependency_states = [installer.get_state(job.context.project, d, r, machine) for d, r in job.dependencies]
        state = State(RunningState.WAITING, job.name, queue_name, None, job.context, dependency_states, job.activate_script, job.script, current_timestamp(), None, None)
        f.write(util.serialize(state))

def ensure_init(machine):
    os.makedirs(settings.job_dirpath(machine), exist_ok=True)
    try:
        atomic_counter.fetch(settings.job_counter_filepath(machine))
    except:
        atomic_counter.reset(settings.job_counter_filepath(machine))
