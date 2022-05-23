from collections import namedtuple
import os
import subprocess
import time
import click

from . import settings
from . import context
from . import heartbeat

RunningState = heartbeat.RunningState
State = namedtuple("State", ["running_state", "worker_id", "init_time", "start_time", "latest_time"])

def current_timestamp():
    return int(time.time())

def on_start_job(job, worker_id, machine):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        init_time = int(f.read().split()[1])
        f.seek(0)
        f.write("{} {} {} {}".format(str(RunningState.RUNNING), worker_id, str(init_time), str(current_timestamp())))
        f.truncate()

def on_finish_job(job, worker_id, machine):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        states = f.read().split()
        init_time = int(states[2])
        start_time = int(states[3])
        f.seek(0)
        f.write("{} {} {} {} {}".format(str(RunningState.TERMINATED), worker_id, str(init_time), str(start_time), str(current_timestamp())))
        f.truncate()

def run_job(job, worker_id, machine, stdout):
    color = "blue"
    print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=stdout, flush=True)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)
    with context.context(job.context):
        with subprocess.Popen(["tee", settings.job_log_filepath(machine, job.id)], stdin=subprocess.PIPE, stdout=stdout, encoding="utf-8", start_new_session=True) as tee:
            on_start_job(job, worker_id, machine)
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
            finally:
                on_finish_job(job, worker_id, machine)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)

def get_state(machine, job_id):
    try:
        with open(settings.job_state_filepath(machine, job_id), "r") as f:
            states = f.read().split()
        if states[0] == str(RunningState.WAITING):
            return State(RunningState.WAITING, None, int(states[1]), None, None)
        elif states[0] == str(RunningState.RUNNING):
            worker_id = int(states[1])
            worker_state = heartbeat.get_state(settings.worker_heartbeat_filepath(machine, worker_id))
            latest_time = current_timestamp() if worker_state.running_state == heartbeat.RunningState.RUNNING else worker_state.latest_time
            return State(worker_state.running_state, worker_id, int(states[2]), int(states[3]), latest_time)
        elif states[0] == str(RunningState.TERMINATED):
            return State(RunningState.TERMINATED, int(states[1]), int(states[2]), int(states[3]), int(states[4]))
    except:
        return State(RunningState.INVALID, None, None, None, None)

def init(machine, job_id):
    with open(settings.job_state_filepath(machine, job_id), "w") as f:
        f.write("{} {}".format(str(RunningState.WAITING), current_timestamp()))
