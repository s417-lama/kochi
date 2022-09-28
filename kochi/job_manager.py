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
from . import job_config
from . import job_canceler
from . import artifact

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
        elif self.value == self.CANCELED:
            return "canceled"
        elif self.value == self.KILLED:
            return "killed"
        else:
            return "invalid"
    INVALID = 0
    WAITING = 1
    RUNNING = 2
    TERMINATED = 3
    ABORTED = 4
    CANCELED = 5
    KILLED = 6

state_fields = ["running_state", "name", "queue", "worker_id", "context", "dependency_states", "envs",
                "artifacts_conf", "activate_script", "build_executed", "build_params", "build_script",
                "run_params", "run_script", "init_time", "start_time", "latest_time"]
State = namedtuple("State", state_fields)

def update_state(state, **kwargs):
    d = dict()
    for i, f in enumerate(state_fields):
        d[f] = kwargs.get(f, state[i])
    return State(**d)

def current_timestamp():
    return int(time.time())

def on_start_job(job, worker_id, machine, envs, build_executed, build_params, build_script, run_params, run_script):
    with open(settings.job_state_filepath(machine, job.id), "r+") as f:
        state = util.deserialize(f.read())
        next_state = update_state(state, running_state=RunningState.RUNNING, worker_id=worker_id, start_time=current_timestamp(), envs=envs,
                                  build_executed=build_executed, build_params=build_params, build_script=build_script,
                                  run_params=run_params, run_script=run_script)
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

def run_job(job, worker_id, machine, queue_name, exec_build, stdout):
    build_success = False
    dep_envs = installer.deps_env(job.context.project, machine, job.dependencies) if job.context else dict()
    with context.context(job.context):
        with util.tee(settings.job_log_filepath(machine, job.id), stdout=stdout) as tee:
            color = "blue"
            print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=tee.stdin, flush=True)
            print(click.style("-" * 80, fg=color), file=tee.stdin, flush=True)
            env = os.environ.copy()
            env.update(dep_envs)
            env["KOCHI_MACHINE"] = machine
            env["KOCHI_WORKER_ID"] = str(worker_id)
            env["KOCHI_QUEUE"] = queue_name
            env["KOCHI_JOB_ID"] = str(job.id)
            env["KOCHI_JOB_NAME"] = job.name
            # build env
            build_script = job.build_conf.get("script", [])
            build_params = filter_params(job.params, job.build_conf.get("depend_params", []))
            build_env = env.copy()
            build_env.update(params2env(build_params))
            # run env
            run_script = job.run_conf.get("script", [])
            run_params = filter_params(job.params, job.run_conf.get("depend_params", []))
            run_env = env.copy()
            run_env.update(params2env(run_params))
            # save job state
            on_start_job(job, worker_id, machine, env, exec_build, build_params, build_script, run_params, run_script)
            try:
                with job_canceler.job_canceler(machine, job.id):
                    # build
                    if exec_build:
                        subprocess.run("\n".join(job.activate_script + build_script), env=build_env, shell=True, executable="/bin/bash",
                                       stdout=tee.stdin, stderr=tee.stdin, check=True)
                        build_success = True
                    # run
                    subprocess.run("\n".join(job.activate_script + run_script), env=run_env, shell=True, executable="/bin/bash",
                                   stdout=tee.stdin, stderr=tee.stdin, check=True)
                    if job.context and len(job.artifacts_conf) > 0:
                        print(click.style("Saving artifacts...", fg=color), file=tee.stdin, flush=True)
                        artifact.save(machine, worker_id, job)
            except KeyboardInterrupt:
                if job_canceler.check_canceled(machine, job.id):
                    print(click.style("Kochi job {} (ID={}) canceled.".format(job.name, job.id), fg="red"), file=tee.stdin, flush=True)
                    on_finish_job(job, worker_id, machine, RunningState.CANCELED)
                else:
                    print(click.style("Kochi job {} (ID={}) interrupted.".format(job.name, job.id), fg="red"), file=tee.stdin, flush=True)
                    on_finish_job(job, worker_id, machine, RunningState.ABORTED)
            except BaseException as e:
                print(click.style("Kochi job {} (ID={}) failed: {}".format(job.name, job.id, str(e)), fg="red"), file=tee.stdin, flush=True)
                on_finish_job(job, worker_id, machine, RunningState.ABORTED)
            else:
                on_finish_job(job, worker_id, machine, RunningState.TERMINATED)
            print(click.style("-" * 80, fg=color), file=tee.stdin, flush=True)
            return build_success

def cancel(machine, job_id):
    job_canceler.cancel(machine, job_id)

def invalid_state():
    return State(*[RunningState.INVALID if f == "running_state" else None for f in state_fields])

def get_state(machine, job_id):
    try:
        with open(settings.job_state_filepath(machine, job_id), "r") as f:
            state = util.deserialize(f.read())
        if state.running_state == RunningState.WAITING:
            if job_canceler.check_canceled(machine, job_id):
                return update_state(state, running_state=RunningState.CANCELED)
            else:
                return state
        elif state.running_state == RunningState.RUNNING:
            worker_state = heartbeat.get_state(settings.worker_heartbeat_filepath(machine, state.worker_id))
            if worker_state.running_state == heartbeat.RunningState.RUNNING:
                return update_state(state, latest_time=current_timestamp())
            else:
                return update_state(state, running_state=RunningState.KILLED, latest_time=worker_state.latest_time)
        else:
            return state
    except:
        return invalid_state()

def parse_params(commands, machine):
    params = job_config.default_params(commands[0], machine)
    for param in commands[1:]:
        k, v = param.split("=")
        if not k in params:
            print("Warning: parameter '{}' is not specified in 'default_params' in job config file {}.".format(k, commands[0]))
        params[k] = v
    return params

def params2env(params):
    env = dict()
    for k, v in params.items():
        env_name = "KOCHI_PARAM_" + k.upper().replace("-", "_")
        if isinstance(v, bool):
            env[env_name] = "true" if v else "false"
        else:
            env[env_name] = str(v)
    return env

def filter_params(params, param_list):
    return dict(filter(lambda x: x[0] in param_list, params.items()))

def get_dependency_states(job, machine):
    return [installer.get_state(job.context.project, d, r, machine) for d, r in job.dependencies.items()]

def build_state(job, machine):
    dep_states = get_dependency_states(job, machine)
    build_params = filter_params(job.params, job.build_conf.get("depend_params", []))
    return dict(dependency_states=dep_states, context=job.context, params=build_params)

def init(job, machine, queue_name):
    with open(settings.job_state_filepath(machine, job.id), "w") as f:
        dep_states = get_dependency_states(job, machine)
        state = State(RunningState.WAITING, job.name, queue_name, None, job.context, dep_states, None,
                      job.artifacts_conf, job.activate_script, None, None, None, None, None,
                      current_timestamp(), None, None)
        f.write(util.serialize(state))

def ensure_init(machine):
    os.makedirs(settings.job_dirpath(machine), exist_ok=True)
    try:
        atomic_counter.fetch(settings.job_counter_filepath(machine))
    except:
        atomic_counter.reset(settings.job_counter_filepath(machine))
