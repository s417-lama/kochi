from collections import namedtuple
import os
import time
import copy
import click

from . import util
from . import settings
from . import job_manager
from . import job_queue
from . import job_canceler
from . import atomic_counter
from . import context
from . import sshd
from . import heartbeat

RunningState = heartbeat.RunningState
state_fields = ["running_state", "queue", "init_time", "start_time", "latest_time"]
State = namedtuple("State", state_fields)

def get_worker_id(machine):
    idx = atomic_counter.fetch_and_add(settings.worker_counter_filepath(machine), 1)
    return idx

def worker_loop(idx, queue_name, blocking, machine, stdout):
    prev_job_build_state = dict()
    while True:
        job = job_queue.pop(machine, queue_name)
        if job:
            if not job_canceler.check_canceled(machine, job.id):
                build_state = job_manager.build_state(job, machine)
                exec_build = build_state != prev_job_build_state
                build_success = job_manager.run_job(job, idx, machine, queue_name, exec_build, stdout)
                if build_success:
                    prev_job_build_state = build_state
        elif blocking:
            time.sleep(0.1)
        else:
            return

def start(queue_name, blocking, worker_id, machine):
    with util.tmpdir(settings.worker_workspace_dirpath(machine, worker_id)):
        with heartbeat.heartbeat(settings.worker_heartbeat_filepath(machine, worker_id)):
            with sshd.sshd(machine, worker_id):
                with util.tee(settings.worker_log_filepath(machine, worker_id)) as tee:
                    color = "green"
                    print(click.style("Kochi worker {} started on machine {}.".format(worker_id, machine), fg=color), file=tee.stdin, flush=True)
                    print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)
                    try:
                        worker_loop(worker_id, queue_name, blocking, machine, tee.stdin)
                    except KeyboardInterrupt:
                        print(click.style("Kochi worker {} interrupted.".format(worker_id), fg="red"), file=tee.stdin, flush=True)
                    except BaseException as e:
                        print(click.style("Kochi worker {} failed: {}".format(worker_id, str(e)), fg="red"), file=tee.stdin, flush=True)
                    print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)

def get_state(machine, worker_id):
    try:
        with open(settings.worker_state_filepath(machine, worker_id), "r") as f:
            queue = f.read().strip()
        hb_state = heartbeat.get_state(settings.worker_heartbeat_filepath(machine, worker_id))
        return State(hb_state.running_state, queue, hb_state.init_time, hb_state.start_time, hb_state.latest_time)
    except:
        return State(RunningState.INVALID, None, None, None, None)

def watch(machine, worker_ids):
    if len(worker_ids) == 0:
        return
    log_files = [settings.worker_log_filepath(machine, w) for w in worker_ids]
    remains = copy.copy(worker_ids)
    with util.tailf(log_files):
        while True:
            for w in remains:
                state = get_state(machine, w).running_state
                if state == RunningState.TERMINATED or state == RunningState.INVALID:
                    remains.remove(w)
            if len(remains) == 0:
                break
            time.sleep(1)

def init(machine, queue, worker_id):
    worker_id = get_worker_id(machine) if worker_id == -1 else worker_id
    with open(settings.worker_state_filepath(machine, worker_id), "w") as f:
        f.write(queue)
    with open(settings.worker_log_filepath(machine, worker_id), "w") as f:
        f.write("")
    heartbeat.init(settings.worker_heartbeat_filepath(machine, worker_id))
    return worker_id

def ensure_init(machine):
    os.makedirs(settings.worker_dirpath(machine), exist_ok=True)
    try:
        atomic_counter.fetch(settings.worker_counter_filepath(machine))
    except:
        atomic_counter.reset(settings.worker_counter_filepath(machine))
    try:
        atomic_counter.fetch(settings.worker_min_active_filepath(machine))
    except:
        atomic_counter.reset(settings.worker_min_active_filepath(machine))

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
    worker_id = init("local", queue_name, -1)
    start(queue_name, False, worker_id, "local")
