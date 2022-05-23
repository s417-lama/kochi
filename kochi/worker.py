from collections import namedtuple
import os
import subprocess
import time
import click

from . import util
from . import settings
from . import job_manager
from . import job_queue
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
    while True:
        job = job_queue.pop(machine, queue_name)
        if job:
            job_manager.run_job(job, idx, machine, stdout)
        elif blocking:
            time.sleep(0.1) # TODO: monitor filesystem events?
        else:
            return

def start(queue_name, blocking, worker_id, machine):
    with util.tmpdir(settings.worker_workspace_dirpath(machine, worker_id)):
        with heartbeat.heartbeat(settings.worker_heartbeat_filepath(machine, worker_id)):
            with sshd.sshd(machine, worker_id):
                with subprocess.Popen(["tee", settings.worker_log_filepath(machine, worker_id)], stdin=subprocess.PIPE, encoding="utf-8", start_new_session=True) as tee:
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

def init(machine, queue, worker_id):
    worker_id = get_worker_id(machine) if worker_id == -1 else worker_id
    with open(settings.worker_state_filepath(machine, worker_id), "w") as f:
        f.write(queue)
    heartbeat.init(settings.worker_heartbeat_filepath(machine, worker_id))
    return worker_id

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
    init("local", queue_name)
    start(queue_name, False, -1, "local")
