from collections import namedtuple
import sys
import time
import multiprocessing
import contextlib
import enum

from . import settings

class RunningState(enum.IntEnum):
    def __str__(self):
        if self.value == self.WAITING:
            return "waiting"
        elif self.value == self.RUNNING:
            return "running"
        elif self.value == self.TERMINATED:
            return "terminated"
        else:
            return "invalid"
    INVALID = 0
    WAITING = 1
    RUNNING = 2
    TERMINATED = 3

State = namedtuple("State", ["running_state", "init_time", "start_time", "latest_time"])

def current_timestamp():
    return int(time.time())

def update_timestamp(filename, **opts):
    timestamp = current_timestamp()
    with open(filename, "r+") as f:
        states = f.read().split()
        init_time = int(states[1])
        if states[0] == str(RunningState.WAITING):
            start_time = timestamp
        else:
            start_time = int(states[2])
        f.seek(0)
        f.write("{} {} {} {}".format(str(opts.get("state", RunningState.RUNNING)), str(init_time), str(start_time), str(timestamp)))
        f.truncate()

def daemon(queue, filename, interval):
    try:
        while True:
            update_timestamp(filename)
            if queue.empty():
                time.sleep(interval)
            elif queue.get_nowait() == "terminate":
                update_timestamp(filename, state=RunningState.TERMINATED)
                return
            else:
                print("Something is wrong in heartbeat.", file=sys.stderr)
                exit(1)
    except KeyboardInterrupt:
        update_timestamp(filename, state=RunningState.TERMINATED)
        return

@contextlib.contextmanager
def heartbeat(machine, worker_id, **opts):
    filename = settings.worker_heartbeat_filepath(machine, worker_id)
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=daemon, args=(q, filename, opts.get("interval", 3)))
    p.start()
    try:
        yield
    finally:
        q.put("terminate")
        p.join()

def get_state(machine, worker_id, **opts):
    filename = settings.worker_heartbeat_filepath(machine, worker_id)
    try:
        with open(filename, "r") as f:
            states = f.read().split()
        if states[0] == str(RunningState.WAITING):
            return State(RunningState.WAITING, int(states[1]), None, None)
        elif states[0] == str(RunningState.RUNNING):
            timestamp = int(states[3])
            if timestamp + opts.get("margin", 5) < current_timestamp():
                return State(RunningState.TERMINATED, int(states[1]), int(states[2]), timestamp)
            else:
                return State(RunningState.RUNNING, int(states[1]), int(states[2]), timestamp)
        elif states[0] == str(RunningState.TERMINATED):
                return State(RunningState.TERMINATED, int(states[1]), int(states[2]), int(states[3]))
    except:
        return State(RunningState.INVALID, None, None, None)

def init(machine, worker_id):
    filename = settings.worker_heartbeat_filepath(machine, worker_id)
    with open(filename, "w") as f:
        f.write("{} {}".format(str(RunningState.WAITING), current_timestamp()))
