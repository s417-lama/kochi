from collections import namedtuple
import sys
import time
import queue
import multiprocessing
import contextlib
import enum

from . import util

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

state_fields = ["running_state", "init_time", "start_time", "latest_time"]
State = namedtuple("State", state_fields)

def update_state(state, **kwargs):
    d = dict()
    for i, f in enumerate(state_fields):
        d[f] = kwargs.get(f, state[i])
    return State(**d)

def current_timestamp():
    return int(time.time())

def update_timestamp(filename, **opts):
    timestamp = current_timestamp()
    with open(filename, "r+") as f:
        state = util.deserialize(f.read())
        start_time = timestamp if state.running_state == RunningState.WAITING else state.start_time
        next_state = update_state(state, running_state=opts.get("state", RunningState.RUNNING), start_time=start_time, latest_time=current_timestamp())
        f.seek(0)
        f.write(util.serialize(next_state))
        f.truncate()

def daemon(q, filename, interval):
    while True:
        try:
            update_timestamp(filename)
            try:
                item = q.get(timeout=interval)
            except queue.Empty:
                pass
            else:
                if item == "terminate":
                    update_timestamp(filename, state=RunningState.TERMINATED)
                    return
                else:
                    print("Something is wrong in heartbeat.", file=sys.stderr)
                    exit(1)
        except KeyboardInterrupt:
            pass

@contextlib.contextmanager
def heartbeat(filepath, **opts):
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=daemon, args=(q, filepath, opts.get("interval", 3)))
    p.start()
    try:
        yield
    finally:
        q.put("terminate")
        p.join()

def get_state(filepath, **opts):
    try:
        with open(filepath, "r") as f:
            state = util.deserialize(f.read())
        if state.running_state == RunningState.RUNNING:
            running_state = RunningState.TERMINATED if state.latest_time + opts.get("margin", 5) < current_timestamp() else RunningState.RUNNING
            return update_state(state, running_state=running_state)
        else:
            return state
    except:
        return State(RunningState.INVALID, None, None, None)

def init(filepath):
    with open(filepath, "w") as f:
        state = State(RunningState.WAITING, current_timestamp(), None, None)
        f.write(util.serialize(state))
