import sys
import time
import multiprocessing
import contextlib
import enum

from . import settings

class State(enum.IntEnum):
    def __str__(self):
        if self.value == self.READY:
            return "ready"
        elif self.value == self.RUNNING:
            return "running"
        elif self.value == self.TERMINATED:
            return "terminated"
        else:
            return "invalid"
    INVALID = 0
    READY = 1
    RUNNING = 2
    TERMINATED = 3

def current_timestamp():
    return int(time.time())

def update_timestamp(filename):
    with open(filename, "w") as f:
        t_sec = current_timestamp()
        f.write(str(t_sec))

def daemon(queue, filename, interval):
    try:
        while True:
            update_timestamp(filename)
            if queue.empty():
                time.sleep(interval)
            elif queue.get_nowait() == "terminate":
                return
            else:
                print("Something is wrong in heartbeat.", file=sys.stderr)
                exit(1)
    except KeyboardInterrupt:
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
            s = f.read()
            if s == "initialized":
                return State.READY
            timestamp = int(s)
            if timestamp + opts.get("margin", 5) < current_timestamp():
                return State.TERMINATED
            else:
                return State.RUNNING
    except:
        return State.INVALID

def init(machine, worker_id):
    filename = settings.worker_heartbeat_filepath(machine, worker_id)
    with open(filename, "w") as f:
        f.write("initialized")
