import sys
import time
import multiprocessing
import contextlib

from . import settings

def update_timestamp(filename):
    with open(filename, "w") as f:
        t_sec = int(time.time())
        f.write(str(t_sec))
        pass

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
