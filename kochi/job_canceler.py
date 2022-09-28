import os
import sys
import signal
import queue
import multiprocessing
import contextlib

from . import settings

def daemon(q, machine, job_id, interval, parent_pid):
    while True:
        try:
            try:
                item = q.get(timeout=interval)
            except queue.Empty:
                pass
            else:
                if item == "terminate":
                    return
                else:
                    print("Something is wrong in job_canceler.", file=sys.stderr)
                    exit(1)
            if check_canceled(machine, job_id):
                pgid = os.getpgid(parent_pid)
                os.killpg(pgid, signal.SIGINT)
        except KeyboardInterrupt:
            pass

@contextlib.contextmanager
def job_canceler(machine, job_id, **opts):
    pid = os.getpid()
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=daemon, args=(q, machine, job_id, opts.get("interval", 5), pid))
    p.start()
    try:
        yield
    finally:
        q.put("terminate")
        p.join()

def cancel(machine, job_id):
    with open(settings.job_cancelreq_filepath(machine, job_id), "w") as f:
        f.write("canceled")

def check_canceled(machine, job_id):
    filepath = settings.job_cancelreq_filepath(machine, job_id)
    return os.path.exists(filepath)
