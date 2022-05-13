import subprocess
import time

from . import settings
from . import job_queue
from . import atomic_counter

def worker_loop(queue_name, blocking, stdout):
    while True:
        job = job_queue.pop(queue_name)
        if job:
            print("Kochi job {} started.".format(job.name), file=stdout, flush=True)
            subprocess.run(job.commands, shell=not isinstance(job.commands, list), stdout=stdout)
            print("Kochi job {} finished.".format(job.name), file=stdout, flush=True)
        elif blocking:
            time.sleep(0.1) # TODO: monitor filesystem events?
        else:
            return

def start(queue_name, blocking):
    idx = atomic_counter.fetch_and_add(settings.worker_counter_filepath(), 1)
    tee = subprocess.Popen(["tee", settings.worker_log_filepath(idx)], stdin=subprocess.PIPE, encoding="utf-8")
    print("Kochi worker {} started.".format(idx), file=tee.stdin, flush=True)
    worker_loop(queue_name, blocking, tee.stdin)
    print("Kochi worker {} finished.".format(idx), file=tee.stdin, flush=True)
    tee.stdin.close()

if __name__ == "__main__":
    """
    machine1$ python3 -m kochi.worker
    machine2$ python3 -m kochi.worker
    """
    queue_name = "test"
    for i in range(100):
        job_queue.push(queue_name, job_queue.Job("test_job_{}".format(i), [], "", "sleep 0.1; echo job {} completed".format(i)))
    start(queue_name, False)
