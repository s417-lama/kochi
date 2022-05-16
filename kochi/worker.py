import subprocess
import time
import click

from . import util
from . import settings
from . import job_queue
from . import atomic_counter
from . import context

def run_job(job, stdout):
    color = "blue"
    print(click.style("Kochi job {} (ID={}) started.".format(job.name, job.id), fg=color), file=stdout, flush=True)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)
    with context.context(job.context):
        with subprocess.Popen(["tee", settings.job_log_filepath(job.id)], stdin=subprocess.PIPE, stdout=stdout, encoding="utf-8") as tee:
            subprocess.run(job.commands, shell=not isinstance(job.commands, list), stdout=tee.stdin, stderr=tee.stdin)
    print(click.style("-" * 80, fg=color), file=stdout, flush=True)

def worker_loop(queue_name, blocking, stdout):
    while True:
        job = job_queue.pop(queue_name)
        if job:
            run_job(job, stdout)
        elif blocking:
            time.sleep(0.1) # TODO: monitor filesystem events?
        else:
            return

def start(queue_name, blocking):
    idx = atomic_counter.fetch_and_add(settings.worker_counter_filepath(), 1)
    workspace = settings.worker_workspace_dirpath(idx)
    with util.tmpdir(workspace):
        with subprocess.Popen(["tee", settings.worker_log_filepath(idx)], stdin=subprocess.PIPE, encoding="utf-8") as tee:
            color = "green"
            print(click.style("Kochi worker {} started.".format(idx), fg=color), file=tee.stdin, flush=True)
            print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)
            worker_loop(queue_name, blocking, tee.stdin)
            print(click.style("=" * 80, fg=color), file=tee.stdin, flush=True)

if __name__ == "__main__":
    """
    machine1$ python3 -m kochi.worker
    machine2$ python3 -m kochi.worker
    """
    import os
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
            job_queue.push(queue_name, job_queue.Job("test_job_{}".format(i), [], ctx, "sleep 0.1; cat {}".format(test_filename)))
    os.remove(test_filename)
    start(queue_name, False)
