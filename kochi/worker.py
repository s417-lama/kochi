import subprocess

from . import job_queue

def worker_loop(queue_name):
    while True:
        job = job_queue.pop(queue_name)
        if job is None:
            return
        subprocess.run(job.commands, shell=True)

if __name__ == "__main__":
    """
    machine1$ python3 -m kochi.worker
    machine2$ python3 -m kochi.worker
    """
    queue_name = "test"
    for i in range(100):
        job_queue.push(queue_name, job_queue.Job("test_job_{}".format(i), [], "", "sleep 0.1; echo job {} completed".format(i)))
    worker_loop(queue_name)
