import os
import subprocess

from . import settings
from . import atomic_counter

def show_queues():
    queues = sorted(os.listdir(settings.queue_dirpath()))
    print(queues)

def show_workers():
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath())
    for idx in range(max_workers):
        print("Worker {}".format(idx))

def show_jobs():
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath())
    for idx in range(max_jobs):
        print("Job {}".format(idx))

def show_job_log(idx):
    subprocess.run(["cat", settings.job_log_filepath(idx)])
