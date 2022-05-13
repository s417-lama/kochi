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

def show_worker_log(idx):
    subprocess.run(["cat", settings.worker_log_filepath(idx)])
