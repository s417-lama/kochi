import os

from . import settings
from . import atomic_counter

def show_queues(machine):
    queues = sorted(os.listdir(settings.queue_dirpath(machine)))
    for q in queues:
        print(q)

def show_workers(machine):
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath(machine))
    for idx in range(max_workers):
        print("Worker {}".format(idx))

def show_jobs(machine):
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath(machine))
    for idx in range(max_jobs):
        print("Job {}".format(idx))

def show_projects():
    projects = sorted(os.listdir(settings.project_dirpath()))
    for p in projects:
        print(p)
