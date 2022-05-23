import os
import datetime
import tabulate

from . import settings
from . import atomic_counter
from . import worker

def show_queues(machine):
    queues = sorted(os.listdir(settings.queue_dirpath(machine)))
    for q in queues:
        print(q)

def show_workers(machine, all):
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath(machine))
    table = []
    for idx in range(max_workers):
        state = worker.get_state(machine, idx)
        if all or state.running_state == worker.RunningState.RUNNING:
            init_dt   = datetime.datetime.fromtimestamp(state.init_time)   if state.init_time   else None
            start_dt  = datetime.datetime.fromtimestamp(state.start_time)  if state.start_time  else None
            latest_dt = datetime.datetime.fromtimestamp(state.latest_time) if state.latest_time else None
            table.append([idx, str(state.running_state), init_dt, start_dt,
                          latest_dt - start_dt if start_dt and latest_dt else None])
    print(tabulate.tabulate(table, headers=["ID", "State", "Created Time", "Start Time", "Running Time"]))

def show_jobs(machine):
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath(machine))
    for idx in range(max_jobs):
        print("Job {}".format(idx))

def show_projects():
    projects = sorted(os.listdir(settings.project_dirpath()))
    for p in projects:
        print(p)
