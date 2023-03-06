import os
import sys
import datetime
import tabulate

from . import settings
from . import atomic_counter
from . import worker
from . import job_manager
from . import installer

def get_all_worker_states(machine):
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath(machine))
    worker_states = []
    for idx in range(max_workers):
        state = worker.get_state(machine, idx)
        worker_states.append((idx, state))
    return worker_states

def get_all_active_worker_states(machine):
    min_workers = atomic_counter.fetch(settings.worker_min_active_filepath(machine))
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath(machine))
    min_updated = False
    worker_states = []
    for idx in range(min_workers, max_workers):
        state = worker.get_state(machine, idx)
        if state.running_state == worker.RunningState.WAITING or \
           state.running_state == worker.RunningState.RUNNING:
            worker_states.append((idx, state))
            if not min_updated:
                min_updated = True
                if idx > min_workers:
                    atomic_counter.reset(settings.worker_min_active_filepath(machine), idx)
    return worker_states

def get_all_job_states(machine, limit, show_all):
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath(machine))
    if show_all:
        limit = max_jobs
    job_states = []
    for idx in range(max(0, max_jobs - limit), max_jobs):
        state = job_manager.get_state(machine, idx)
        job_states.append((idx, state))
    return job_states

def get_all_active_job_states(machine, limit, show_all):
    min_jobs = atomic_counter.fetch(settings.job_min_active_filepath(machine))
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath(machine))
    if show_all:
        limit = max_jobs - min_jobs
    min_updated = False
    job_states = []
    for idx in range(min_jobs, max_jobs):
        state = job_manager.get_state(machine, idx)
        if state.running_state == job_manager.RunningState.WAITING or \
           state.running_state == job_manager.RunningState.RUNNING:
            job_states.append((idx, state))
            if not min_updated:
                min_updated = True
                if idx > min_jobs:
                    atomic_counter.reset(settings.job_min_active_filepath(machine), idx)
    return job_states[-limit:]

def show_queues(machine, **opts):
    queues = sorted(os.listdir(settings.queue_dirpath(machine)))
    for q in queues:
        print(q, file=opts.get("stdout", sys.stdout))

def show_workers(machine, show_all, queues, **opts):
    states = get_all_worker_states(machine) if show_all else get_all_active_worker_states(machine)
    table = []
    for idx, state in states:
        if (len(queues) == 0 or state.queue in queues):
            init_dt   = datetime.datetime.fromtimestamp(state.init_time)   if state.init_time   else None
            start_dt  = datetime.datetime.fromtimestamp(state.start_time)  if state.start_time  else None
            latest_dt = datetime.datetime.fromtimestamp(state.latest_time) if state.latest_time else None
            table.append([idx, str(state.running_state), state.queue, init_dt, start_dt,
                          latest_dt - start_dt if start_dt and latest_dt else None])
    print(tabulate.tabulate(table, headers=["ID", "State", "Queue", "Created Time", "Start Time", "Running Time"]), file=opts.get("stdout", sys.stdout))

def show_jobs(machine, show_terminated, limit, show_all, queues, names, **opts):
    states = get_all_job_states(machine, limit, show_all) if show_terminated else get_all_active_job_states(machine, limit, show_all)
    table = []
    for idx, state in states:
        if (len(queues) == 0 or state.queue in queues) and \
           (len(names) == 0 or state.name in names):
            init_dt   = datetime.datetime.fromtimestamp(state.init_time)   if state.init_time   else None
            start_dt  = datetime.datetime.fromtimestamp(state.start_time)  if state.start_time  else None
            latest_dt = datetime.datetime.fromtimestamp(state.latest_time) if state.latest_time else None
            table.append([idx, state.name, str(state.running_state), state.queue, state.worker_id, init_dt, start_dt,
                          latest_dt - start_dt if start_dt and latest_dt else None])
    print(tabulate.tabulate(table, headers=["ID", "Name", "State", "Queue", "Worker ID", "Created Time", "Start Time", "Running Time"]), file=opts.get("stdout", sys.stdout))

def show_job_detail(machine, job_id, **opts):
    state = job_manager.get_state(machine, job_id)
    table = []
    table.append(["Job ID", job_id])
    table.append(["Job Name", state.name])
    table.append(["Running State", str(state.running_state)])
    table.append(["Project", state.project_name])
    table.append(["Queue", state.queue])
    init_dt   = datetime.datetime.fromtimestamp(state.init_time)   if state.init_time   else None
    start_dt  = datetime.datetime.fromtimestamp(state.start_time)  if state.start_time  else None
    latest_dt = datetime.datetime.fromtimestamp(state.latest_time) if state.latest_time else None
    table.append(["Created Time", init_dt])
    table.append(["Start Time", start_dt])
    table.append(["Running Time", latest_dt - start_dt if start_dt and latest_dt else None])
    table.append(["Context Project", state.context.project if state.context else None])
    table.append(["Context Ref", state.context.reference if state.context else None])
    table.append(["Context Diff", state.context.diff if state.context else None])
    table.append(["Environment Variables", "\n".join(["{}={}".format(k, v) for k,v in state.envs.items()])])
    table.append(["Activate Script", "\n".join(state.activate_script)])
    table.append(["Build Executed", state.build_executed])
    table.append(["Build Parameters", "\n".join(["{}={}".format(k, v) for k,v in state.build_params.items()])])
    table.append(["Build Script", "\n".join(state.build_script)])
    table.append(["Run Parameters", "\n".join(["{}={}".format(k, v) for k,v in state.run_params.items()])])
    table.append(["Run Script", "\n".join(state.run_script)])
    print(tabulate.tabulate(table), file=opts.get("stdout", sys.stdout))
    for d in state.dependency_states:
        print("\n", file=opts.get("stdout", sys.stdout))
        print("Dependency {}:{}:".format(d.dependency, d.recipe), file=opts.get("stdout", sys.stdout))
        installer.show_detail(d, recurse=False, **opts)

def show_installs(machine, project_name, recipes, **opts):
    table = []
    for d, r in recipes:
        try:
            state = installer.get_state(project_name, d, r, machine)
            installation_state = "installed"
            installed_time = datetime.datetime.fromtimestamp(state.installed_time)
        except:
            installation_state = "NOT installed"
            installed_time = None
        table.append([d, r, installation_state, installed_time])
    print(tabulate.tabulate(table, headers=["Dependency", "Recipe", "State", "Installed Time"]), file=opts.get("stdout", sys.stdout))

def show_install_detail(machine, project_name, dependency, recipe, **opts):
    state = installer.get_state(project_name, dependency, recipe, machine)
    installer.show_detail(state, recurse=True, **opts)

def show_projects(**opts):
    projects = sorted(os.listdir(settings.project_dirpath()))
    for p in projects:
        print(p, file=opts.get("stdout", sys.stdout))

