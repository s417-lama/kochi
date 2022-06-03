import os
import sys
import datetime
import tabulate

from . import settings
from . import atomic_counter
from . import worker
from . import job_manager
from . import installer

def show_queues(machine, **opts):
    queues = sorted(os.listdir(settings.queue_dirpath(machine)))
    for q in queues:
        print(q, file=opts.get("stdout", sys.stdout))

def show_workers(machine, show_all, queues, **opts):
    max_workers = atomic_counter.fetch(settings.worker_counter_filepath(machine))
    table = []
    for idx in range(max_workers):
        state = worker.get_state(machine, idx)
        if (show_all or state.running_state == worker.RunningState.WAITING or state.running_state == worker.RunningState.RUNNING) and \
           (len(queues) == 0 or state.queue in queues):
            init_dt   = datetime.datetime.fromtimestamp(state.init_time)   if state.init_time   else None
            start_dt  = datetime.datetime.fromtimestamp(state.start_time)  if state.start_time  else None
            latest_dt = datetime.datetime.fromtimestamp(state.latest_time) if state.latest_time else None
            table.append([idx, str(state.running_state), state.queue, init_dt, start_dt,
                          latest_dt - start_dt if start_dt and latest_dt else None])
    print(tabulate.tabulate(table, headers=["ID", "State", "Queue", "Created Time", "Start Time", "Running Time"]), file=opts.get("stdout", sys.stdout))

def show_jobs(machine, show_all, queues, names, **opts):
    max_jobs = atomic_counter.fetch(settings.job_counter_filepath(machine))
    table = []
    for idx in range(max_jobs):
        state = job_manager.get_state(machine, idx)
        if (show_all or state.running_state == job_manager.RunningState.WAITING or state.running_state == job_manager.RunningState.RUNNING) and \
           (len(queues) == 0 or state.queue in queues) and \
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
    table.append(["Running State", state.running_state])
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
    table.append(["Parameters", "\n".join(["{}={}".format(k, v) for k,v in state.params.items()])])
    table.append(["Environment Variables", "\n".join(["{}={}".format(k, v) for k,v in state.envs.items()])])
    table.append(["Activate Script", "\n".join(state.activate_script)])
    table.append(["Script", "\n".join(state.script)])
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

