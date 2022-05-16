import os
import toml

from . import atomic_counter

def root_path():
    return os.environ.get("KOCHI_ROOT", os.path.join(os.path.expanduser("~"), ".kochi"))

def config_filepath():
    return os.path.join(root_path(), "conf.toml")

def config():
    with open(config_filepath(), "r") as f:
        return toml.load(f)

# Queues
# -----------------------------------------------------------------------------

def queue_dirpath():
    return os.path.join(root_path(), "queues")

def queue_filepath(queue_name):
    return os.path.join(queue_dirpath(), "{}.lock".format(queue_name))

# Workers
# -----------------------------------------------------------------------------

def worker_dirpath():
    return os.path.join(root_path(), "workers")

def worker_counter_filepath():
    return os.path.join(worker_dirpath(), "counter.lock")

def worker_log_filepath(idx):
    return os.path.join(worker_dirpath(), "log_{}.txt".format(idx))

def worker_workspace_dirpath(idx):
    return os.path.join(worker_dirpath(), "workspace_{}".format(idx))

# Jobs
# -----------------------------------------------------------------------------

def job_dirpath():
    return os.path.join(root_path(), "jobs")

def job_counter_filepath():
    return os.path.join(job_dirpath(), "counter.lock")

def job_log_filepath(idx):
    return os.path.join(job_dirpath(), "log_{}.txt".format(idx))

# Machine
# -----------------------------------------------------------------------------

def machine_config(machine):
    return config()["machines"][machine]

def ensure_init():
    os.makedirs(queue_dirpath() , exist_ok=True)
    os.makedirs(worker_dirpath(), exist_ok=True)
    os.makedirs(job_dirpath()   , exist_ok=True)
    try:
        atomic_counter.fetch(worker_counter_filepath())
    except:
        atomic_counter.reset(worker_counter_filepath())
    try:
        atomic_counter.fetch(job_counter_filepath())
    except:
        atomic_counter.reset(job_counter_filepath())
