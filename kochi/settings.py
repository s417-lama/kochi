import os

from . import atomic_counter

def root_path():
    return os.environ.get("KOCHI_ROOT", os.path.join(os.path.expanduser("~"), ".kochi"))

def queue_filepath(queue_name):
    return os.path.join(root_path(), "queues", "{}.lock".format(queue_name))

def worker_counter_filepath():
    return os.path.join(root_path(), "workers", "counter.lock")

def worker_log_filepath(idx):
    return os.path.join(root_path(), "workers", "log_{}.txt".format(idx))

def init():
    os.makedirs(os.path.join(root_path(), "queues"), exist_ok=True)
    os.makedirs(os.path.join(root_path(), "workers"), exist_ok=True)
    atomic_counter.reset(worker_counter_filepath())
