from collections import namedtuple
import os

from . import util
from . import settings
from . import job_manager
from . import locked_queue
from . import atomic_counter

Job = namedtuple("Job", ["name", "machine", "queue", "dependencies", "context", "commands"])
JobEnqueued = namedtuple("JobEnqueued", ["id", "name", "dependencies", "context", "commands"])

def push(job):
    idx = atomic_counter.fetch_and_add(settings.job_counter_filepath(job.machine), 1)
    job_enqueued = JobEnqueued(idx, job.name, job.dependencies, job.context, job.commands)
    job_manager.init(job_enqueued, job.machine, job.queue)
    locked_queue.push(settings.queue_filepath(job.machine, job.queue), util.serialize(job_enqueued))
    return job_enqueued

def pop(machine, queue):
    job_serialized = locked_queue.pop(settings.queue_filepath(machine, queue))
    if job_serialized:
        return util.deserialize(job_serialized)
    else:
        return None

def ensure_init(machine):
    os.makedirs(settings.queue_dirpath(machine), exist_ok=True)

if __name__ == "__main__":
    """
    $ python3 -m kochi.job_queue
    """
    queue_name = "test"
    push(Job("test_job1", "local", queue_name, ["aaa"]       , "context", "commands"))
    push(Job("test_job2", "local", queue_name, ["bbb"]       , "context", "commands"))
    push(Job("test_job3", "local", queue_name, ["ccc", "ddd"], "context", "commands"))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name)) # should be None
