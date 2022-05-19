from collections import namedtuple

from . import util
from . import settings
from . import locked_queue
from . import atomic_counter

Job = namedtuple("Job", ["name", "machine", "queue", "dependencies", "context", "commands"])
JobEnqueued = namedtuple("JobEnqueued", ["id", "name", "dependencies", "context", "commands"])

def push(job):
    idx = atomic_counter.fetch_and_add(settings.job_counter_filepath(job.machine), 1)
    job_enqueued = JobEnqueued(idx, job.name, job.dependencies, job.context, job.commands)
    job_str = util.serialize(job_enqueued)
    locked_queue.push(settings.queue_filepath(job.machine, job.queue), job_str)
    return job_enqueued

def pop(machine, queue):
    job_str = locked_queue.pop(settings.queue_filepath(machine, queue))
    if job_str:
        return util.deserialize(job_str)
    else:
        return None

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
