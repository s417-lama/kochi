from collections import namedtuple

from . import util
from . import settings
from . import locked_queue
from . import atomic_counter

Job = namedtuple("Job", ["name", "dependencies", "context", "commands"])
JobEnqueued = namedtuple("JobEnqueued", ["id", "name", "dependencies", "context", "commands"])

def push(queue_name, job):
    idx = atomic_counter.fetch_and_add(settings.job_counter_filepath(), 1)
    job_enqueued = JobEnqueued(idx, job.name, job.dependencies, job.context, job.commands)
    job_str = util.serialize(job_enqueued)
    locked_queue.push(settings.queue_filepath(queue_name), job_str)

def pop(queue_name):
    job_str = locked_queue.pop(settings.queue_filepath(queue_name))
    if job_str:
        return util.deserialize(job_str)
    else:
        return None

if __name__ == "__main__":
    """
    $ python3 -m kochi.job_queue
    """
    queue_name = "test"
    push(queue_name, Job("test_job1", ["aaa"]       , "context", "commands"))
    push(queue_name, Job("test_job2", ["bbb"]       , "context", "commands"))
    push(queue_name, Job("test_job3", ["ccc", "ddd"], "context", "commands"))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name)) # should be None
