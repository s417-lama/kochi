from collections import namedtuple
import os

from . import util
from . import settings
from . import job_manager
from . import locked_queue
from . import atomic_counter
from . import installer

Job = namedtuple("Job", ["name", "machine", "project_name", "queue", "dependencies", "context", "params", "artifacts_conf", "activate_script", "build_conf", "run_conf"])
JobEnqueued = namedtuple("JobEnqueued", ["id", "name", "project_name", "dependencies", "context", "params", "artifacts_conf", "activate_script", "build_conf", "run_conf"])

def push(job):
    if job.context:
        installer.check_dependencies(job.project_name, job.machine, job.dependencies)
    idx = atomic_counter.fetch_and_add(settings.job_counter_filepath(job.machine), 1)
    job_enqueued = JobEnqueued(idx, job.name, job.project_name, job.dependencies, job.context, job.params, job.artifacts_conf, job.activate_script, job.build_conf, job.run_conf)
    job_manager.init(job_enqueued, job.machine, job.queue)
    locked_queue.push(settings.queue_filepath(job.machine, job.queue), util.serialize(job_enqueued))
    return job_enqueued

def pop(machine, queue):
    try:
        job_serialized = locked_queue.pop(settings.queue_filepath(machine, queue))
    except FileNotFoundError:
        return None
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
    push(Job("test_job1", "local", "proj", queue_name, ["aaa"]       , "context", dict(), [], "", dict(), dict(script=["run1"])))
    push(Job("test_job2", "local", "proj", queue_name, ["bbb"]       , "context", dict(), [], "", dict(), dict(script=["run2"])))
    push(Job("test_job3", "local", "proj", queue_name, ["ccc", "ddd"], "context", dict(), [], "", dict(), dict(script=["run3"])))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name)) # should be None
