from collections import namedtuple
import pickle
import base64

from . import locked_queue

Job = namedtuple("Job", ["name", "dependencies", "environment", "commands"])

def _queue_filename(queue_name):
    return "{}.lock".format(queue_name)

def push(queue_name, job):
    job_str = base64.b64encode(pickle.dumps(job)).decode()
    locked_queue.push(_queue_filename(queue_name), job_str)

def pop(queue_name):
    job_str = locked_queue.pop(_queue_filename(queue_name))
    if job_str:
        return pickle.loads(base64.b64decode(job_str.encode()))
    else:
        return None

if __name__ == "__main__":
    """
    $ python3 -m kochi.job_queue
    """
    queue_name = "test"
    push(queue_name, Job("test_job1", ["aaa"]       , "env", "commands"))
    push(queue_name, Job("test_job2", ["bbb"]       , "env", "commands"))
    push(queue_name, Job("test_job3", ["ccc", "ddd"], "env", "commands"))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name))
    print(pop(queue_name)) # should be None
