import os
import subprocess
import shutil
import pickle
import base64
import contextlib

def run_command_ssh(host, commands):
    subprocess.run("ssh -o LogLevel=QUIET -t {} '{}'".format(host, commands), shell=True)

def serialize(obj):
    return base64.b64encode(pickle.dumps(obj)).decode()

def deserialize(obj_str):
    return pickle.loads(base64.b64decode(obj_str.encode()))

@contextlib.contextmanager
def cwd(path):
    cwd_orig = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd_orig)

@contextlib.contextmanager
def tmpdir(path):
    os.makedirs(path)
    try:
        with cwd(path):
            yield
    finally:
        shutil.rmtree(path)
