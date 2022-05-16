import os
import subprocess
import shutil
import pickle
import base64
import contextlib
import pathlib
import urllib

def run_command_ssh_interactive(host, commands):
    subprocess.run("ssh -o LogLevel=QUIET -t {} '{}'".format(host, commands), shell=True)

def run_command_ssh(host, commands):
    return subprocess.run("ssh -o LogLevel=QUIET {} '{}'".format(host, commands), shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", check=True).stdout

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

def get_path(s):
    try:
        parsed = urllib.parse.urlparse(s)
        if parsed.scheme and parsed.netloc:
            return parsed.path
        else:
            return s
    except:
        return s

def git_repo_name(git_remote):
    return pathlib.Path(get_path(git_remote)).stem

def is_inside_git_dir():
    try:
        result = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], stdout=subprocess.PIPE, check=True, encoding="utf-8").stdout.strip()
        return result == "true"
    except subprocess.CalledProcessError:
        return False
