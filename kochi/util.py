import os
import sys
import subprocess
import shutil
import pickle
import base64
import contextlib
import pathlib
import urllib
import textwrap

def decorate_command(commands, **opts):
    cmds = []
    if opts.get("env"):
        for k, v in opts.get("env").items():
            cmds.append("export {}=\"{}\"".format(k, v))
    if opts.get("cwd"):
        cmds.append("cd " + opts.get("cwd"))
    cmds.append(commands)
    return " && ".join(cmds)

def run_command_ssh_interactive(host, commands, **opts):
    subprocess.run("ssh -o LogLevel=QUIET -t {} \"{}\"".format(host, decorate_command(commands, **opts)),
                   shell=True)

def run_command_ssh(host, commands, **opts):
    return subprocess.run("ssh -o LogLevel=QUIET {} \"{}\"".format(host, decorate_command(commands, **opts)),
                          shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", check=True).stdout

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

def toplevel_git_dirpath():
    try:
        return subprocess.run(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    except subprocess.CalledProcessError:
        print("This command must be called inside a git project.", file=sys.stderr)
        sys.exit(1)

def ssh_keygen(keypath):
    subprocess.run(["ssh-keygen", "-t", "rsa", "-f", keypath, "-N", "", "-q"], check=True)

def dedent(string, **opts):
    indent = opts.get("indent", 0)
    return textwrap.indent(textwrap.dedent(string.strip("\n")), " " * indent)
