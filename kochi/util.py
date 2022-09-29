import os
import sys
import subprocess
import shutil
import re
import string
import itertools
import pickle
import base64
import contextlib
import pathlib
import urllib
import graphlib
import textwrap
import pexpect
import click

def decorate_command(commands, **opts):
    cmds = ["export {}=\"{}\"".format(k, v) for k, v in opts.get("env").items() if v] if opts.get("env") else []
    if opts.get("cwd"):
        cmds.append("cd " + opts.get("cwd"))
    cmds.append("\n".join(commands) if isinstance(commands, list) else commands)
    return " && ".join(cmds)

def run_command_ssh_expect(host, commands, send_commands, **opts):
    p = pexpect.spawn("ssh -o LogLevel=QUIET -t {} '{}'".format(host, decorate_command(commands, **opts)))
    for c in send_commands:
        p.sendline(c)
    p.interact()

def run_command_ssh_interactive(host, commands, **opts):
    subprocess.run("ssh -o LogLevel=QUIET -t {} '{}'".format(host, decorate_command(commands, **opts)),
                   shell=True, executable="/bin/bash")

def run_command_ssh(host, commands, **opts):
    return subprocess.run("ssh -o LogLevel=QUIET {} '{}'".format(host, decorate_command(commands, **opts)),
                          shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", check=True).stdout

def serialize(obj):
    return base64.b64encode(pickle.dumps(obj)).decode()

def deserialize(obj_str):
    return pickle.loads(base64.b64decode(obj_str.encode()))

@contextlib.contextmanager
def cwd(path):
    cwd_orig = os.getcwd()
    os.chdir(path)
    try:
        yield cwd_orig
    finally:
        os.chdir(cwd_orig)

@contextlib.contextmanager
def tmpdir(path):
    os.makedirs(path)
    try:
        with cwd(path) as old_cwd:
            yield old_cwd
    finally:
        shutil.rmtree(path)

def is_git_remote(s):
    if len(s.split(":")) == 2:
        # FIXME: too simple check for ssh host:path
        return True
    try:
        parsed = urllib.parse.urlparse(s)
        return all(parsed.scheme, parsed.netloc)
    except:
        return False

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

@contextlib.contextmanager
def tee(filepath, **opts):
    stdout = opts.get("stdout", sys.stdout)
    with subprocess.Popen(["tee", filepath], stdin=subprocess.PIPE, encoding="utf-8", start_new_session=True, stdout=stdout) as p:
        yield p

@contextlib.contextmanager
def tailf(filepaths, **opts):
    stdout = opts.get("stdout", sys.stdout)
    with subprocess.Popen(["tail", "-F"] + filepaths, encoding="utf-8", stdout=stdout, stderr=subprocess.DEVNULL) as p:
        try:
            yield p
        finally:
            p.terminate()

def get_ip_address_candidates():
    return subprocess.run(["hostname", "--all-ip-addresses"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip().split()

def ensure_dir_exists(filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

# params
# -----------------------------------------------------------------------------

def param_eval(value):
    if not hasattr(param_eval, "regexp"):
        backtick = re.escape("`")
        exppattern = r"[^{}]+".format(backtick)
        pattern = r"{backtick}({exp}){backtick}".format(backtick=backtick, exp=exppattern)
        param_eval.regexp = re.compile(pattern, re.IGNORECASE | re.VERBOSE)
    if isinstance(value, str):
        substitutes = [str(eval(exp)) for exp in param_eval.regexp.findall(value)]
        for s in substitutes:
            value = param_eval.regexp.sub(s, value, 1)
    return value

def param_substitute(params):
    if not hasattr(param_substitute, "regexp"):
        # The regexp was mostly copied from Python 3.10 implementation of string.Template
        delim = re.escape("$")
        idpattern = r"(?a:[_a-z][_a-z0-9]*)"
        pattern = r"""
        {delim}(?:
          {delim}    | # Escape sequence of two delimiters
          ({id})     | # delimiter and a Python identifier
          {{({id})}} | # delimiter and a braced identifier
        )
        """.format(delim=delim, id=idpattern)
        param_substitute.regexp = re.compile(pattern, re.IGNORECASE | re.VERBOSE)
    ts = graphlib.TopologicalSorter()
    for k, v in params.items():
        depend_params = []
        if isinstance(v, str):
            for dep in set(sum(param_substitute.regexp.findall(v), ())):
                if k == dep:
                    click.secho("Param '{}' cannot depend on itself.".format(k), fg="red", file=sys.stderr)
                    exit(1)
                if dep:
                    depend_params.append(dep)
        ts.add(k, *depend_params)
    try:
        keys_sorted = list(ts.static_order())
    except graphlib.CycleError as e:
        cycle = " -> ".join(e.args[1])
        click.secho("Parameter dependencies have at least one cycle ({}).".format(cycle), fg="red", file=sys.stderr)
        exit(1)
    new_params = params.copy()
    for k in keys_sorted:
        v = new_params[k]
        if isinstance(v, str):
            v = string.Template(v).substitute(new_params)
        new_params[k] = param_eval(v)
    return new_params

def param_product(params):
    param_pairs = []
    for k, vl in params.items():
        if isinstance(vl, list):
            param_pairs.append([(k, v) for v in vl])
        else:
            param_pairs.append([(k, vl)])
    return [dict(d) for d in itertools.product(*param_pairs)]
