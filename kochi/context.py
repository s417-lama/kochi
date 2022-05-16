from collections import namedtuple
import os
import subprocess
import contextlib

from . import util

Context = namedtuple("Context", ["git_remote", "commit_hash", "diff"])

def create(git_remote):
    commit_hash = subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    subprocess.run(["git", "add", "-N", "."], check=True)
    diff = subprocess.run(["git", "diff", "--binary", commit_hash], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout
    if not git_remote:
        git_remote = subprocess.run(["git", "remote", "get-url", "origin"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    return Context(git_remote, commit_hash, diff)

def deploy(ctx):
    subprocess.run(["git", "fetch", "-q"], check=True)
    subprocess.run(["git", "checkout", "-f", "-q", ctx.commit_hash], check=True)
    subprocess.run(["git", "clean", "-f", "-d", "-q"], check=True)
    subprocess.run(["git", "apply", "-"], input=ctx.diff, encoding="utf-8", check=True)

@contextlib.contextmanager
def context(ctx):
    if ctx:
        dirname = util.git_repo_name(ctx.git_remote)
        if not os.path.isdir(dirname):
            subprocess.run(["git", "clone", "-q", ctx.git_remote, dirname], check=True)
        with util.cwd(dirname):
            deploy(ctx)
            yield
    else:
        yield
