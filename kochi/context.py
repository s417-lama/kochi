from collections import namedtuple
import os
import subprocess
import contextlib

from . import util
from . import settings
from . import project

Context = namedtuple("Context", ["project", "git_remote", "commit_hash", "diff"])

def create(git_remote):
    project_name = project.project_name_of_cwd()
    commit_hash = subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    subprocess.run(["git", "add", "-N", "."], check=True)
    diff = subprocess.run(["git", "diff", "--binary", commit_hash], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout
    return Context(project_name, git_remote, commit_hash, diff)

def deploy(ctx):
    subprocess.run(["git", "fetch", "-q"], check=True)
    subprocess.run(["git", "checkout", "-f", "-q", ctx.commit_hash], check=True)
    subprocess.run(["git", "clean", "-f", "-d", "-q"], check=True)
    subprocess.run(["git", "apply", "-"], input=ctx.diff, encoding="utf-8", check=True)

@contextlib.contextmanager
def context(ctx):
    if ctx:
        if not os.path.isdir(ctx.project):
            git_remote = ctx.git_remote if ctx.git_remote else settings.project_git_dirpath(ctx.project)
            subprocess.run(["git", "clone", "-q", git_remote, ctx.project], check=True)
        with util.cwd(ctx.project):
            deploy(ctx)
            yield
    else:
        yield
