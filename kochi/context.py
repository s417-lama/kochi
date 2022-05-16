from collections import namedtuple
import os
import sys
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

def create_with_branch(branch, git_remote):
    project_name = project.project_name_of_cwd()
    commit_hash = subprocess.run(["git", "rev-parse", branch], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    return Context(project_name, git_remote, commit_hash, "")

def create_with_commit_hash(commit_hash, git_remote):
    project_name = project.project_name_of_cwd()
    return Context(project_name, git_remote, commit_hash, "")

def create_with_project_config(dep_config, git_remote):
    if dep_config.get("current_state") and (dep_config.get("branch") or dep_config.get("commit_hash")):
        print("'current_state' cannot coexist with 'branch' or 'commit_hash' in installation config.", file=sys.stderr)
        exit(1)
    if dep_config.get("branch") and dep_config.get("commit_hash"):
        print("'branch' and 'commit_hash' cannot coexist in installation config.", file=sys.stderr)
        exit(1)
    if dep_config.get("current_state"):
        return create(git_remote)
    elif dep_config.get("branch"):
        return create_with_branch(dep_config["branch"], git_remote)
    elif dep_config.get("commit_hash"):
        return create_with_commit_hash(dep_config["commit_hash"], git_remote)
    else:
        print("Please specify any of 'current_state', 'branch', and 'commit_hash' option in installation config.", file=sys.stderr)
        exit(1)

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
