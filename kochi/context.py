from collections import namedtuple
import os
import sys
import subprocess
import contextlib

from . import util
from . import settings
from . import config
from . import project

Context = namedtuple("Context", ["project", "git_remote", "reference", "diff"])

def create(git_remote):
    project_name = project.project_name_of_cwd()
    commit_hash = subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    subprocess.run(["git", "add", "-N", "."], check=True)
    diff = subprocess.run(["git", "diff", "--binary", commit_hash], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout
    return Context(project_name, git_remote, commit_hash, diff)

def create_with_branch(branch, git_remote):
    project_name = project.project_name_of_cwd()
    return Context(project_name, git_remote, branch, None)

def create_with_commit_hash(commit_hash, git_remote):
    project_name = project.project_name_of_cwd()
    return Context(project_name, git_remote, commit_hash, None)

def create_for_recipe(dep, recipe, git_remote):
    mirror      = config.recipe_mirror(dep, recipe)
    mirror_dir  = config.recipe_mirror_dir(dep, recipe)
    branch      = config.recipe_branch(dep, recipe)
    commit_hash = config.recipe_commit_hash(dep, recipe)
    if mirror:
        if not mirror_dir:
            print("Please specify 'mirror_dir' if 'mirror' is True in recipe config.", file=sys.stderr)
            exit(1)
        with util.cwd(util.toplevel_git_dirpath()):
            with util.cwd(mirror_dir):
                return create(git_remote)
    elif branch:
        return create_with_branch(branch, git_remote)
    elif commit_hash:
        return create_with_commit_hash(commit_hash, git_remote)
    else:
        print("Please specify any of 'mirror', 'branch', and 'commit_hash' option in recipe config.", file=sys.stderr)
        exit(1)

def deploy(ctx):
    subprocess.run(["git", "fetch", "-q"], check=True)
    subprocess.run(["git", "checkout", "-f", "-q", ctx.reference], check=True)
    subprocess.run(["git", "submodule", "update", "--init", "--recursive", "--quiet"], check=True)
    subprocess.run(["git", "clean", "-f", "-d", "-q"], check=True)
    if ctx.diff:
        subprocess.run(["git", "apply", "--whitespace=nowarn", "-"], input=ctx.diff, encoding="utf-8", check=True)

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
