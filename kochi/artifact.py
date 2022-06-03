import os
import subprocess
import shutil
import time
import string
import pathlib

from . import util
from . import settings
from . import stats
from . import project

def artifact_path(machine, worker_id, project_name, relative_path):
    return os.path.join(settings.artifacts_dirpath(machine, project_name, worker_id), machine, relative_path)

def save(machine, worker_id, job):
    ensure_init_worker(machine, worker_id, job.context)
    for artifact_conf in job.artifacts_conf:
        dest_path = string.Template(artifact_path(machine, worker_id, job.context.project, artifact_conf["dest"])).substitute(job.params)
        util.ensure_dir_exists(dest_path)
        if artifact_conf["type"] == "stdout":
            shutil.copy2(settings.job_log_filepath(machine, job.id), dest_path)
        elif artifact_conf["type"] == "stats":
            with open(dest_path, "w") as f:
                stats.show_job_detail(machine, job.id, stdout=f)
        elif artifact_conf["type"] == "file":
            shutil.copy2(artifact_conf["src"], dest_path)
    push_loop(machine, worker_id, job.context)

def try_push(machine):
    branch = settings.artifacts_branch(machine)
    try:
        subprocess.run(["git", "pull", "--rebase", "-s", "recursive", "-X", "theirs", "-q", "origin", branch], check=True)
        subprocess.run(["git", "push", "-q", "origin", branch], check=True)
    except:
        subprocess.run(["git", "reset"], check=True)
        return False
    else:
        return True

def push_loop(machine, worker_id, ctx):
    max_retry = 20
    retry_count = 0
    with util.cwd(settings.artifacts_dirpath(machine, ctx.project, worker_id)):
        commit_msg = "[kochi] add artifact on {}".format(machine)
        subprocess.run(["git", "add", "--all"], check=True)
        subprocess.run(["git", "-c", "user.name='kochi'", "-c", "user.email='<>'", "commit", "-q", "-m", commit_msg], check=True)
        while not try_push(machine):
            if retry_count == max_retry:
                raise Exception("Could not push artifacts (max_retry={})".format(max_retry))
            retry_count += 1
            time.sleep(1)

def ensure_init_worker(machine, worker_id, ctx):
    dirpath = settings.artifacts_dirpath(machine, ctx.project, worker_id)
    if not os.path.isdir(dirpath):
        git_remote = ctx.git_remote if ctx.git_remote else settings.project_artifact_git_dirpath(ctx.project)
        branch = settings.artifacts_branch(machine)
        subprocess.run(["git", "clone", "--recursive", "-q", "-b", branch, git_remote, dirpath], check=True)
    os.makedirs(artifact_path(machine, worker_id, ctx.project, "."), exist_ok=True)

def get_artifact_worktree():
    for line in subprocess.run(["git", "worktree", "list"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip().split("\n"):
        path, _commit_hash, branch = line.split()
        if branch == "[{}]".format(settings.artifacts_master_branch()):
            return path
    return None

def ensure_init_machine(machine):
    """
    Ensures that artifact branchs exist in the current project.
    This function must be called locally.
    """
    worktree_path = get_artifact_worktree()
    if not worktree_path:
        raise Exception(util.dedent("Please run 'kochi artifact init <git_worktree_path>'."))
    with util.cwd(worktree_path) as old_cwd:
        try:
            subprocess.run(["git", "checkout", "-q", settings.artifacts_branch(machine)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except:
            subprocess.run(["git", "checkout", "-q", "-B", settings.artifacts_branch(machine)], check=True)
            with util.cwd(old_cwd):
                destination = project.git_destination(machine, True)
            subprocess.run(["git", "push", "-u", "-q", destination, settings.artifacts_branch(machine)], check=True)
        finally:
            subprocess.run(["git", "checkout", "-q", settings.artifacts_master_branch()], check=True)

def init(new_worktree_path):
    """
    Creates an artifact branch as an orphan branch and creates a git worktree for that branch at `new_worktree_path`.
    """
    worktree_path = get_artifact_worktree()
    if worktree_path:
        raise Exception("A git worktree for '{}' branch already exists at {}".format(settings.artifacts_master_branch(), worktree_path))
    subprocess.run(["git", "worktree", "add", "--detach", new_worktree_path], check=True)
    with util.cwd(new_worktree_path):
        subprocess.run(["git", "checkout", "--orphan", settings.artifacts_master_branch()], check=True)
        subprocess.run(["git", "reset", "--hard"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "[kochi] create an artifact branch"], check=True)

def sync(machine):
    worktree_path = get_artifact_worktree()
    if not worktree_path:
        raise Exception("Please run 'kochi artifact init <git_worktree_path>'.")
    with util.cwd(worktree_path):
        try:
            subprocess.run(["git", "checkout", "-q", settings.artifacts_branch(machine)], check=True)
            subprocess.run(["git", "pull"], check=True)
        finally:
            subprocess.run(["git", "checkout", "-q", settings.artifacts_master_branch()], check=True)
        subprocess.run(["git", "merge", "--no-edit", settings.artifacts_branch(machine)], check=True)
