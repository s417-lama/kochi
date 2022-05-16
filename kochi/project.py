import os
import subprocess

from . import util
from . import settings

def project_name_of_cwd():
    toplevel_gitdir = subprocess.run(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    return util.git_repo_name(toplevel_gitdir)

def ensure_init(host, project_name):
    if host:
        try:
            return util.run_command_ssh(host, "kochi show path project {}".format(project_name)).strip()
        except subprocess.CalledProcessError:
            remote_git_path = util.run_command_ssh(host, "kochi show path project -f {}".format(project_name)).strip()
            util.run_command_ssh(host, "[ -d {0} ] || git init -q --bare {0}".format(remote_git_path))
            return remote_git_path
    else:
        local_git_path = settings.project_git_dirpath(project_name)
        if not os.path.isdir(local_git_path):
            subprocess.run(["git", "init", "-q", "--bare", local_git_path], check=True)
        return local_git_path

def sync(host):
    project_name = project_name_of_cwd()
    git_path = ensure_init(host, project_name)
    destination = "{}:{}".format(host, git_path) if host else git_path
    subprocess.run(["git", "push", "-q", destination, "--all"], check=True)
