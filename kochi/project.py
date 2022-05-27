import os
import subprocess

from . import util
from . import settings
from . import config

def project_name_of_cwd():
    return util.git_repo_name(util.toplevel_git_dirpath())

def get_all_dependencies():
    deps = []
    for d in config.dependency_list():
        for r in config.recipe_list(d):
            deps.append((d, r))
    return deps

def run_on_login_node(machine, script):
    return util.run_command_ssh(config.login_host(machine), config.load_env_login_script(machine) + [script],
                                cwd=config.work_dir(machine)).strip()

def ensure_init(machine, project_name):
    os.makedirs(settings.project_dirpath(), exist_ok=True)
    if machine == "local":
        local_git_path = settings.project_git_dirpath(project_name)
        if not os.path.isdir(local_git_path):
            subprocess.run(["git", "init", "-q", "--bare", local_git_path], check=True)
        return local_git_path
    else:
        try:
            return run_on_login_node(machine, "kochi show path project {}".format(project_name))
        except subprocess.CalledProcessError:
            remote_git_path = run_on_login_node(machine, "kochi show path project -f {}".format(project_name))
            run_on_login_node(machine, "[ -d {0} ] || git init -q --bare {0}".format(remote_git_path))
            return remote_git_path

def sync(machine):
    project_name = project_name_of_cwd()
    git_path = ensure_init(machine, project_name)
    destination = "{}:{}".format(config.login_host(machine), git_path) if machine != "local" else git_path
    subprocess.run(["git", "push", "-q", destination, "--all"], check=True)
