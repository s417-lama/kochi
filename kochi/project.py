import os
import subprocess

from . import util
from . import settings
from . import config

def project_name_of_cwd():
    return util.git_repo_name(util.toplevel_git_dirpath())

def get_all_recipes():
    recipes = []
    for d in config.dependency_list():
        for r in config.recipe_list(d):
            recipes.append((d, r))
    return recipes

def run_on_login_node(machine, script):
    return util.run_command_ssh(config.login_host(machine), config.load_env_login_script(machine) + [script],
                                cwd=config.work_dir(machine)).strip()

def ensure_init(machine, project_name, is_artifact=False):
    os.makedirs(settings.project_dirpath(), exist_ok=True)
    if machine == "local":
        local_git_path = settings.project_artifact_git_dirpath(project_name) if is_artifact else settings.project_git_dirpath(project_name)
        if not os.path.isdir(local_git_path):
            subprocess.run(["git", "init", "-q", "--bare", local_git_path], check=True)
        return local_git_path
    else:
        is_artifact_opt = "--is-artifact" if is_artifact else ""
        try:
            return run_on_login_node(machine, "kochi show path project {} {}".format(is_artifact_opt, project_name))
        except subprocess.CalledProcessError:
            remote_git_path = run_on_login_node(machine, "kochi show path project -f {} {}".format(is_artifact_opt, project_name))
            run_on_login_node(machine, "[ -d {0} ] || git init -q --bare {0}".format(remote_git_path))
            return remote_git_path

def git_destination(machine, is_artifact=False):
    project_name = project_name_of_cwd()
    git_path = ensure_init(machine, project_name, is_artifact)
    return "{}:{}".format(config.login_host(machine), git_path) if machine != "local" else git_path

def sync(machine):
    destination = git_destination(machine)
    subprocess.run(["git", "push", "-f", "-q", destination, "--all"], check=True)
