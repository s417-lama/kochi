import subprocess

from . import util

def project_name_of_cwd():
    toplevel_gitdir = subprocess.run(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE, encoding="utf-8", check=True).stdout.strip()
    return util.git_repo_name(toplevel_gitdir)

def ensure_init(host, project_name):
    try:
        return util.run_command_ssh(host, "kochi show path project {}".format(project_name)).strip()
    except subprocess.CalledProcessError:
        remote_git_path = util.run_command_ssh(host, "kochi show path project -f {}".format(project_name)).strip()
        util.run_command_ssh(host, "[ -d {0} ] || git init -q --bare {0}".format(remote_git_path))
        return remote_git_path

def sync(host):
    project_name = project_name_of_cwd()
    remote_git_path = ensure_init(host, project_name)
    subprocess.run(["git", "push", "-q", "{}:{}".format(host, remote_git_path), "--all"], check=True)
