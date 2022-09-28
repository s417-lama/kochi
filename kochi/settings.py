import os

from . import util

def root_path():
    return os.environ.get("KOCHI_ROOT", os.path.join(os.path.expanduser("~"), ".kochi"))

def config_filepath():
    if util.is_inside_git_dir():
        filepath = os.path.join(util.toplevel_git_dirpath(), ".kochi.yaml")
    else:
        filepath = os.path.join(root_path(), "conf.yaml")
    return os.environ.get("KOCHI_CONF", filepath)

# Queues
# -----------------------------------------------------------------------------

def queue_dirpath(machine):
    return os.path.join(root_path(), "queues", machine)

def queue_filepath(machine, queue_name):
    return os.path.join(queue_dirpath(machine), "{}.lock".format(queue_name))

# Workers
# -----------------------------------------------------------------------------

def worker_dirpath(machine):
    return os.path.join(root_path(), "workers", machine)

def worker_counter_filepath(machine):
    return os.path.join(worker_dirpath(machine), "counter.lock")

def worker_min_active_filepath(machine):
    return os.path.join(worker_dirpath(machine), "min_active.lock")

def worker_log_filepath(machine, idx):
    return os.path.join(worker_dirpath(machine), "log_{}.txt".format(idx))

def worker_workspace_dirpath(machine, idx):
    return os.path.join(worker_dirpath(machine), "workspace_{}".format(idx))

def worker_state_filepath(machine, idx):
    return os.path.join(worker_dirpath(machine), "state_{}.txt".format(idx))

def worker_heartbeat_filepath(machine, idx):
    return os.path.join(worker_dirpath(machine), "heartbeat_{}.txt".format(idx))

# Jobs
# -----------------------------------------------------------------------------

def job_dirpath(machine):
    return os.path.join(root_path(), "jobs", machine)

def job_counter_filepath(machine):
    return os.path.join(job_dirpath(machine), "counter.lock")

def job_min_active_filepath(machine):
    return os.path.join(job_dirpath(machine), "min_active.lock")

def job_log_filepath(machine, idx):
    return os.path.join(job_dirpath(machine), "log_{}.txt".format(idx))

def job_state_filepath(machine, idx):
    return os.path.join(job_dirpath(machine), "state_{}.txt".format(idx))

def job_cancelreq_filepath(machine, idx):
    return os.path.join(job_dirpath(machine), "cancelreq_{}.txt".format(idx))

# Projects
# -----------------------------------------------------------------------------

def project_dirpath():
    return os.path.join(root_path(), "projects")

def project_git_dirpath(project_name):
    return os.path.join(project_dirpath(), project_name, "git")

def project_artifact_git_dirpath(project_name):
    return os.path.join(project_dirpath(), project_name, "artifact_git")

def project_dep_install_dest_dirpath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dirpath(), project_name, "install", machine, dep_name, recipe_name)

def project_dep_install_src_dirpath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dirpath(), project_name, "install_src", machine, dep_name, recipe_name)

def project_dep_install_log_filepath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dep_install_dest_dirpath(project_name, machine, dep_name, recipe_name), ".kochi_log.txt")

def project_dep_install_state_filepath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dep_install_dest_dirpath(project_name, machine, dep_name, recipe_name), ".kochi_state.txt")

# sshd
# -----------------------------------------------------------------------------

def sshd_dirpath():
    return os.path.join(root_path(), "sshd")

def sshd_etc_dirpath():
    return os.path.join(sshd_dirpath(), "etc")

def sshd_hostkey_filepath():
    return os.path.join(sshd_etc_dirpath(), "ssh_host_rsa_key")

def sshd_clientkey_filepath():
    return os.path.join(sshd_etc_dirpath(), "ssh_client_rsa_key")

def sshd_clientpubkey_filepath():
    return os.path.join(sshd_etc_dirpath(), "ssh_client_rsa_key.pub")

def sshd_authorized_keys_filepath():
    return os.path.join(sshd_etc_dirpath(), "authorized_keys")

def sshd_config():
    return util.dedent("""
        HostKey {}
        AuthorizedKeysFile {}
        """.format(sshd_hostkey_filepath(), sshd_authorized_keys_filepath()))

def sshd_config_filepath():
    return os.path.join(sshd_etc_dirpath(), "sshd_config")

def sshd_var_run_dirpath(machine, worker_id):
    return os.path.join(worker_workspace_dirpath(machine, worker_id), "var", "run")

def sshd_client_config_filepath(machine, worker_id):
    return os.path.join(sshd_var_run_dirpath(machine, worker_id), "ssh_client_config")

def sshd_port():
    return os.environ.get("KOCHI_SSH_PORT", 2022)

# artifacts
# -----------------------------------------------------------------------------

def artifacts_dirname(machine, project_name):
    return "{}_artifacts_{}".format(project_name, machine)

def artifacts_master_branch():
    return "kochi_artifacts"

def artifacts_branch(machine):
    return "kochi_artifacts_{}".format(machine)

def artifacts_dirpath(machine, project_name, worker_id):
    return os.path.join(worker_workspace_dirpath(machine, worker_id), artifacts_dirname(machine, project_name))
