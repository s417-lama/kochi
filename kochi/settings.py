import os
import yaml

from . import util
from . import atomic_counter

def root_path():
    return os.environ.get("KOCHI_ROOT", os.path.join(os.path.expanduser("~"), ".kochi"))

def config_filepath():
    if util.is_inside_git_dir():
        filepath = os.path.join(util.toplevel_git_dirpath(), ".kochi.yaml")
    else:
        filepath = os.path.join(root_path(), "conf.yaml")
    return os.environ.get("KOCHI_CONF", filepath)

def config():
    with open(config_filepath(), "r") as f:
        return yaml.safe_load(f)

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

def worker_log_filepath(machine, idx):
    return os.path.join(worker_dirpath(machine), "log_{}.txt".format(idx))

def worker_workspace_dirpath(machine, idx):
    return os.path.join(worker_dirpath(machine), "workspace_{}".format(idx))

def worker_heartbeat_filepath(machine, idx):
    return os.path.join(worker_dirpath(machine), "heartbeat_{}.txt".format(idx))

# Jobs
# -----------------------------------------------------------------------------

def job_dirpath(machine):
    return os.path.join(root_path(), "jobs", machine)

def job_counter_filepath(machine):
    return os.path.join(job_dirpath(machine), "counter.lock")

def job_log_filepath(machine, idx):
    return os.path.join(job_dirpath(machine), "log_{}.txt".format(idx))

# Projects
# -----------------------------------------------------------------------------

def project_dirpath():
    return os.path.join(root_path(), "projects")

def project_git_dirpath(project_name):
    return os.path.join(project_dirpath(), project_name, "git")

def project_dep_install_dirpath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dirpath(), project_name, "install", machine, dep_name, recipe_name)

def project_dep_install_tmp_dirpath(project_name, machine, dep_name, recipe_name):
    return os.path.join(project_dirpath(), project_name, "tmp", "install", machine, dep_name, recipe_name)

def project_dep_config(project_name):
    return config()["dependencies"][project_name]

def project_dep_recipe_config(project_name, recipe_name):
    return project_dep_config(project_name)["recipes"][recipe_name]

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

# Machine
# -----------------------------------------------------------------------------

def machine_config(machine):
    return config()["machines"][machine]

def ensure_init():
    os.makedirs(project_dirpath() , exist_ok=True)
    os.makedirs(sshd_dirpath()    , exist_ok=True)
    os.makedirs(sshd_etc_dirpath(), exist_ok=True)
    if not os.path.isfile(sshd_hostkey_filepath()):
        util.ssh_keygen(sshd_hostkey_filepath())
    if not os.path.isfile(sshd_clientkey_filepath()):
        util.ssh_keygen(sshd_clientkey_filepath())
        with open(sshd_authorized_keys_filepath(), "w") as fw:
            with open(sshd_clientpubkey_filepath(), "r") as fr:
                fw.write(fr.read())
    if not os.path.isfile(sshd_config_filepath()):
        with open(sshd_config_filepath(), "w") as f:
            f.write(sshd_config())

def ensure_init_machine(machine):
    os.makedirs(queue_dirpath(machine) , exist_ok=True)
    os.makedirs(worker_dirpath(machine), exist_ok=True)
    os.makedirs(job_dirpath(machine)   , exist_ok=True)
    try:
        atomic_counter.fetch(worker_counter_filepath(machine))
    except:
        atomic_counter.reset(worker_counter_filepath(machine))
    try:
        atomic_counter.fetch(job_counter_filepath(machine))
    except:
        atomic_counter.reset(job_counter_filepath(machine))
