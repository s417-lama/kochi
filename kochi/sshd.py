import os
import sys
import subprocess
import contextlib
import shutil
import psutil

from . import util
from . import settings

def ssh_client_config(ip_address_candidates, port):
    configs = [util.dedent("""
        Host kochi_worker
          Port {}
          IdentityFile {}
          StrictHostKeyChecking no
        """.format(port, settings.sshd_clientkey_filepath()))]
    for ip in ip_address_candidates:
        configs.append(util.dedent("""
            Match exec "timeout 0.1 nc -z {0} {1}"
              HostName {0}
            """.format(ip, port), indent=2))
    return "\n".join(configs)

@contextlib.contextmanager
def sshd(machine, worker_id):
    os.makedirs(settings.sshd_var_run_dirpath(machine, worker_id), exist_ok=True)
    sshd_cmd = shutil.which("sshd")
    if sshd_cmd:
        port = settings.sshd_port()
        with subprocess.Popen([sshd_cmd, "-f", settings.sshd_config_filepath(), "-D",
                               "-o", "PidFile={}/sshd.pid".format(settings.sshd_var_run_dirpath(machine, worker_id)),
                               "-p", str(port)], start_new_session=True) as sshd:
            with open(settings.sshd_client_config_filepath(machine, worker_id), "w") as f:
                f.write(ssh_client_config(util.get_ip_address_candidates(), port))
            try:
                yield
            finally:
                for p in psutil.Process(sshd.pid).children(recursive=True):
                    p.kill()
                sshd.kill()
    else:
        print("Warning: sshd is not available, so 'inspect' for this worker will not work.")
        yield

def login_to_machine(machine, worker_id):
    config_path = settings.sshd_client_config_filepath(machine, worker_id)
    if not os.path.isfile(config_path):
        print("sshd is not running with worker {} on machine {}.".format(worker_id, machine), file=sys.stderr)
        exit(1)
    try:
        subprocess.run(["ssh", "-F", config_path, "kochi_worker"], check=True)
    except subprocess.CalledProcessError:
        print("ssh to worker {} on machine {} failed.".format(worker_id, machine), file=sys.stderr)
        exit(1)

def ensure_init():
    os.makedirs(settings.sshd_dirpath()    , exist_ok=True)
    os.makedirs(settings.sshd_etc_dirpath(), exist_ok=True)
    if not os.path.isfile(settings.sshd_hostkey_filepath()):
        util.ssh_keygen(settings.sshd_hostkey_filepath())
    if not os.path.isfile(settings.sshd_clientkey_filepath()):
        util.ssh_keygen(settings.sshd_clientkey_filepath())
        with open(settings.sshd_authorized_keys_filepath(), "w") as fw:
            with open(settings.sshd_clientpubkey_filepath(), "r") as fr:
                fw.write(fr.read())
        os.chmod(settings.sshd_authorized_keys_filepath(), 0o600)
    if not os.path.isfile(settings.sshd_config_filepath()):
        with open(settings.sshd_config_filepath(), "w") as f:
            f.write(settings.sshd_config())
