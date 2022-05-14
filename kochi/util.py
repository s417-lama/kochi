import subprocess
import pickle
import base64

def run_command_ssh(host, commands):
    subprocess.run("ssh -o LogLevel=QUIET -t {} '{}'".format(host, commands), shell=True)

def serialize(obj):
    return base64.b64encode(pickle.dumps(obj)).decode()

def deserialize(obj_str):
    return pickle.loads(base64.b64decode(obj_str.encode()))
