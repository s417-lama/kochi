import os
import subprocess
import time
import socket
import contextlib
import threading
import urllib.request
import webbrowser

@contextlib.contextmanager
def multiplexing_aux(dest):
    control_path = os.path.join(os.path.expanduser("~"), ".ssh", "kochi-%r@%h:%p")
    sshflags = ["-o", "ControlMaster=auto", "-o", "ControlPath={}".format(control_path), "-o", "ControlPersist=no"]
    try:
        subprocess.run(["ssh", *sshflags, "-O", "check", dest], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        with subprocess.Popen(["ssh", "-N", "-T", *sshflags, dest],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as p:
            try:
                yield sshflags
            finally:
                p.terminate()
    else:
        yield sshflags

@contextlib.contextmanager
def multiplexing(dest):
    try:
        subprocess.run(["ssh", "-O", "check", dest], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        with multiplexing_aux(dest) as sshflags:
            yield sshflags
    else:
        yield []

def establish_remote_forward(local_port, dest, sshflags):
    max_retry = 10
    retry_count = 0
    while True:
        try:
            return int(subprocess.run(["ssh", *sshflags, "-O", "forward", "-R", "0:localhost:{}".format(local_port), dest],
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding="utf-8", check=True).stdout.strip())
        except subprocess.CalledProcessError:
            if retry_count == max_retry:
                raise Exception("Could not establish remote port forwarding (max_retry={})".format(max_retry))
            retry_count += 1
            time.sleep(0.3)

def cancel_remote_forward(local_port, dest, sshflags):
    subprocess.run(["ssh", *sshflags, "-O", "cancel", "-R", "0:localhost:{}".format(local_port), dest],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

@contextlib.contextmanager
def remote_forward(local_port, dest):
    with multiplexing(dest) as sshflags:
        remote_port = establish_remote_forward(local_port, dest, sshflags)
        try:
            yield remote_port
        finally:
            cancel_remote_forward(local_port, dest, sshflags)

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", 0))
        return s.getsockname()[1]

def establish_local_forward(local_port, target_host, target_port, dest, sshflags):
    subprocess.run(["ssh", *sshflags, "-O", "forward", "-L", "{}:{}:{}".format(local_port, target_host, target_port), dest],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

def cancel_local_forward(local_port, target_host, target_port, dest, sshflags):
    subprocess.run(["ssh", *sshflags, "-O", "cancel", "-L", "{}:{}:{}".format(local_port, target_host, target_port), dest],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

@contextlib.contextmanager
def local_forward(dest, target_host, target_port):
    max_retry = 50
    retry_count = 0
    with multiplexing(dest) as sshflags:
        while True:
            local_port = get_free_port()
            try:
                establish_local_forward(local_port, target_host, target_port, dest, sshflags)
            except subprocess.CalledProcessError:
                if retry_count == max_retry:
                    raise Exception("Could not establish local port forwarding (max_retry={})".format(max_retry))
                retry_count += 1
            else:
                break
        try:
            yield local_port
        finally:
            cancel_local_forward(local_port, target_host, target_port, dest, sshflags)

def recv_until_close(s):
    bs = []
    while True:
        try:
            b = s.recv(1024)
        except:
            b = b""
        if len(b) == 0:
            break
        bs.append(b)
    return b"".join(bs)

def open_webbrowser_if_possible(host, port, path):
    url = "http://localhost:{}{}".format(port, path)
    try:
        with urllib.request.urlopen(url, timeout=1):
            pass
    except:
        return False
    else:
        print("Opening a browser for {}...".format(url))
        webbrowser.open(url)
        return True

@contextlib.contextmanager
def reverse_forward(dest):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", 0))
        s.listen()
        local_port =  s.getsockname()[1]
        with remote_forward(local_port, dest) as remote_port:
            def wait_for_invoke(e):
                nonlocal s
                conn, _addr = s.accept()
                with conn:
                    target_str = recv_until_close(conn).decode() # expect host:port
                    target_host, target_port = target_str.split(":")
                    with local_forward(dest, target_host, int(target_port)) as local_port:
                        browser_opened = False
                        while not e.wait(1):
                            if not browser_opened:
                                browser_opened = open_webbrowser_if_possible("localhost", local_port, "/")
            e = threading.Event()
            t = threading.Thread(target=wait_for_invoke, args=(e,))
            t.start()
            try:
                yield remote_port
            finally:
                e.set()

def invoke_reverse_forward(remote_port, target_host, target_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", remote_port))
        s.send("{}:{}".format(target_host, target_port).encode())
