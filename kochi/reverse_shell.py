import os
import threading
import signal
import tty
import pty
import select
import socket
import fcntl
import termios
import array
import secrets

def proxy_fd(conn, fd_in, fd_out):
    while True:
        rlist, _wlist, _xlist = select.select([conn, fd_in], [], [])
        if conn in rlist:
            try:
                data = conn.recv(1024)
            except:
                data = b""
            if data:
                os.write(fd_out, data)
            else:
                return
        if fd_in in rlist:
            try:
                data = os.read(fd_in, 1024)
            except:
                data = b""
            if data:
                conn.sendall(data)
            else:
                return

def accept_with_token(sock, token):
    while True:
        conn, _addr = sock.accept()
        token_received = conn.recv(1024).decode()
        if token_received == token:
            return conn
        else:
            conn.close()

def wait_to_connect(host, port, **opts):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        token = opts.get("token", secrets.token_hex())
        if opts.get("on_listen_hook"):
            opts["on_listen_hook"](*s.getsockname(), token)
        with accept_with_token(s, token) as conn1:
            with accept_with_token(s, token) as conn2:
                def send_win_size():
                    nonlocal conn2
                    buf = array.array("h", [0] * 4)
                    fcntl.ioctl(0, termios.TIOCGWINSZ, buf, 1)
                    conn2.sendall(buf.tobytes())
                signal.signal(signal.SIGWINCH, lambda signum, frame: send_win_size())
                send_win_size()
                mode = tty.tcgetattr(0)
                try:
                    tty.setraw(0)
                    proxy_fd(conn1, 0, 1)
                finally:
                    tty.tcsetattr(0, tty.TCSAFLUSH, mode)

def launch_shell(host, port, token):
    def watch_window_size(fd):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.send(token.encode())
            while True:
                buf = array.array("h")
                try:
                    data = s.recv(1024)
                except:
                    data = b""
                if data:
                    buf.frombytes(data)
                else:
                    return
                fcntl.ioctl(fd, termios.TIOCSWINSZ, buf, 0)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.send(token.encode())
        pid, fd = pty.fork()
        if pid == 0:
            argv = [os.environ.get("SHELL", "/bin/sh")]
            os.execlp(argv[0], *argv)
        t = threading.Thread(target=watch_window_size, args=(fd,))
        t.start()
        proxy_fd(s, fd, fd)
        os.waitpid(pid, 0)

if __name__ == "__main__":
    """
    terminal1$ python3 -m kochi.reverse_shell client
    terminal2$ python3 -m kochi.reverse_shell server
    """
    import sys
    host = "localhost"
    port = 8888
    token = "test_token"
    if sys.argv[1] == "client":
        wait_to_connect(host, port, token=token)
    elif sys.argv[1] == "server":
        launch_shell(host, port, token)
