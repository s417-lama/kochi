import os
import sys
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

def proxy_fd(in_fds, out_fds):
    while True:
        rlist, _wlist, _xlist = select.select(in_fds, [], [])
        for in_fd, out_fd in zip(in_fds, out_fds):
            if in_fd in rlist:
                try:
                    data = os.read(in_fd, 1024)
                except:
                    data = b""
                if data:
                    while data:
                        n = os.write(out_fd, data)
                        data = data[n:]
                else:
                    return

def recv_exact(s, n):
    n_left = n
    bs = []
    while n_left > 0:
        b = s.recv(n_left)
        if len(b) == 0:
            raise EOFError
        n_left -= len(b)
        bs.append(b)
    return b"".join(bs)

def accept_with_token(sock, token):
    while True:
        conn, _addr = sock.accept()
        try:
            token_received = recv_exact(conn, len(token.encode())).decode()
        except:
            conn.close()
        else:
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
                    fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, buf, 1)
                    conn2.sendall(buf.tobytes())
                signal.signal(signal.SIGWINCH, lambda signum, frame: send_win_size())
                send_win_size()
                try:
                    mode = tty.tcgetattr(sys.stdin.fileno())
                    tty.setraw(sys.stdin.fileno())
                except:
                    reset_tty_mode = False
                else:
                    reset_tty_mode = True
                try:
                    proxy_fd([conn1.fileno(), sys.stdin.fileno()], [sys.stdout.fileno(), conn1.fileno()])
                finally:
                    if reset_tty_mode:
                        tty.tcsetattr(sys.stdin.fileno(), tty.TCSAFLUSH, mode)

def launch_shell(host, port, token):
    def watch_window_size(fd):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.send(token.encode())
            while True:
                buf = array.array("h")
                try:
                    data = recv_exact(s, len(array.array("h", [0] * 4).tobytes()))
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
        proxy_fd([s.fileno(), fd], [fd, s.fileno()])
        os.waitpid(pid, 0)

if __name__ == "__main__":
    """
    terminal1$ python3 -m kochi.reverse_shell client
    terminal2$ python3 -m kochi.reverse_shell server
    """
    host = "localhost"
    port = 8888
    token = "test_token"
    if sys.argv[1] == "client":
        wait_to_connect(host, port, token=token)
    elif sys.argv[1] == "server":
        launch_shell(host, port, token)
