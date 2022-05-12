import fcntl

def push(filename, entry):
    with open(filename, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(entry + "\n")

def pop(filename):
    result = None
    with open(filename, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        s = f.readlines()
        if len(s) > 0:
            result = s[0].split("\n")[0].rstrip("\x00")
            f.seek(0)
            f.writelines(s[1:])
            f.truncate()
    return result

if __name__ == "__main__":
    """
    A locked queue protected by flock.

    To test the behaviour, concurrently run this file on different nodes:
    machine1$ python3 -m kochi.locked_queue > a1
    machine2$ python3 -m kochi.locked_queue > a2

    To check:
    $ cat a1 a2 | sort -n | uniq | wc -l
    2000
    """
    import os
    n = 1000
    filename = "test.lock"
    hostname = os.uname()[1]
    for i in range(n):
        push(filename, "{} {}".format(i, hostname))
    for i in range(n):
        print(pop(filename))
