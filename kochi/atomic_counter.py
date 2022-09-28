import fcntl

def reset(filename, init_value=0):
    with open(filename, "w+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.seek(0)
        f.write(str(init_value))
        f.truncate()

def fetch(filename):
    with open(filename, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        return int(f.read())

def fetch_and_add(filename, inc):
    with open(filename, "r+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        count = int(f.read())
        f.seek(0)
        f.write(str(count + inc))
        f.truncate()
        return count

if __name__ == "__main__":
    """
    Atomic counter protected by flock.

    To test the behaviour:

    1. reset the counter
    $ python3 -c 'from kochi.atomic_counter import reset; reset("test.lock")'

    2. concurrently run this file on different nodes
    machine1$ python3 -m kochi.atomic_counter > a1
    machine2$ python3 -m kochi.atomic_counter > a2

    3. check the result
    $ cat a1 a2 | sort -n | uniq | wc -l
    2000
    $ cat a1 a2 | sort -n | uniq | head -1
    0
    $ cat a1 a2 | sort -n | uniq | tail -1
    1999
    """
    n = 1000
    filename = "test.lock"
    for i in range(n):
        print(fetch_and_add(filename, 1))
