from multiprocessing import Process

from app import spawn
from start import start
from stop import stop
import time
import os

if __name__ == '__main__':
    try:
        os.remove("thor.log")
    except:
        pass

    sys_base = "multiprocTCPBase"
    # sys_base = "simpleSystemBase"
    start(sys_base)
    time.sleep(20)
    procs = []

    for i in range(0, 10):
        proc = Process(target=spawn, args=(sys_base, i+1))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    print("All apps finished")

    stop(sys_base)
