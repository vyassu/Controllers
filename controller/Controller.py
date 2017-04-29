#!/usr/bin/env python
from controller.framework.CFx import CFX
import psutil
import time


# Function checks the system process table for Tincan process
def checktincanstate():
    for process in psutil.process_iter():
        if type(process) is str:
            if process.find("tincan") != -1 or process == "ipop-tincan":
                return True
        else:
            if process.name().find("tincan") != -1 or process.name() == "ipop-tincan":
                return True
    return False


def main():
    stime = time.time()
    # Loop till Tincan not in running state

    while checktincanstate() is False:
        if time.time()-stime > 10:
            print("IPOP Tincan not started.!!")
            stime = time.time()
    time.sleep(10)
    cfx = CFX()
    cfx.initialize()
    cfx.waitForShutdownEvent()
    cfx.terminate()

if __name__ == "__main__":
    main()
