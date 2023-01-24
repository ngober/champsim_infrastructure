import subprocess
import time
import collections
import os
import sys
import itertools
from timeit import default_timer as timer
from datetime import timedelta

def begin(fname, *args):
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    f = open(fname, 'wt')
    return f, subprocess.Popen(args, stdout=f, stderr=f)

def check_finish(f, p):
    retval = p.poll()
    if retval is not None:
        f.close()
    return retval

def run(runs, *, num_cpus=len(os.sched_getaffinity(0))):
  start = timer()
  processargs = collections.deque(runs)
  active_processes = []
  while processargs or active_processes:
      sys.stdout.write('\r')
      unfinished = [(check_finish(*p) is None) for p in active_processes]
      active_processes = list(itertools.compress(active_processes, unfinished))

      while processargs and len(active_processes) < num_cpus:
          active_processes.append(begin(*processargs[0]))
          processargs.popleft()

      sys.stdout.write('{:20} ({}/{}/{}) [{:30s}]'.format(
        str(timedelta(seconds=timer() - start)),
        len(runs)-len(active_processes)-len(processargs),
        len(active_processes),
        len(processargs),
        '='*(30*int(1-(len(active_processes)+len(processargs))/len(runs)))
      ))
      sys.stdout.flush()
      time.sleep(1)
  sys.stdout.write('\n')

