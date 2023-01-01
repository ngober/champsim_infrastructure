import os
import os.path
import glob
import random
import argparse
import itertools
import functools
import operator
import json
import re

def outfilename(output_directory, *trace_files):
    crunch_names = tuple(os.path.splitext(os.path.splitext(os.path.split(tf)[1])[0])[0] for tf in trace_files)
    return os.path.abspath(os.path.join(output_directory, '-'.join(crunch_names)))

def expand(fname):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(fname)))

def unpack(elem, recursive):
    if os.path.isfile(elem):
        yield elem
    else:
        it = os.walk(elem)
        if not recursive:
            it = itertools.islice(it, 1)

        for b,_,f in it:
            yield from (os.path.join(b,t) for t in f)

#def sample_iter(population, k):
    #results = []
    #it = iter(population)
    #for _ in range(k):
        #results.append(next(it))
    #random.shuffle(results)
    #for i, v in enumerate(it, k):
        #r = random.randint(0,i)
        #if r < k:
            #results[r] = v
    #return results

def sample_iter(population, n, k):
    results = set()
    while len(results) < n:
        random.shuffle(population)
        results.add(tuple(population[:k]))
    return list(results)

def buildcmd_iter(executable, output_prefix, population, warmup, simulation):
    yield from ((*x, p, warmup, simulation) for x,p in itertools.product(zip(executable, output_prefix), population))

def sh_out(cmd_iter, use_json=False):
    if use_json:
        fmtstr = 'mkdir -p $(dirname "{output_file}"); "{executable}" -w{warmup} -i{simulation} --json="{output_file}.json" -- "{traces}" > "{output_file}.stdout.txt"'
    else:
        fmtstr = 'mkdir -p $(dirname "{output_file}"); "{executable}" -w{warmup} -i{simulation} -- "{traces}" > "{output_file}.txt"'

    return '\n'.join(fmtstr.format(
            executable=champsim_executable,
            warmup=warmup,
            simulation=simulation,
            traces='" "'.join(trace_file),
            output_file=outfilename(output_prefix, *trace_file)
        ) for champsim_executable, output_prefix, trace_file, warmup, simulation in cmd_iter)

def py_out(cmd_iter, use_json=False):
    if use_json:
        out_tuple = ((outfilename(prefix, *traces)+'.txt', champsim_executable, '-w'+str(warmup), '-i'+str(simulation), '--json='+outfilename(prefix, *traces)+'.json', '--', *traces) for champsim_executable, prefix, traces, warmup, simulation in cmd_iter)
    else:
        out_tuple = (('/dev/null', champsim_executable, '-w'+str(warmup), '-i'+str(simulation), '--', *traces) for champsim_executable, prefix, traces, warmup, simulation in cmd_iter)
    return '''runs = [
'''+',\n'.join('  '+str(tup) for tup in out_tuple)+'''
]

import subprocess, time, collections, os, multiprocessing, itertools
from timeit import default_timer as timer
from datetime import timedelta

start = timer()
def begin(fname, *args):
    t = timer()
    print('[', timedelta(seconds=t - start), ']', 'Start', *args)
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    f = open(fname, 'wt')
    return f, t, subprocess.Popen(args, stdout=f, stderr=f)

def check_finish(f, t, p):
    retval = p.poll()
    if retval is not None:
        f.close()
        print('[', timedelta(seconds=timer() - t), ']', 'Completed', os.path.basename(p.args[0]), 'with exit code', retval)
    return retval

heartbeat_period = 15 # seconds
num_cpus = multiprocessing.num_cpus()

processargs = collections.deque(runs)
active_processes = []
while processargs or active_processes:
    unfinished = [(check_finish(*p) is None) for p in active_processes]
    active_processes = list(itertools.compress(active_processes, unfinished))

    while processargs and len(active_processes) < num_cpus:
        active_processes.append(begin(*processargs[0]))
        processargs.popleft()

    print('[', timedelta(seconds=timer() - start), ']', 'Running:', len(active_processes), 'Finished:', len(runs)-len(active_processes)-len(processargs))
    time.sleep(heartbeat_period)
'''

def get_population(population, n=None, k=1):
    if n is None:
        yield from itertools.permutations(population, k)
    else:
        yield from sample_iter(list(population), n, k)

def impl_get_population_part(directory, match='.*', recursive=True, invert_match=False):
    reg = re.compile(match)

    if invert_match:
        f = lambda x: not reg.search(x)
    else:
        f = reg.search

    for d in glob.iglob(expand(directory)):
        yield from filter(f, unpack(d, recursive))

def get_population_part(elem):
    if isinstance(elem, dict):
        return impl_get_population_part(**elem)
    else:
        return impl_get_population_part(elem)

def parse_json(f):
    if not isinstance(f,list):
        f = [f]
    for record in f:
        if not isinstance(record['test'], list):
            record['test'] = [record['test']]
        population = get_population(list(itertools.chain(*map(get_population_part, record['traces']))), n=record.get("count"), k=record.get('width', 1))
        executables = ({'name': 'base', 'executable': record['base']}, *record['test'])
        for e,p in itertools.product(executables, population):
            sim_instrs = record.get('simulation_instructions', 1000000000)
            warm_instrs = record.get('warmup_instructions', int(0.2*sim_instrs))
            output_prefix = expand(os.path.join(record.get('output_prefix', '.'), e['name'])) # create a subdirectory for results with the given name
            yield expand(e['executable']), output_prefix, p, warm_instrs, sim_instrs

def parse_file(fname):
    with open(fname, 'rt') as rfp:
        f = json.load(rfp)
    yield from parse_json(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Creates a sequence of execution commands for using ChampSim on a compute cluster")

    parser.add_argument('--format', choices=['sh','python'], default='sh',
            help='The format of the resulting output.')

    parser.add_argument('--use-json', action='store_true',
            help='If specified, use experimental ChampSim JSON output')

    parser.add_argument('files', nargs='+',
            help='JSON files describing the build')

    args = parser.parse_args()

    cmd_iter = itertools.chain(*map(parse_file, args.files))

    if args.format == 'sh':
        output = sh_out(cmd_iter, use_json=args.use_json)
    if args.format == 'python':
        output = py_out(cmd_iter, use_json=args.use_json)

    print(output)

schema = '''
[
  {
    "base": "~/champsim/bin/champsim",
    "test": [
      {
        "name": "test_a",
        "executable": "~/champsim/bin/test_a/champsim"
      },
      {
        "name": "test_b",
        "executable": "~/champsim/bin/test_b/champsim"
      }
    ],

    "traces": [
      {
        "directory": "~/traces/dpc3",
        "match": "mcf",
        "invert_match": true
      }
    ],

    "count": 10,
    "width": 4
  }
]
'''

