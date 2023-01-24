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

def out_id(*trace_files):
    return '-'.join(os.path.splitext(os.path.splitext(os.path.split(tf)[1])[0])[0] for tf in trace_files)

def out_file(output_directory, build_id):
    return os.path.abspath(os.path.join(output_directory, build_id))

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
            output_prefix=output_prefix,
            json_file=out_file(output_prefix, out_id(*trace_file)),
            output_file=out_file(output_prefix, out_id(*trace_file))
        ) for champsim_executable, output_prefix, trace_file, warmup, simulation in cmd_iter)

def py_out(cmd_iter, use_json=False):
    if use_json:
        out_tuple = ((out_file(prefix, out_id(*traces))+'.stdout.txt', champsim_executable, '-w'+str(warmup), '-i'+str(simulation), '--json='+out_file(prefix, out_id(*traces))+'.json', '--', *traces) for champsim_executable, prefix, traces, warmup, simulation in cmd_iter)
    else:
        out_tuple = ((out_file(prefix, out_id(*traces))+'.stdout.txt', champsim_executable, '-w'+str(warmup), '-i'+str(simulation), '--', *traces) for champsim_executable, prefix, traces, warmup, simulation in cmd_iter)
    return '''import importlib.util

spec = importlib.util.spec_from_file_location('champsim_runner', '{module_file_name}')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

runner.run([
{outstring}
])
'''.format(
    module_file_name=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'champsim_runner.py'),
    outstring=',\n'.join('  '+str(tup) for tup in out_tuple)
  )

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

def wrap_list(x):
    if not isinstance(x,list):
        return [x]
    else:
        return x

def parse_json(f):
    for record in wrap_list(f):
        population = get_population(itertools.chain(*map(get_population_part, record['traces'])), n=record.get("count"), k=record.get('width', 1))
        executables = ({'name': 'base', 'executable': record['base']}, *wrap_list(record.get('test', [])))

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
            help='The format of the resulting output.')

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

