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
    return os.path.abspath(os.path.join(output_directory, '-'.join(crunch_names) + '.txt'))

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


def sample_iter(population, k):
    while True: # islice this generator
        yield random.sample(population, k)

def buildcmd_iter(executable, output_prefix, population, warmup, simulation):
    yield from ((*x, p, warmup, simulation) for x,p in itertools.product(zip(executable, output_prefix), population))

def sh_out(cmd_iter):
    return '\n'.join('"{executable}" -w{warmup} -i{simulation} "{traces}" > "{output_file}"'.format(
            executable=champsim_executable,
            warmup=warmup,
            simulation=simulation,
            traces='" "'.join(trace_file),
            output_file=outfilename(output_prefix, *trace_file)
        ) for champsim_executable, output_prefix, trace_file, warmup, simulation in cmd_iter)

def get_population(population, n=None, k=1):
    if n is None:
        yield from itertools.permutations(population, k)
    else:
        pop = tuple(population)
        pop_size = functools.reduce(operator.mul, range(len(pop), len(pop)-k, -1))
        targets = random.sample(range(pop_size), k=n)
        yield from itertools.compress(itertools.permutations(pop, k), map(targets.__contains__, itertools.count()))

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
        f = list((f,))
    for record in f:
        if not isinstance(record['test'], list):
            record['test'] = list((record['test'],))
        population = get_population(list(itertools.chain.from_iterable(map(get_population_part, record['traces']))), n=record.get("count"), k=record.get('width', 1))
        executables = ({'name': 'base', 'executable': record['base']}, *record['test'])
        for e,p in itertools.product(executables, population):
            yield expand(e['executable']), expand(os.path.join(record.get('output_prefix', '.'), e['name'])), p, record.get('warmup_instructions', 40000000), record.get('simulation_instructions', 1000000000)

def parse_file(fname):
    with open(fname, 'rt') as rfp:
        f = json.load(rfp)
    yield from parse_json(f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Creates a sequence of execution commands for using ChampSim on a compute cluster")

    parser.add_argument('--format', choices=['sh'], default='sh',
            help='The format of the resulting output.')

    parser.add_argument('files', nargs='+',
            help='JSON files describing the build')

    args = parser.parse_args()

    cmd_iter = itertools.chain.from_iterable(map(parse_file, args.files))

    if args.format == 'sh':
        output = sh_out(cmd_iter)

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

