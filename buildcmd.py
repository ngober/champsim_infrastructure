import os
import os.path
import glob
import random
import argparse
import itertools
import functools

def outfilename(output_directory, *trace_files):
    crunch_names = tuple(os.path.splitext(os.path.splitext(os.path.split(tf)[1])[0])[0] for tf in trace_files)
    return os.path.abspath(os.path.join(output_directory, '-'.join(crunch_names) + '.txt'))

def expand(fname):
    yield from glob.iglob(os.path.abspath(os.path.expanduser(os.path.expandvars(fname))))

def unpack(elements):
    for elem in itertools.chain.from_iterable(map(expand, elements)):
        if os.path.isfile(elem):
            yield elem
        else:
            for b,_,f in os.walk(elem):
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Creates a sequence of execution commands for using ChampSim on a compute cluster")

    parser.add_argument('--format', choices=['sh'], default='sh',
            help='The format of the resulting output.')

    parser.add_argument('--champsim-executable', action='append', required=True,
            help='The ChampSim executable to run. When specified multiple times, zips together with --output-prefix and runs once for each trace in the population')
    parser.add_argument('--output-prefix', action='append',
            help='The output directory where the results will be written.')

    parser.add_argument('--trace-prefix', action='append', required=True,
            help='The location of traces that comprise the population. When specified multiple times, all populations are merged.')

    parser.add_argument('-n', type=int,
            help='The size of the population to use. Defaults to all traces found')

    parser.add_argument('-k', type=int, default=1,
            help='The number of traces to use per simulation. If k > 1 and n is specified, the traces will be randomly selected')

    parser.add_argument('--warmup-instructions', type=int, default=400000000)
    parser.add_argument('--simulation-instructions', type=int, default=1000000000)

    args = parser.parse_args()

    population = tuple(unpack(args.trace_prefix))

    size = args.n or (len(population) * args.k)

    if (args.k > 1):
        population = sample_iter(population, k=args.k)
    else:
        population = map(lambda x: list((x,)), population)

    cmd_iter = buildcmd_iter(args.champsim_executable, args.output_prefix or ['.'], itertools.islice(population, size), args.warmup_instructions, args.simulation_instructions)

    if args.format is 'sh':
        print(sh_out(cmd_iter))

