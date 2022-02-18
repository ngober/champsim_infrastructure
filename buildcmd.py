import os
import os.path
import glob
import random
import argparse
import itertools
import functools

def outfilename(output_directory, *trace_files):
    crunch_names = tuple(os.path.splitext(os.path.splitext(os.path.split(tf)[1])[0])[0] for tf in trace_files)
    return os.path.join(output_directory, '-'.join(crunch_names) + '.txt')

def unpack(elements):
    for elem in itertools.chain.from_iterable(map(glob.iglob, elements)):
        if os.path.isfile(elem):
            yield elem
        else:
            for b,_,f in os.walk(elem):
                yield from (os.path.join(b,t) for t in f)

def sample_iter(population, k):
    while True: # islice this generator
        yield random.sample(population, k)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Creates a sequence of execution commands for using ChampSim on a compute cluster")

    parser.add_argument('--champsim-executable', action='append',
            help='The ChampSim executable to run. When specified multiple times, zips together with --output-prefix and runs once for each trace in the population')
    parser.add_argument('--output-prefix', action='append',
            help='The output directory where the results will be written.')

    parser.add_argument('--trace-prefix', action='append',
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


    div = '" "' # The separator for file names
    for x,trace_file in itertools.product(zip(args.champsim_executable, args.output_prefix), itertools.islice(population, size)):
        champsim_executable, output_prefix = x
        output_file = outfilename(output_prefix, *trace_file)
        print(f'"{champsim_executable}" -w{args.warmup_instructions} -i{args.simulation_instructions} "{div.join(trace_file)}" > "{output_file}"')

