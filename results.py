import argparse
import re
import itertools
import functools
import operator
import os
import glob

import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gmean

class pattern:
    def __init__(self, regex, matcher, aggregator):
        self.regex = re.compile(regex)
        self.matcher = matcher
        self.aggregator = aggregator

    # Return a functor over an iterable that matches each line, filters out Nones, projects the match, then aggregates
    def __call__(self, rfp):
        return self.aggregator(map(self.matcher, filter(None, map(lambda x: self.regex.match(x), rfp))))

trace_file_pat = pattern(
        r'^CPU (?P<index>\d+) runs (?P<tracename>[-./\w\d]+)$',
        lambda match: os.path.basename(match['tracename']),
        lambda results: pd.DataFrame(results, columns=['trace_name'])
    )

cpu_stats_pat = pattern(
        r'^CPU (?P<cpu>\d+) cumulative IPC: \d+\.?\d* instructions: (?P<instructions>\d+) cycles: (?P<cycles>\d+)$',
        operator.methodcaller('groupdict',0),
        lambda results : pd.DataFrame.from_records(results, columns=['cpu', 'instructions', 'cycles']).astype('int64')
    )

def settle(it):
    cpu_num = itertools.count()
    retval = []
    chunk = {}
    for k,v in it:
        if k in chunk:
            #retval['cpu' + str(next(cpu_num))] = dict(itertools.chain.from_iterable((((k, 'hit'), v['hit']), ((k, 'miss'), v['miss'])) for k,v in chunk.items()))
            retval.append(dict(itertools.chain.from_iterable(((k+'_hit', v['hit']), (k+'_miss', v['miss'])) for k,v in chunk.items()), cpu=next(cpu_num)))
            chunk = {}
        chunk.update({k: { 'hit': v[0], 'miss': v[1] }})
    retval = pd.DataFrame(retval, columns=['LOAD_hit', 'LOAD_miss', 'RFO_hit', 'RFO_miss', 'PREFETCH_hit', 'PREFETCH_miss', 'WRITEBACK_hit', 'WRITEBACK_miss', 'TRANSLATION_hit', 'TRANSLATION_miss']).fillna(0)
    retval['TOTAL_hit'] = retval[[c for c in retval.columns if not c.startswith('PREFETCH') and c.endswith('hit')]].sum(axis=1)
    retval['TOTAL_miss'] = retval[[c for c in retval.columns if not c.startswith('PREFETCH') and c.endswith('miss')]].sum(axis=1)
    return retval

def cache_stats_pat(cache):
    return pattern(
        '^' + cache + r' (?P<type>LOAD|RFO|PREFETCH|WRITEBACK|TRANSLATION)\s+ACCESS:\s+\d+  HIT:\s+(?P<hit>\d+)  MISS:\s+(?P<miss>\d+)$',
        lambda m: (m['type'], (int(m['hit']), int(m['miss']))),
        lambda results : settle(results)
    )

def pref_stats_pat(cache):
    return pattern(
        '^' + cache + r' PREFETCH  REQUESTED:\s+(?P<prefetch_requested>\d+)  ISSUED:\s+(?P<prefetch_issued>\d+)  USEFUL:\s+(?P<prefetch_useful>\d+)  USELESS:\s+(?P<prefetch_useless>\d+)$',
        operator.methodcaller('groupdict',0),
        lambda results : pd.DataFrame.from_records(results, columns=['prefetch_requested', 'prefetch_issued', 'prefetch_useful', 'prefetch_useless']).astype('int64')
    )

amat_pat = pattern(
        r'^(\S+) AVERAGE MISS LATENCY: (\d+\.?\d*) cycles$',
        lambda m : print('AMAT', m.groups),
        lambda _: None
    )

dram_rq_pat = pattern(
        r'^ RQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)$',
        lambda m : print('DRAM', m.groups),
        lambda _: None
    )

dram_wq_pat = pattern(
        r'^ WQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)  FULL:\s+(\d+)$',
        lambda m : print('DRAM', m.groups),
        lambda _: None
    )

dram_dbus_pat = pattern(
        r'^ DBUS AVG_CONGESTED_CYCLE:\s+(\d+\.?\d*)$',
        lambda m : print('DRAM', m.groups),
        lambda _: None
    )

# Return a functor over an iterable that broadcasts the iterable over each functor passed to it
# FIXME is there a way to do this without hoisting the entire file into memory? Does it matter?
def broadcast(*parsers):
    return lambda rfp : tuple(p(t) for t,p in zip(itertools.tee(rfp, len(parsers)), parsers))

def expand(fname):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(fname)))

def unpack(elem):
    elem = expand(elem)
    if os.path.isfile(elem):
        yield elem
    else:
        for b,_,f in os.walk(elem):
            yield from (os.path.join(b,t) for t in f)

# Parse the file with the given parsers
def parse_file(parsers, fname):
    with open(fname) as rfp:
        return parsers(rfp)

# Attach the cpu number to the column labels
def collapse(record):
    record.set_index('cpu')
    record = record.unstack()
    record.index = [x + '_' + str(y) for x,y in record.index]

    return record

# Get the instructions per cycle
def get_ipc(file):
    parsers = broadcast(
        trace_file_pat,
        cpu_stats_pat
    )

    parse_result = parse_file(parsers, file)
    parse_result = (parse_result[0], parse_result[1].iloc[:len(parse_result[0])]) # Select full-simulation stats. This is a hack and I hate it, but I need to improve ChampSim's output first
    result = parse_result[0].join(parse_result[1]) # Join trace names to instruction and cycle counts
    result['ipc'] = result['instructions'] / result['cycles']

    return collapse(result)

# Get hit/miss counts for each type for the given caches
def get_cache_stats(file, cache):
    parsers = broadcast(
        trace_file_pat,
        cache_stats_pat(cache),
        pref_stats_pat(cache)
    )

    parse_result = parse_file(parsers, file)
    print(parse_result[2])
    parse_result = (parse_result[0], parse_result[1], parse_result[2].iloc[0] if not parse_result[2].empty else pd.Series(index=parse_result[2].columns))
    parse_result = (parse_result[0], parse_result[1].iloc[:len(parse_result[0])], parse_result[2]) # Select full-simulation stats. This is a hack and I hate it, but I need to improve ChampSim's output first
    result = parse_result[0].join(parse_result[1]) # Join trace names to instruction and cycle counts

    # manually collapse
    result = result.unstack()
    result.index = [x + '_' + str(y) for x,y in result.index]

    print(result)
    print(parse_result[2])
    result = pd.concat([result, parse_result[2]])
    result['TOTAL_hit'] = result[[c for c in result.index if c.startswith('TOTAL_hit')]].sum()
    result['TOTAL_miss'] = result[[c for c in result.index if c.startswith('TOTAL_miss')]].sum()
    return result

# Calculate the baseline and improved data for a given data point
def get_base_test_pair(func, base_files, test_files):
    return func(base_files), func(test_files)

def get_records(func):
    return lambda files: pd.DataFrame([func(f) for f in files])

# Calculate the speedup
def get_speedup(bases, tests):
    # Get IPC results
    base_ipc, test_ipc = get_base_test_pair(get_records(get_ipc), itertools.chain(*map(unpack, bases)), itertools.chain(*map(unpack, tests)))

    # Index by trace names
    col_index_names = [c for c in test_ipc.columns if c.startswith('trace_name')]
    base_ipc.set_index(col_index_names, inplace=True)
    test_ipc.set_index(col_index_names, inplace=True)

    # Divide
    ipc_index_names = [c for c in test_ipc.columns if c.startswith('ipc')]
    speedup = test_ipc[ipc_index_names].div(base_ipc[ipc_index_names])

    # Name the columns something useful
    speedup.columns = ['Core ' + c.split('_')[-1] for c in speedup.columns]

    return speedup

def get_diff_cache_change(bases, tests, cache):
    eval_func = get_records(functools.partial(get_cache_stats, cache=cache))
    base_result, test_result = get_base_test_pair(eval_func, itertools.chain(*map(unpack, bases)), itertools.chain(*map(unpack, tests)))

    col_index_names = [c for c in test_result.columns if c.startswith('trace_name')]
    test_result.set_index(col_index_names, inplace=True)
    base_result.set_index(col_index_names, inplace=True)

    return test_result.sub(base_result)

# Calculate the percent change in cache accesses
def get_pct_cache_change(bases, tests, cache):
    eval_func = get_records(functools.partial(get_cache_stats, cache=cache))
    base_result, test_result = get_base_test_pair(eval_func, itertools.chain(*map(unpack, bases)), itertools.chain(*map(unpack, tests)))

    col_index_names = [c for c in test_result.columns if c.startswith('trace_name')]
    test_result.set_index(col_index_names, inplace=True)
    base_result.set_index(col_index_names, inplace=True)

    return test_result.div(base_result)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()

    # File lists
    argparser.add_argument('--base', nargs='*')
    argparser.add_argument('files', nargs='+')
    argparser.add_argument('-o', '--output')

    # Data selectors
    argparser.add_argument('--speedup', action='store_true')
    argparser.add_argument('--cache')
    args = argparser.parse_args()

    if args.speedup:
        if args.base:
            speedup = get_speedup(args.base, args.files)

            if args.output is None:
                (speedup-1).plot.bar(bottom=1, ylabel='Speedup')
                plt.hlines(1, -1, len(speedup.index), colors='black')
                plt.show()
            else:
                speedup.to_csv(args.output)
        else:
            print(get_records(get_ipc)(itertools.chain(*map(unpack, args.files))))

    elif args.cache is not None:
        if args.base:
            test_results = get_pct_cache_change(args.base, args.files, args.cache)
            axs = (test_results[['TOTAL_hit', 'TOTAL_miss']]-1).plot.bar(bottom=1, subplots=True, ylabel='Percent Change')
            for ax in axs:
                ax.hlines(1,-1, len(test_results.index), colors='black', linewidth=1)
            plt.show()
        else:
            eval_func = lambda files: pd.DataFrame([get_cache_stats(f, cache=args.cache) for f in files])
            test_results = eval_func(itertools.chain(*map(unpack, args.files)))

            print(test_results)

