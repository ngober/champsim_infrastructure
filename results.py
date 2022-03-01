import argparse
import re
import itertools
import functools
import os
import glob

import pandas as pd

trace_file_pat = re.compile(r'^CPU (?P<index>\d+) runs (?P<tracename>[-./\w\d]+)$')

def trace_file_proj(match):
    return os.path.basename(match['tracename'])

def trace_file_agg(agg, result):
    if len(agg) > 0:
        return agg
    return result

cpu_stats_pat = re.compile(r'^CPU (\d+) cumulative IPC: \d+\.?\d* instructions: (?P<instructions>\d+) cycles: (?P<cycles>\d+)$')

def cpu_stats_proj(match):
    return pd.DataFrame.from_records([{k:int(v) for k,v in match.groupdict(0).items()}], index=[int(match[1])])

def cpu_stats_agg(agg, result):
    return pd.concat([agg, result])

cache_stats_pat = re.compile(r'^(?P<name>\S+) (?P<type>LOAD|RFO|PREFETCH|TRANSLATION)\s+ACCESS:\s+\d+  HIT:\s+(?P<hits>\d+)  MISS:\s+(?P<misses>\d+)$')

def cache_stats_proj(match):
    return dict([((match['name'], match['type'].lower(), 'hit'), int(match['hits'])), ((match['name'], match['type'].lower(), 'miss'), int(match['misses']))])

def cache_stats_agg(agg, result):
    agg.update(result)
    return agg

pref_stats_pat = re.compile(r'^(\S+) PREFETCH  REQUESTED:\s+(\d+)  ISSUED:\s+(\d+)  USEFUL:\s+(\d+)  USELESS:\s+(\d+)$')

def pref_stats_proj(match):
    print('AMAT', m.groups)

def pref_stats_agg(agg, result):
    return agg

amat_pat = re.compile(r'^(\S+) AVERAGE MISS LATENCY: (\d+\.?\d*) cycles$')

def amat_proj(match):
    print('AMAT', m.groups)

def amat_agg(agg, result):
    return agg

dram_rq_pat = re.compile(r'^ RQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)$')

def dram_rq_proj(match):
    print('DRAM', m.groups)

def dram_rq_agg(agg, result):
    return agg

dram_wq_pat = re.compile(r'^ WQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)  FULL:\s+(\d+)$')

def dram_wq_proj(match):
    print('DRAM', m.groups)

def dram_wq_agg(agg, result):
    return agg

dram_dbus_pat = re.compile(r'^ DBUS AVG_CONGESTED_CYCLE:\s+(\d+\.?\d*)$')

def dram_dbus_proj(match):
    print('DRAM', m.groups)

def dram_dbus_agg(agg, result):
    return agg

# Return a functor over an iterable that matches each line, filters out Nones, projects the match, then aggregates
def read_file(pat, proj, agg, init):
    return lambda rfp : functools.reduce(agg, map(proj, filter(None, map(lambda x: pat.match(x), rfp))), init)

# Return a functor over an iterable that broadcasts the iterable over each functor passed to it
# FIXME is there a way to do this without hoisting the entire file into memory? Does it matter?
def broadcast(*parsers):
    return lambda rfp : tuple(p(t) for t,p in zip(itertools.tee(rfp, len(parsers)), parsers))

def expand(fname):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(fname)))

def unpack(elements):
    for elem in map(expand, elements):
        if os.path.isfile(elem):
            yield elem
        else:
            for b,_,f in os.walk(elem):
                yield from (os.path.join(b,t) for t in f)

# Parse the file with the given parsers
def parse_file(parsers, fname):
    with open(fname) as rfp:
        return parsers(rfp)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()

    # File lists
    argparser.add_argument('--base', action='append')
    argparser.add_argument('files', nargs='+')

    # Data selectors
    argparser.add_argument('--speedup', action='store_true')
    argparser.add_argument('--cache', action='append')
    args = argparser.parse_args()

    if args.speedup:
        parsers = broadcast(
            read_file(trace_file_pat, trace_file_proj, trace_file_agg, ''),
            read_file(cpu_stats_pat, cpu_stats_proj, cpu_stats_agg, pd.DataFrame(columns=['instructions','cycles']))
        )

        test_result = dict(parse_file(parsers, f) for f in unpack(args.files))
        test_result = pd.concat(test_result.values(), keys=test_result.keys(), names=['trace','cpu'])
        test_ipc = test_result['instructions'] / test_result['cycles']
        print(test_ipc)

        base_result = dict(parse_file(parsers, f) for f in unpack(args.base))
        base_result = pd.concat(base_result.values(), keys=base_result.keys(), names=['trace','cpu'])
        base_ipc = base_result['instructions'] / base_result['cycles']
        print(base_ipc)

        result = test_ipc.div(base_ipc)
        print(result)
        #result.plot.bar()
        #result.to_csv('results.csv')

        #print(test_ipc/base_ipc)

    if args.cache is not None:
        parsers = broadcast(
            read_file(trace_file_pat, trace_file_proj, trace_file_agg, ''),
            read_file(cache_stats_pat, cache_stats_proj, cache_stats_agg, {})
        )

        base_result = pd.DataFrame.from_dict(dict(parse_file(parsers, f) for f in unpack(args.base)))
        base_result = base_result.loc[args.cache]
        test_result = pd.DataFrame.from_dict(dict(parse_file(parsers, f) for f in unpack(args.files)))
        test_result = test_result.loc[args.cache]
        print(test_result - base_result)
        print(base_result.sum(level=0))
        print((test_result - base_result)/base_result.sum(level=0))

