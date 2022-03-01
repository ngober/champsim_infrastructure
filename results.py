import argparse
import re
import itertools
import os
import glob

import pandas as pd

trace_file_pat = re.compile(r'^CPU (\d+) runs ([-./\w\d]+)$')

cpu_stats_pat = re.compile(r'^CPU (\d+) cumulative IPC: \d+\.?\d* instructions: (?P<instructions>\d+) cycles: (?P<cycles>\d+)$')
cache_stats_pat = re.compile(r'^(?P<name>\S+) (?P<type>LOAD|RFO|PREFETCH|TRANSLATION)\s+ACCESS:\s+\d+  HIT:\s+(?P<hits>\d+)  MISS:\s+(?P<misses>\d+)$')
pref_stats_pat = re.compile(r'^(\S+) PREFETCH  REQUESTED:\s+(\d+)  ISSUED:\s+(\d+)  USEFUL:\s+(\d+)  USELESS:\s+(\d+)$')
amat_pat = re.compile(r'^(\S+) AVERAGE MISS LATENCY: (\d+\.?\d*) cycles$')

dram_rq_pat = re.compile(r'^ RQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)$')
dram_wq_pat = re.compile(r'^ WQ ROW_BUFFER_HIT:\s+(\d+)  ROW_BUFFER_MISS:\s+(\d+)  FULL:\s+(\d+)$')
dram_dbus_pat = re.compile(r'^ DBUS AVG_CONGESTED_CYCLE:\s+(\d+\.?\d*)$')

def expand(fname):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(fname)))

def unpack(elements):
    for elem in map(expand, elements):
        if os.path.isfile(elem):
            yield elem
        else:
            for b,_,f in os.walk(elem):
                yield from (os.path.join(b,t) for t in f)

def parse_file_core(fname):
    corestats = pd.DataFrame(columns=['instructions','cycles'])
    tracename = ''
    with open(fname, 'rt') as rfp:
        for line in rfp:
            m = trace_file_pat.match(line)
            if m is not None:
                tracename = os.path.basename(m[2])

            m = cpu_stats_pat.match(line)
            if m is not None:
                x = pd.DataFrame.from_records([{k:int(v) for k,v in m.groupdict(0).items()}], index=pd.MultiIndex.from_arrays([[tracename], ['cpu'+m[1]]]))
                corestats = pd.concat([corestats, x])

    return corestats

def parse_file_cache(fname):
    cachestats = {}
    tracename = ''
    with open(fname, 'rt') as rfp:
        for line in rfp:
            m = trace_file_pat.match(line)
            if m is not None:
                tracename = os.path.basename(m[2])

            m = cache_stats_pat.match(line)
            if m is not None:
                cachestats[m['name'], m['type'].lower(), 'hit'] = int(m['hits'])
                cachestats[m['name'], m['type'].lower(), 'miss'] = int(m['misses'])

            #m = pref_stats_pat.match(line)
            #if m is not None:
                #print('PREF', m.groups())

            #m = amat_pat.match(line)
            #if m is not None:
                #print('AMAT', m.groups())

            #m = dram_rq_pat.match(line)
            #if m is not None:
                #print('DRAM', m.groups())

            #m = dram_wq_pat.match(line)
            #if m is not None:
                #print('DRAM', m.groups())

            #m = dram_dbus_pat.match(line)
            #if m is not None:
                #print('DRAM', m.groups())

    return tracename, cachestats

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # File lists
    parser.add_argument('--base', action='append')
    parser.add_argument('files', nargs='+')

    # Data selectors
    parser.add_argument('--speedup', action='store_true')
    parser.add_argument('--cache', action='append')
    args = parser.parse_args()

    if args.speedup:
        test_result = pd.concat([parse_file_core(f) for f in unpack(args.files)])
        print(test_result)
        test_ipc = test_result['instructions'] / test_result['cycles']
        #print(test_ipc)

        base_result = pd.concat([parse_file_core(f) for f in unpack(args.base)])
        base_ipc = base_result['instructions'] / base_result['cycles']
        #print(base_ipc)

        result = test_ipc.div(base_ipc)
        print(result)
        #result.plot.bar()
        #result.to_csv('results.csv')

        #print(test_ipc/base_ipc)

    if len(args.cache) > 0:
        base_result = pd.DataFrame.from_dict(dict(parse_file_cache(f) for f in unpack(args.base)))
        base_result = base_result.loc[args.cache]
        test_result = pd.DataFrame.from_dict(dict(parse_file_cache(f) for f in unpack(args.files)))
        test_result = test_result.loc[args.cache]
        print(test_result - base_result)
        print(base_result.sum(level=0))
        print((test_result - base_result)/base_result.sum(level=0))

