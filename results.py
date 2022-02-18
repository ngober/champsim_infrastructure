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

def unpack(elements):
    for elem in itertools.chain.from_iterable(map(glob.iglob, elements)):
        if os.path.isfile(elem):
            yield elem
        else:
            for b,_,f in os.walk(elem):
                yield from (os.path.join(b,t) for t in f)

def parse_file(fname):
    cachestats = {}
    corestats = {}
    tracename = ''
    with open(fname, 'rt') as rfp:
        for line in rfp:
            m = trace_file_pat.match(line)
            if m is not None:
                tracename = os.path.basename(m[2])

            m = cpu_stats_pat.match(line)
            if m is not None:
                corestats['cpu'+m[1]] = {k:int(v) for k,v in m.groupdict(0).items()}

            #m = cache_stats_pat.match(line)
            #if m is not None:
                #cachestats[m['name'], m['type'].lower(), 'hit'] = int(m['hits'])
                #cachestats[m['name'], m['type'].lower(), 'miss'] = int(m['misses'])

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

    return zip(itertools.repeat(tracename), *zip(*corestats.items()))
    #return tracename, cachestats

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', type=str, nargs=1)
    parser.add_argument('files', type=str, nargs='+')
    args = parser.parse_args()

    test_vals = itertools.chain.from_iterable(parse_file(f) for f in unpack(args.files))
    test_result = pd.DataFrame.from_dict({(f,c) : val for f,c,val in test_vals}, orient='index')
    test_ipc = test_result['instructions'] / test_result['cycles']

    base_vals = itertools.chain.from_iterable(parse_file(f) for f in unpack(args.base))
    base_result = pd.DataFrame.from_dict({(f,c) : val for f,c,val in base_vals}, orient='index')
    base_ipc = base_result['instructions'] / base_result['cycles']

    print(test_ipc/base_ipc)

    #result = pd.DataFrame.from_dict(dict(parse_file(f) for f in unpack(args.files)))
    #print(result.T)

