import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

sleeps = [100000, 0]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

# collect all results
op_results_batch = []

def get_opdata(filename, results, i):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            opdata[bucket].append(val)
        results.append(opdata)

for s in sleeps:
    get_opdata('results/lobsters_results/concurrent_disguise_stats_{}sleep_batch.csv'.format(s),op_results_batch,1)

for s in range(len(sleeps)):
    xs = list(op_results_batch[s].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in op_results_batch[s].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, linestyle=":", label='{} Sleep'.format(sleeps[s]))

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0)
    plt.xlim(xmin=0, xmax=100)
    plt.legend(loc="upper left")
    plt.title("Lobsters Op Latency vs. Disguiser Sleep Time")

plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results_timeseries.pdf')
