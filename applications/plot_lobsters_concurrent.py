import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

ndisguising = [0, 1, 50, 100]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

# collect all results
op_results = []
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

def get_stats(filename, i):
    vals = []
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            val = float(x[1])/1000
            vals.append(val)
    print (min(vals), statistics.median(vals), max(vals))

for nd in ndisguising:
    get_opdata('results/lobsters_results/concurrent_disguise_stats_disguising_{}group.csv'.format(nd),op_results,0)
    get_opdata('results/lobsters_results/concurrent_disguise_stats_disguising_{}group_batch.csv'.format(nd),op_results_batch,0)
    if nd > 0:
        print(nd)
        get_stats('results/lobsters_results/concurrent_disguise_stats_disguising_{}group.csv'.format(nd),1)
        get_stats('results/lobsters_results/concurrent_disguise_stats_disguising_{}group_batch.csv'.format(nd),1)
        get_stats('results/lobsters_results/concurrent_disguise_stats_disguising_{}group.csv'.format(nd),2)
        get_stats('results/lobsters_results/concurrent_disguise_stats_disguising_{}group_batch.csv'.format(nd),2)


xs = list(op_results[0].keys())
order = np.argsort(xs)
xs = np.array(xs)[order]
ys = [statistics.mean(x) for x in op_results_batch[0].values()]
ys = np.array(ys)[order]
plt.plot(xs, ys, label='Baseline', color='k')

colors=['m','c']
for r in range(1, len(ndisguising)):
    xs = list(op_results[r].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in op_results[r].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, color=colors[r-1], linestyle=":", label='{} Disguisers'.format(ndisguising[r]))

    xs = list(op_results_batch[r].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in op_results_batch[r].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, color=colors[r-1], label='{} Disguisers (Batch)'.format(ndisguising[r]))

plt.xlabel('Benchmark Time (s)')
plt.ylabel('Latency (ms)')
plt.ylim(ymin=0, ymax=500)
plt.xlim(xmin=0, xmax=100)
plt.legend(loc="upper left")
plt.title("Lobsters Operation Latency vs. Number of Concurrent Disguisers")

plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results.pdf')
