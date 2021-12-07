import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

sleeps = [10000, 5000, 1000, 100, 0]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = 20

# collect all results
edit_results_batch = []

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

def get_editdata(filename, results):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        editpairs = [x.split(':') for x in rows[1].strip().split(',')]
        editdata = defaultdict(list)
        for x in editpairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            editdata[bucket].append(val)
        results.append(editdata)

for s in sleeps:
    get_editdata('results/websubmit_results/concurrent_disguise_stats_{}sleep_batch.csv'.format(s),
            edit_results_batch)

for s in range(len(sleeps)):
    xs = list(edit_results_batch[s].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results_batch[s].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, linestyle=":", label='{} Sleep'.format(sleeps[s]))

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0)
    plt.xlim(xmin=0, xmax=100)
    plt.legend(loc="upper left")
    plt.title("WebSubmit Edit Latency vs. Disguiser Sleep Time")

plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results_timeseries.pdf')
