import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

nds = [0, 1]
users = [100, 30, 1]
maxts = 120000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = 20

# collect all results
edit_results_batch = defaultdict(list)

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

def get_editdata(filename, results, u):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        editpairs = [x.split(':') for x in rows[1].strip().split(',')]
        editdata = defaultdict(list)
        for x in editpairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            editdata[bucket].append(val)
        results[u].append(editdata)

for u in users:
    for nd in nds:
        get_editdata('results/websubmit_results/concurrent_{}users_0sleep_{}disguisers.csv'.format(u,
            nd), edit_results_batch, u)

for u in users:
    for nd in nds:
        xs = list(edit_results_batch[u][nd].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in edit_results_batch[u][nd].values()]
        ys = np.array(ys)[order]
        plt.plot(xs, ys, label='{} Normal Users, {} Disguisers (0 sleep)'.format(u, nd))

plt.xlabel('Benchmark Time (s)')
plt.ylabel('Latency (ms)')
plt.ylim(ymin=0, ymax=30)
plt.xlim(xmin=0, xmax=100)
plt.legend(loc="upper left")
plt.title("WebSubmit Edit Latency")
plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results_timeseries.pdf')
