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
edit_results= {}
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

def get_data(filename, results, i):
    vals = []
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        ndisguises = int(rows[0].strip())
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            val = float(x[1])/1000
            vals.append(val)
        results[ndisguises] = [
                np.percentile(vals, 5),
                statistics.median(vals),
                np.percentile(vals, 95)]

for s in sleeps:
    get_editdata('results/websubmit_results/concurrent_disguise_stats_{}sleep_batch.csv'.format(s), edit_results)

xs = list(edit_results_baseline[0].keys())
order = np.argsort(xs)
xs = np.array(xs)[order]
ys = [statistics.mean(x) for x in edit_results_baseline[0].values()]
ys = np.array(ys)[order]
plt.plot(xs, ys, label='Baseline', color='k')

colors=['m','c']
for s in sleeps:
for p in range(len(props)):
    xs = list(edit_results_batch[p].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results_batch[p].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, color=colors[p], label='{} Disguisers'.format(int(props[p]*100)))

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0, ymax=2000)
    plt.xlim(xmin=0, xmax=100)
    plt.legend(loc="upper left")
    plt.title("WebSubmit Edit Latency vs. Number of Concurrent Disguisers")

plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results_{}lec_{}users.pdf'.format(lec, 100))
