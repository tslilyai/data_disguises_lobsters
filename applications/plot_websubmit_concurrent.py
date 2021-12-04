import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

props = [1/6, 3/10]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = 20

# collect all results
edit_results = []
edit_results_batch = []
edit_results_baseline = []

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

def get_editdata(filename, results):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        editpairs = [x.split(':') for x in rows[0].strip().split(',')]
        editdata = defaultdict(list)
        for x in editpairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            editdata[bucket].append(val)
        results.append(editdata)

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

get_editdata('results/websubmit_results/concurrent_disguise_stats_{}lec_{}users_30disguisers_baseline.csv'
        .format(lec, 100), edit_results_baseline)

for nd in [int(100 * prop) for prop in props]:
    get_editdata('results/websubmit_results/concurrent_disguise_stats_{}lec_{}users_disguising_{}group.csv'
            .format(lec, 100, nd), edit_results)
    get_editdata('results/websubmit_results/concurrent_disguise_stats_{}lec_{}users_disguising_{}group_batch.csv'
            .format(lec, 100, nd), edit_results_batch)
    if nd > 0:
        get_stats('results/websubmit_results/concurrent_disguise_stats_20lec_100users_disguising_{}group.csv'.format(nd),1)
        get_stats('results/websubmit_results/concurrent_disguise_stats_20lec_100users_disguising_{}group_batch.csv'.format(nd),1)
        get_stats('results/websubmit_results/concurrent_disguise_stats_20lec_100users_disguising_{}group.csv'.format(nd),2)
        get_stats('results/websubmit_results/concurrent_disguise_stats_20lec_100users_disguising_{}group_batch.csv'.format(nd),2)

xs = list(edit_results_baseline[0].keys())
order = np.argsort(xs)
xs = np.array(xs)[order]
ys = [statistics.mean(x) for x in edit_results_baseline[0].values()]
ys = np.array(ys)[order]
plt.plot(xs, ys, label='Baseline', color='k')

colors=['m','c']
for p in range(len(props)):
    xs = list(edit_results[p].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results[p].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, color=colors[p], linestyle=":", label='{} Disguisers'.format(int(props[p]*100,)))

    xs = list(edit_results_batch[p].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results_batch[p].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, color=colors[p], label='{} Disguisers (Batch)'.format(int(props[p]*100)))

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0, ymax=5000)
    plt.xlim(xmin=0, xmax=100)
    plt.legend(loc="upper left")
    plt.title("WebSubmit Edit Latency vs. Number of Concurrent Disguisers")

plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results_{}lec_{}users.pdf'.format(lec, 100))
