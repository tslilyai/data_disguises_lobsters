import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict
plt.style.use('seaborn-deep')

nusers = [100, 200]
props = [1/10, 1/4]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = sys.argv[1]

# collect all results, compute maximum latency over all tests + all query  types
edit_results = defaultdict(list)
delete_results = defaultdict(list)
restore_results = defaultdict(list)
edit_results_baseline = defaultdict(list)

for u in nusers:
    for nd in [int(u * prop) for prop in props]:
        with open('concurrent_disguise_stats_{}lec_{}users_disguising_{}batch.csv'.format(lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = defaultdict(list)
            for x in editpairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                editdata[bucket].append(val)

            deletepairs = [x.split(':') for x in rows[1].strip().split(',')]
            deletedata = defaultdict(list)
            for x in deletepairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                deletedata[bucket].append(val)

            restorepairs = [x.split(':') for x in rows[2].strip().split(',')]
            restoredata = defaultdict(list)
            for x in restorepairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                restoredata[bucket].append(val)

            edit_results[u].append(editdata)
            delete_results[u].append(deletedata)
            restore_results[u].append(restoredata)

        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers_baseline.csv'.format(lec, u, nd),'r') as csvfile:
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = defaultdict(list)
            for x in editpairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                editdata[bucket].append(val)
            edit_results_baseline[u].append(editdata)

for u in nusers:
    fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(8,15))
    axes_flat = axes.flatten()
    for i in range(len(props)):
        xs = list(edit_results_baseline[u][i].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in edit_results_baseline[u][0].values()]
        ys = np.array(ys)[order]
        axes_flat[0].plot(xs, ys, label='baseline_{}'.format(props[i]))

        xs = list(edit_results[u][i].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in edit_results[u][i].values()]
        ys = np.array(ys)[order]
        axes_flat[1].plot(xs, ys, label='edna_{}'.format(props[i]))

    for i in range(len(props)):
        axes_flat[2].scatter(delete_results[u][i].keys(), [statistics.mean(x) for x in delete_results[u][i].values()], label='edna_{}'.format(props[i]))
    for i in range(len(props)):
        axes_flat[3].scatter(restore_results[u][i].keys(), [statistics.mean(x) for x in restore_results[u][i].values()], label='edna_{}'.format(props[i]))

    for i in range(len(axes_flat)):
        axes_flat[i].set_xlabel('Benchmark Time (s)')
        axes_flat[i].set_ylabel('Latency (ms)')
        axes_flat[i].set_ylim(ymin=0)
        axes_flat[i].set_xlim(xmin=0, xmax=nbuckets)
        axes_flat[i].legend(loc='upper left');

    axes_flat[0].set_title("Baseline: Editing Answers to Lecture Latency")
    axes_flat[1].set_title("With Disguises: Editing Answers to Lecture Latency")
    axes_flat[2].set_title("Delete Account Latency")
    axes_flat[3].set_title("Restore Account Latency")

    fig.tight_layout(h_pad=4)
    plt.savefig('concurrent_results_{}lec_{}users.png'.format(lec, u), dpi=300)
    plt.clf()
