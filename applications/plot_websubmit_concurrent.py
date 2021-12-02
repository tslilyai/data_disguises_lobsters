import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

nusers = [100]
props = [1/10, 1/6, 1/4]#, 1/5, 3/10]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = 20

fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(8,8))
axes_flat = axes.flatten()

folder = 'batch'
batch = '_batch'
for i in range(2):
    if i == 0:
        folder = 'no_batch'
        batch = ''
    else:
        folder = 'batch'
        batch = '_batch'
    # collect all results
    normal_edit_results = defaultdict(list)
    edit_results = defaultdict(list)
    edit_results_baseline = defaultdict(list)
    delete_results = defaultdict(list)
    restore_results = defaultdict(list)

    for u in nusers:
        with open('results/websubmit_results/{}/concurrent_disguise_stats_{}lec_{}users_disguising{}.csv'
                .format(folder, lec, u, batch),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = defaultdict(list)
            for x in editpairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                editdata[bucket].append(val)
            normal_edit_results[u] = editdata

        for nd in [int(u * prop) for prop in props]:
            with open('results/websubmit_results/{}/concurrent_disguise_stats_{}lec_{}users_disguising_{}group{}.csv'
                    .format(folder, lec, u, nd, batch),'r') as csvfile:
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

        with open('results/websubmit_results/{}/concurrent_disguise_stats_{}lec_{}users_25disguisers_baseline.csv'
                .format(folder, lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = defaultdict(list)
            for x in editpairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                editdata[bucket].append(val)
            edit_results_baseline[u].append(editdata)

    xs = list(edit_results_baseline[100][0].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results_baseline[100][0].values()]
    ys = np.array(ys)[order]
    axes_flat[i].plot(xs, ys, label='0 Disguisers', color='y')

    xs = list(normal_edit_results[100].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in normal_edit_results[100].values()]
    ys = np.array(ys)[order]
    axes_flat[i].plot(xs, ys, label='1 Disguisers', color='k')

    for p in range(len(props)):
        xs = list(edit_results[100][p].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in edit_results[100][p].values()]
        ys = np.array(ys)[order]
        axes_flat[i].plot(xs, ys, label='{} Disguisers'.format(int(props[p]*100)))

    axes_flat[i].set_xlabel('Benchmark Time (s)')
    axes_flat[i].set_ylabel('Latency (ms)')
    axes_flat[i].set_ylim(ymin=0, ymax=3000)
    axes_flat[i].set_xlim(xmin=0, xmax=nbuckets)

    if i == 0:
        axes_flat[i].set_title("Edit Latency vs. Number of Concurrent Disguisers (Unbatched)")
    else:
        axes_flat[i].set_title("Edit Latency vs. Number of Concurrent Disguisers (Batched)")

axes_flat[0].legend(loc="best")
plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results_{}lec_{}users.pdf'.format(lec, 100))
