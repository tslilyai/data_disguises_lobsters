import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

ndisguising = [0, 1, 10, 20, 30, 50, 100]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(8,8))
axes_flat = axes.flatten()

batch = '_batch'
for i in range(2):
    if i == 0:
        batch = ''
    else:
        batch = '_batch'

    # collect all results
    op_results = []
    delete_results = []
    restore_results = []

    for nd in ndisguising:
        with open('results/lobsters_results/concurrent_disguise_stats_disguising_{}group{}.csv'
                .format(nd, batch),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            opdata = defaultdict(list)
            for x in editpairs:
                bucket = int((float(x[0]))/bucketwidth)
                val = float(x[1])/1000
                opdata[bucket].append(val)
            op_results.append(opdata)

            if nd > 0:
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

                delete_results.append(deletedata)
                restore_results.append(restoredata)
            else:
                delete_results.append({})
                restore_results.append({})

    for r in range(len(ndisguising)):
        xs = list(op_results[r].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in op_results[r].values()]
        ys = np.array(ys)[order]
        axes_flat[i].plot(xs, ys, label='{} Disguisers'.format(ndisguising[r]))

    axes_flat[i].set_xlabel('Benchmark Time (s)')
    axes_flat[i].set_ylabel('Latency (ms)')
    axes_flat[i].set_ylim(ymin=0, ymax=4000)
    axes_flat[i].set_xlim(xmin=0, xmax=nbuckets)
    axes_flat[i].legend(loc="upper left")

    if i == 0:
        axes_flat[i].set_title("Lobsters Operation Latency vs. Number of Concurrent Disguisers (Unbatched)")
    else:
        axes_flat[i].set_title("Lobsters Operation Latency vs. Number of Concurrent Disguisers (Batched)")

plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results.pdf')
