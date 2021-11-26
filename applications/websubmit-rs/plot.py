import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

nusers = [100]
props = [1/10, 1/6, 1/4]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = sys.argv[1]

# collect all results, compute maximum latency over all tests + all query  types
normal_edit_results = defaultdict(list)
edit_results = defaultdict(list)
edit_results_baseline = defaultdict(list)
delete_results = defaultdict(list)
restore_results = defaultdict(list)

for u in nusers:
    with open('concurrent_disguise_stats_{}lec_{}users_disguising.csv'.format(lec, u),'r') as csvfile:
        rows = csvfile.readlines()
        editpairs = [x.split(':') for x in rows[0].strip().split(',')]
        editdata = defaultdict(list)
        for x in editpairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            editdata[bucket].append(val)
        normal_edit_results[u] = editdata

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

    with open('concurrent_disguise_stats_{}lec_{}users_25disguisers_baseline.csv'.format(lec, u, nd),'r') as csvfile:
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
plt.plot(xs, ys, label='0 Disguisers', color='y')

xs = list(normal_edit_results[100].keys())
order = np.argsort(xs)
xs = np.array(xs)[order]
ys = [statistics.mean(x) for x in normal_edit_results[100].values()]
ys = np.array(ys)[order]
plt.plot(xs, ys, label='1 Disguisers', color='k')

for i in range(len(props)):
    xs = list(edit_results[100][i].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in edit_results[100][i].values()]
    ys = np.array(ys)[order]
    plt.plot(xs, ys, label='{} Disguisers'.format(int(props[i]*100)))

#for i in range(len(props)):
    #axes_flat[2].scatter(delete_results[100][i].keys(), [statistics.mean(x) for x in delete_results[100][i].values()], label='edna_{}'.format(props[i]))
#for i in range(len(props)):
    #axes_flat[3].scatter(restore_results[100][i].keys(), [statistics.mean(x) for x in restore_results[100][i].values()], label='edna_{}'.format(props[i]))

plt.xlabel('Benchmark Time (s)')
plt.ylabel('Latency (ms)')
plt.ylim(ymin=0)
plt.xlim(xmin=0, xmax=nbuckets)

plt.title("Edit Latency vs. Amount of Concurrent Disguising Actions")
plt.legend(loc="best")

plt.tight_layout(h_pad=4)
plt.savefig('concurrent_results_{}lec_{}users.pdf'.format(lec, 100))
