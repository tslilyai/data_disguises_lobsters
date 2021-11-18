import matplotlib.pyplot as plt
import csv
import statistics
import sys
from collections import defaultdict
from itertools import cycle
plt.style.use('seaborn-deep')

nusers = [10, 30, 50]#, 70, 100]
props = [1/10, 1/4, 1/2]
maxts = 10000
nbuckets = 100
bucketwidth = maxts/nbuckets
buckets = [b * bucketwidth for b in range(nbuckets)]

lec = sys.argv[1]

# collect all results, compute maximum latency over all tests + all query  types
edit_results = defaultdict(list)
delete_results = defaultdict(list)
restore_results = defaultdict(list)
edit_results_baseline = defaultdict(list)

for u in nusers:
    for nd in [int(u * prop) for prop in props]:
        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers.csv'.format(lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = [[]] * nbuckets
            for x in editpairs:
                editdata[int((float(x[0])/1000)/bucketwidth)].append(float(x[1])/1000)

            deletepairs = [x.split(':') for x in rows[1].strip().split(',')]
            deletedata = [[]] * nbuckets
            for x in deletepairs:
                deletedata[int((float(x[0])/1000)/bucketwidth)].append(float(x[1])/1000)

            restorepairs = [x.split(':') for x in rows[2].strip().split(',')]
            restoredata = [[]] * nbuckets
            for x in restorepairs:
                restoredata[int((float(x[0])/1000)/bucketwidth)].append(float(x[1])/1000)

            edit_results[u].append([statistics.mean(x) for x in editdata])
            delete_results[u].append([statistics.mean(x) for x in deletedata])
            restore_results[u].append([statistics.mean(x) for x in restoredata])

        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers_baseline.csv'.format(lec, u, nd),'r') as csvfile:
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            editdata = [[]] * nbuckets
            for x in editpairs:
                editdata[int((float(x[0])/1000)/bucketwidth)].append(float(x[1])/1000)
            edit_results_baseline[u].append([statistics.mean(x) for x in editdata])

for u in nusers:
    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(8,15))
    axes_flat = axes.flatten()
    axes_flat[0].plot(buckets, edit_results_baseline[u][0], label='baseline')
    for i in range(len(props)):
        axes_flat[0].plot(buckets, edit_results[u][i], label='edna_{}'.format(props[i]))
    for i in range(len(props)):
        axes_flat[1].plot(buckets, delete_results[u][i], label='edna_{}'.format(props[i]))
    for i in range(len(props)):
        axes_flat[2].plot(buckets, restore_results[u][i], label='edna_{}'.format(props[i]))

    for i in range(len(axes_flat)):
        axes_flat[i].set_xlabel('Benchmark Time (ms)')
        axes_flat[i].set_ylabel('Latency (ms)')
        axes_flat[i].set_ylim(ymin=0)
        axes_flat[i].set_xlim(xmin=0)
        axes_flat[i].legend(loc='upper left');

    axes_flat[0].set_title("Editing Answers to Lecture Latency")
    axes_flat[1].set_title("Delete Account Latency")
    axes_flat[2].set_title("Restore Account Latency")

    fig.tight_layout(h_pad=4)
    plt.savefig('concurrent_results_{}lec_{}users.png'.format(lec, u), dpi=300)
    plt.clf()
