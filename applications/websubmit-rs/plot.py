import matplotlib.pyplot as plt
import csv
import statistics
import sys
from collections import defaultdict
from itertools import cycle
plt.style.use('seaborn-deep')

nusers = [10, 30, 50, 70, 100]
props = [1/10, 1/4, 1/2]

lec = sys.argv[0]

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
            edit_data = [(float(x[0])/1000, float(x[0])/1000 for x in editpairs]

            deletepairs = [x.split(':') for x in rows[1].strip().split(',')]
            delete_data = [(float(x[0])/1000, float(x[1])/1000 for x in deletepairs]

            restorepairs = [x.split(':') for x in rows[2].strip().split(',')]
            restore_data = [(float(x[0])/1000, float(x[2])/1000 for x in restorepairs]

            edit_results[u].append(edit_data)
            delete_results[u].append(delete_data)
            restore_results[u].append(restore_data)

        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers_baseline.csv'.format(lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            editpairs = [x.split(':') for x in rows[0].strip().split(',')]
            edit_data = [(float(x[0])/1000, float(x[0])/1000 for x in editpairs]
            edit_results_baseline[u].append(edit_data)

for u in nusers:
    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(8,15))
    axes_flat = axes.flatten()

    axes_flat[0].hist(edit_results_baseline[u][0], label='baseline')
    for i in range(len(props)):
        axes_flat[0].hist(edit_results[u][i], label='edna_{}'.format(props[i]))
    for i in range(len(props)):
        axes_flat[1].hist(nusers, delete_results[i], label='edna_{}'.format(props[i]))
    for i in range(len(props)):
        axes_flat[2].hist(nusers, restore_results[i], label='edna_{}'.format(props[i]))

    for i in xrange(len(axes_flat)):
        axes_flat[i].set_xlabel('Benchmark Time (ms)')
        axes_flat[i].set_ylabel('Latency (ms)')
        axes_flat[i].set_ylim(ymin=0)
        axes_flat[i].set_xlim(xmin=0)
        axes_flat[i].legend(loc='upper left');

    axes_flat[0].set_title("Time to Edit Answers to Lecture")
    axes_flat[1].set_title("Average Time to Delete Account")
    axes_flat[2].set_title("Average Time to Restore Account")

    fig.tight_layout(h_pad=4)
    plt.savefig('concurrent_results_{}lec_{}users.png'.format(lec, u), dpi=300)
