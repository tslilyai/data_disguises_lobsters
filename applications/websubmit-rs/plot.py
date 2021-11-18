import matplotlib.pyplot as plt
import csv
import statistics
import sys
from collections import defaultdict
from itertools import cycle
cycol1 = cycle('bgrm')
cycol2 = cycle('cykw')

nusers = [10, 30, 50, 70, 100]
props = [1/10, 1/6, 1/4, 1/2]

lec = sys.argv[1]

# collect all results, compute maximum latency over all tests + all query  types
account_results = defaultdict(list)
edit_results = defaultdict(list)
delete_results = defaultdict(list)
restore_results = defaultdict(list)

account_results_baseline = defaultdict(list)
edit_results_baseline = defaultdict(list)

fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(8,15))
axes_flat = axes.flatten()

for u in nusers:
    ndisg = [int(u * prop) for prop in props]
    for i, nd in enumerate(ndisg):
        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers.csv'.format(lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
            edit_durs = [float(x)/1000 for x in rows[1].strip().split(',')]
            delete_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
            restore_durs = [float(x)/1000 for x in rows[3].strip().split(',')]

            account_results[i].append(statistics.mean(account_durs))
            edit_results[i].append(statistics.mean(edit_durs))
            delete_results[i].append(statistics.mean(delete_durs))
            restore_results[i].append(statistics.mean(restore_durs))

        with open('concurrent_disguise_stats_{}lec_{}users_{}disguisers_baseline.csv'.format(lec, u, nd),'r') as csvfile:
            rows = csvfile.readlines()
            account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
            edit_durs = [float(x)/1000 for x in rows[1].strip().split(',')]

            account_results_baseline[i].append(statistics.mean(account_durs))
            edit_results_baseline[i].append(statistics.mean(edit_durs))

for (i, results) in account_results.items():
    axes_flat[0].plot(nusers, account_results[i], label='edna_{}disguisers'.format(props[i]), color=next(cycol1))
    axes_flat[0].plot(nusers, account_results_baseline[i], label='baseline_{}disguisers'.format(props[i]), color=next(cycol2))

for (i, results) in edit_results.items():
    axes_flat[1].plot(nusers, edit_results[i], label='edna_{}disguisers'.format(props[i]), color=next(cycol1))
    axes_flat[1].plot(nusers, edit_results_baseline[i], label='baseline_{}disguisers'.format(props[i]), color=next(cycol2))

for (i, results) in delete_results.items():
    axes_flat[2].plot(nusers, delete_results[i], label='edna_{}disguisers'.format(props[i]), color=next(cycol1))

for (i, results) in restore_results.items():
    axes_flat[3].plot(nusers, restore_results[i], label='edna_{}disguisers'.format(props[i]), color=next(cycol1))

axes_flat[0].set_title("Average Time to Create Account")
axes_flat[0].set_xlabel('Number of users')
axes_flat[0].set_ylabel('Time (ms)')
axes_flat[0].set_ylim(ymin=0)
axes_flat[0].legend(loc='upper left');

axes_flat[1].set_title("Average Time to Edit Answers to Lecture")
axes_flat[1].set_xlabel('Number of users')
axes_flat[1].set_ylabel('Time (ms)')
axes_flat[1].set_ylim(ymin=0)

axes_flat[2].set_title("Average Time to Delete Account")
axes_flat[2].set_xlabel('Number of users')
axes_flat[2].set_ylabel('Time (ms)')
axes_flat[2].set_ylim(ymin=0)

axes_flat[3].set_title("Average Time to Restore Account")
axes_flat[3].set_xlabel('Number of users')
axes_flat[3].set_ylabel('Time (ms)')
axes_flat[3].set_ylim(ymin=0)

fig.tight_layout(h_pad=4)
plt.savefig('concurrent_results_{}lec.png'.format(lec), dpi=300)
