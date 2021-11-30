import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np

plt.style.use('seaborn-deep')

fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(8,8))
axes_flat = axes.flatten()
barwidth = 0.25
# positions
X = np.arange(5)
labels = ['Create Account', 'Edit Data', 'Delete Account', 'Restore Account', 'Anonymize Account']

# WEBSUBMIT RESULTS
for (i, ax) in enumerate(axes_flat):
    account_results = 0
    anon_results = 0
    edit_results = 0
    delete_results = 0
    restore_results = 0
    edit_results_noanon = 0
    delete_results_noanon = 0
    restore_results_noanon = 0

    account_results_baseline = 0
    anon_results_baseline = 0
    edit_results_baseline = 0
    delete_results_baseline = 0

    filename = "hotcrp_disguise_stats_450users.csv"
    filename_baseline = "hotcrp_disguise_stats_450users_baseline.csv"
    title = "HotCRP Operation Latencies"
    if i == 0:
        filename = 'websubmit_{}lec_{}users_stats.csv'.format(20, 100)
        filename_baseline = 'websubmit_{}lec_{}users_baseline_stats.csv'.format(20, 100)
        title = "WebSubmit Operation Latencies"
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs = [float(x)/1000 for x in rows[3].strip().split(',')]
        restore_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
        edit_durs_noanon = [float(x)/1000 for x in rows[5].strip().split(',')]
        delete_durs_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
        restore_durs_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]

        account_results = statistics.mean(account_durs)
        anon_results = statistics.mean(anon_durs)
        edit_results=statistics.mean(edit_durs)
        delete_results=statistics.mean(delete_durs)
        restore_results=statistics.mean(restore_durs)
        edit_results_noanon=statistics.mean(edit_durs_noanon)
        delete_results_noanon=statistics.mean(delete_durs_noanon)
        restore_results_noanon=statistics.mean(restore_durs_noanon)

    with open(filename_baseline,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs = [float(x)/1000 for x in rows[3].strip().split(',')]

        account_results_baseline=statistics.mean(account_durs)
        anon_results_baseline=statistics.mean(anon_durs)
        edit_results_baseline=statistics.mean(edit_durs)
        delete_results_baseline=statistics.mean(delete_durs)

    # add baseline closer to red line for anonymize
    ax.bar(X-barwidth, [account_results_baseline, edit_results_baseline, delete_results_baseline, 0,
        0], color='g', width=barwidth, label="No Edna")
    ax.bar(X-barwidth/2, [0, 0, 0, 0, anon_results_baseline], color='g', width=barwidth)

    ax.bar(X, [account_results, edit_results_noanon, delete_results_noanon, 0, 0],
            color='b', width=barwidth, label="Edna")
    ax.bar(X-barwidth/2, [0, 0, 0, restore_results_noanon, 0], color='b', width=barwidth)
    ax.bar(X+barwidth/2, [0, 0, 0, 0, anon_results], color='b', width=barwidth)

    ax.bar(X+barwidth, [account_results, edit_results, delete_results, 0, 0],
            color='r', width=barwidth, label="Edna + Temporary Recorrelation")
    ax.bar(X+barwidth/2, [0, 0, 0, restore_results, 0], color='r', width=barwidth)

    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0.01)
    ax.set_xticks(X)
    ax.set_yscale('log')
    ax.set_xticklabels(labels)
    ax.legend(loc='best');

fig.tight_layout(h_pad=4)
plt.savefig('client_op_stats.pdf', dpi=300)
