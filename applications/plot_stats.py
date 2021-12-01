import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np

plt.style.use('seaborn-deep')

fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(8,10))
axes_flat = axes.flatten()
barwidth = 0.25
# positions
X = np.arange(5)
labels = ['Create Account', 'Edit Data', 'Delete Account', 'Restore Account', 'Anonymize Account']

# WEBSUBMIT RESULTS
for (i, ax) in enumerate(axes_flat[:2]):
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

# LOBSTERS
X = np.arange(5)
labels = ['Create Account', 'Delete Account', 'Restore Account', 'Decay Account', 'Undecay Account']

account_results_low = []
delete_results_low = []
restore_results_low = []
decay_results_low = []
undecay_results_low = []
account_results_low_baseline = []
delete_results_low_baseline = []

account_results_med = []
delete_results_med = []
restore_results_med = []
decay_results_med = []
undecay_results_med = []
account_results_med_baseline = []
delete_results_med_baseline = []

account_results_high = []
delete_results_high = []
restore_results_high = []
decay_results_high = []
undecay_results_high = []
account_results_high_baseline = []
delete_results_high_baseline = []

xs = []
account_results_all = []
delete_results_all = []
restore_results_all = []
decay_results_all = []
undecay_results_all = []
account_results_all_baseline = []
delete_results_all_baseline = []

filename = "lobsters_stats.csv"
with open(filename,'r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        vals = [x/1000 for x in vals]
        create_baseline = vals[2]
        create_edna = vals[3]
        decay = vals[4]
        undecay = vals[5]
        delete = vals[6]
        restore = vals[7]
        delete_baseline = vals[8]

        xs.append(ndata)
        account_results_all.append(create_edna);
        account_results_all_baseline.append(create_baseline);
        delete_results_all.append(delete)
        delete_results_all_baseline.append(delete_baseline)
        restore_results_all.append(restore)
        decay_results_all.append(decay)
        undecay_results_all.append(undecay)

        if ndata < 10:
            account_results_low.append(create_edna);
            account_results_low_baseline.append(create_baseline);
            delete_results_low.append(delete)
            delete_results_low_baseline.append(delete_baseline)
            restore_results_low.append(restore)
            decay_results_low.append(decay)
            undecay_results_low.append(undecay)

        if ndata > 200 and ndata < 250:
            account_results_med.append(create_edna);
            account_results_med_baseline.append(create_baseline);
            delete_results_med.append(delete)
            delete_results_med_baseline.append(delete_baseline)
            restore_results_med.append(restore)
            decay_results_med.append(decay)
            undecay_results_med.append(undecay)

        if ndata > 500:
            account_results_high.append(create_edna);
            account_results_high_baseline.append(create_baseline);
            delete_results_high.append(delete)
            delete_results_high_baseline.append(delete_baseline)
            restore_results_high.append(restore)
            decay_results_high.append(decay)
            undecay_results_high.append(undecay)

ax = axes_flat[2]
ax.plot(xs, delete_results_all_baseline, color='g', label="Delete, No Edna")
ax.plot(xs, delete_results_all, color='b', label="Delete, Edna")
ax.plot(xs, restore_results_all, color='b', linestyle=':', label="Restore, Edna")
ax.plot(xs, decay_results_all, color='r', label="Decay, Edna")
ax.plot(xs, undecay_results_all, color='r', linestyle=':', label="Undecay, Edna")

'''
ax.bar(X-barwidth/2, [statistics.mean(account_results_low_baseline),
    statistics.mean(delete_results_low_baseline),
    0, statistics.mean(delete_results_low_baseline), 0], color='g', width=barwidth, label="No Edna")
ax.bar(X+barwidth/2, [statistics.mean(account_results_low),
    statistics.mean(delete_results_low), 0,
    statistics.mean(decay_results_low), 0], color='b', width=barwidth, label="Edna")
ax.bar(X, [0, 0,
    statistics.mean(restore_results_low), 0,
    statistics.mean(undecay_results_low)], color='b', width=barwidth)

ax = axes_flat[3]
ax.bar(X-barwidth/2, [statistics.mean(account_results_med_baseline),
    statistics.mean(delete_results_med_baseline),
    0, statistics.mean(delete_results_med_baseline), 0], color='g', width=barwidth, label="No Edna")
ax.bar(X+barwidth/2, [statistics.mean(account_results_med),
    statistics.mean(delete_results_med), 0,
    statistics.mean(decay_results_med), 0], color='b', width=barwidth, label="Edna")
ax.bar(X, [0, 0,
    statistics.mean(restore_results_med), 0,
    statistics.mean(undecay_results_med)], color='b', width=barwidth)

ax = axes_flat[4]
ax.bar(X-barwidth/2, [statistics.mean(account_results_high_baseline),
    statistics.mean(delete_results_high_baseline),
    0, statistics.mean(delete_results_high_baseline), 0], color='g', width=barwidth, label="No Edna")
ax.bar(X+barwidth/2, [statistics.mean(account_results_high),
    statistics.mean(delete_results_high), 0,
    statistics.mean(decay_results_high), 0], color='b', width=barwidth, label="Edna")
ax.bar(X, [0, 0,
    statistics.mean(restore_results_high), 0,
    statistics.mean(undecay_results_high)], color='b', width=barwidth)
'''

title = "Lobsters Operation Latencies vs. Amount of User Data"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0)
ax.set_xlabel('Number of User-Owned Stories and Comments')
ax.legend(loc='best');

fig.tight_layout(h_pad=4)
plt.savefig('client_op_stats.pdf', dpi=300)
