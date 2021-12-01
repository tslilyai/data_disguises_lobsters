import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('seaborn-deep')

def add_labels(x,y,ax,color,offset):
    for i in range(len(x)):
        ax.text(x[i], y[i]+offset, "{0:.1f}".format(y[i]), ha='center', color=color)

fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(8,12))
axes_flat = axes.flatten()
barwidth = 0.25
# positions
X = np.arange(5)
labels = ['Create Account', 'Edit Data', 'Delete Account', 'Restore Account', 'Anonymize Account']

# WEBSUBMIT RESULTS
for (i, ax) in enumerate(axes_flat[:2]):
    account_durs = []
    anon_durs = []
    edit_durs = []
    delete_durs = []
    restore_durs = []
    edit_durs_noanon = []
    delete_durs_noanon = []
    restore_durs_noanon = []

    account_durs_baseline = []
    anon_durs_baseline = []
    edit_durs_baseline = []
    delete_durs_baseline = []

    filename = "hotcrp_disguise_stats_450users.csv"
    filename_baseline = "hotcrp_disguise_stats_450users_baseline.csv"
    title = "HotCRP Reviewer Operation Latencies"
    offset = 50
    if i == 0:
        filename = 'websubmit_{}lec_{}users_stats.csv'.format(20, 100)
        filename_baseline = 'websubmit_{}lec_{}users_baseline_stats.csv'.format(20, 100)
        title = "WebSubmit Operation Latencies"
        offset = 10
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

    with open(filename_baseline,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs_baseline = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs_baseline = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs_baseline = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs_baseline = [float(x)/1000 for x in rows[3].strip().split(',')]

    # add baseline closer to red line for anonymize
    ax.bar((X-barwidth/2)[:1],
            [statistics.mean(account_durs_baseline)],
            yerr= [[0], [statistics.mean(account_durs_baseline) + statistics.stdev(account_durs_baseline)]],
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[:1], [statistics.mean(account_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth)[1:2], [statistics.mean(edit_durs_baseline)],
            yerr= [[0], [statistics.mean(edit_durs_baseline) + statistics.stdev(edit_durs_baseline)]],
            color='g', capsize=5, width=barwidth, label="Manual Privacy Transformation (No Edna)")
    add_labels((X-barwidth)[1:2], [statistics.mean(edit_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth/2)[2:3], [statistics.mean(delete_durs_baseline)],
            yerr= [[0], [statistics.mean(delete_durs_baseline) + statistics.stdev(delete_durs_baseline)]],
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[2:3], [statistics.mean(delete_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth/2)[4:], [statistics.mean(anon_durs_baseline)],  color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[4:], [statistics.mean(anon_durs_baseline)], ax, 'g', offset)


    # edna
    ax.bar((X+barwidth/2)[:1], [statistics.mean(account_durs)],
            yerr= [[0], [statistics.mean(account_durs) + statistics.stdev(account_durs)]],
            color='m', capsize=5, width=barwidth)
    add_labels((X+barwidth/2)[:1], [statistics.mean(account_durs)], ax, 'm', offset)

    ax.bar((X)[1:2], [statistics.mean(edit_durs_noanon)],
            yerr= [[0], [statistics.mean(edit_durs_noanon) + statistics.stdev(edit_durs_noanon)]],
            color='m', capsize=5, width=barwidth, label="Edna")
    add_labels((X)[1:2], [statistics.mean(edit_durs_noanon)], ax, 'm', offset)

    ax.bar((X+barwidth/2)[2:3], [statistics.mean(delete_durs_noanon)],
            yerr= [[0], [statistics.mean(delete_durs_noanon) + statistics.stdev(delete_durs_noanon)]],
            color='m', capsize=5, width=barwidth)
    add_labels((X+barwidth/2)[2:3], [statistics.mean(delete_durs_noanon)], ax, 'm', offset)

    ax.bar((X)[3:4], [statistics.mean(restore_durs)],
            yerr= [[0], [statistics.mean(restore_durs) + statistics.stdev(restore_durs)]],
            color='m', capsize=5, width=barwidth)
    add_labels((X)[3:4], [statistics.mean(restore_durs)], ax, 'm', offset)

    ax.bar((X+barwidth/2)[4:], [statistics.mean(anon_durs)],  color='m', capsize=5, width=barwidth)
    add_labels((X+barwidth/2)[4:], [statistics.mean(anon_durs)], ax, 'm', offset)

    # edna with temp recorrelation
    ax.bar((X+barwidth)[1:2], [statistics.mean(edit_durs)],
            yerr= [[0], [statistics.mean(edit_durs) + statistics.stdev(edit_durs)]],
            color='y', capsize=5, width=barwidth, label="Edna After Anonymization")
    add_labels((X+barwidth)[1:2], [statistics.mean(edit_durs)], ax, 'y', offset)

    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0, ymax=(statistics.mean(restore_durs)+statistics.stdev(restore_durs))*2)
    ax.set_xticks(X)
    ax.set_xticklabels(labels)

# LOBSTERS
xs = []
account_results_all = []
delete_results_all = []
restore_results_all = []
decay_results_all = []
undecay_results_all = []
account_results_all_baseline = []
delete_results_all_baseline = []
xs_decay = []
decay_results_all_baseline = []

with open('lobster_decay_baseline.csv','r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        decay_baseline = vals[2]/1000
        xs_decay.append(ndata)
        decay_results_all_baseline.append(decay_baseline)

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

X = np.arange(5)
labels = ['Create Account',
        'Delete Account',
        'Decay Account',
        'Restore Deleted\nAccount',
        'Restore Decayed\nAccount']
ax = axes_flat[2]
ax.bar((X-barwidth/2)[:3], [
        statistics.mean(account_results_all_baseline),
        statistics.mean(delete_results_all_baseline),
        statistics.mean(decay_results_all_baseline),
    ],
    yerr= [
        [0,0,0],
        [statistics.mean(account_results_all_baseline) + statistics.stdev(account_results_all_baseline),
            statistics.mean(delete_results_all_baseline) + statistics.stdev(delete_results_all_baseline),
            statistics.mean(decay_results_all_baseline) + statistics.stdev(decay_results_all_baseline)],
    ],
    capsize=5,
    color='g', width=barwidth, label="No Edna")
add_labels((X-barwidth/2)[:3], [
       statistics.mean(account_results_all_baseline),
       statistics.mean(delete_results_all_baseline),
       statistics.mean(decay_results_all_baseline),
   ], ax, 'g', 50)
ax.bar((X+barwidth/2)[:3], [
        statistics.mean(account_results_all),
        statistics.mean(delete_results_all),
        statistics.mean(decay_results_all)
    ],
    yerr = [
        [0,0,0],
        [statistics.mean(account_results_all) + statistics.stdev(account_results_all),
            statistics.mean(delete_results_all) + statistics.stdev(delete_results_all),
            statistics.mean(decay_results_all) + statistics.stdev(decay_results_all)],
    ],
    capsize=5,
    color='m', width=barwidth, label="Edna")
add_labels((X+barwidth/2)[:3], [
        statistics.mean(account_results_all),
        statistics.mean(delete_results_all),
        statistics.mean(decay_results_all),
    ], ax, 'm', 50)

ax.bar(X[3:], [
        statistics.mean(restore_results_all),
        statistics.mean(undecay_results_all)
    ],
    yerr = [
        [0, 0],
        [statistics.mean(restore_results_all) + statistics.stdev(restore_results_all),
            statistics.mean(undecay_results_all) + statistics.stdev(undecay_results_all)
    ]],
    capsize=5, color='m', width=barwidth)
add_labels(X[3:], [
     statistics.mean(restore_results_all),
     statistics.mean(undecay_results_all)], ax, 'm', 50)

title = "Lobsters Operation Latencies"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0,ymax=(statistics.mean(restore_results_all)+statistics.stdev(restore_results_all))*1.5)
ax.set_xticks(X)
ax.set_xticklabels(labels)

# one legend per everything
axes_flat[0].legend(loc='upper left');

fig.tight_layout(h_pad=4)
plt.savefig('client_op_stats.pdf', dpi=300)
