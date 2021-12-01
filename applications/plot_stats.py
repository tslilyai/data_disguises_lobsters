import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('seaborn-deep')

def add_labels(x,y,ax,color):
    for i in range(len(x)):
        ax.text(x[i], y[i], "{0:.1f}".format(y[i]), ha='center', color=color)

fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(8,15))
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
    add_labels((X-barwidth)[:3], [account_results_baseline, edit_results_baseline,
        delete_results_baseline], ax, 'g')
    ax.bar(X-barwidth/2, [0, 0, 0, 0, anon_results_baseline], color='g', width=barwidth)
    add_labels((X-barwidth/2)[4:], [anon_results_baseline], ax, 'g')

    # edna
    ax.bar(X, [account_results, edit_results_noanon, delete_results_noanon, 0, 0],
            color='b', width=barwidth, label="Edna")
    add_labels(X[:3], [account_results,edit_results_noanon,delete_results_noanon], ax, 'b')
    ax.bar(X-barwidth/2, [0, 0, 0, restore_results_noanon, 0], color='b', width=barwidth)
    add_labels((X-barwidth/2)[3:4], [restore_results_noanon], ax, 'b')
    ax.bar(X+barwidth/2, [0, 0, 0, 0, anon_results], color='b', width=barwidth)
    add_labels((X+barwidth/2)[4:], [anon_results], ax, 'b')

    # temp recorrelation
    ax.bar(X+barwidth, [account_results, edit_results, delete_results, 0, 0],
            color='r', width=barwidth, label="Edna + Temporary Recorrelation")
    add_labels((X+barwidth)[:3], [account_results, edit_results, delete_results], ax, 'r')

    ax.bar(X+barwidth/2, [0, 0, 0, restore_results, 0], color='r', width=barwidth)
    add_labels((X+barwidth/2)[3:4], [restore_results], ax, 'r')

    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0)
    ax.set_xticks(X)
    #ax.set_yscale('log')
    ax.set_xticklabels(labels)
    ax.legend(loc='best');

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

account_results_per = []
delete_results_per = []
restore_results_per = []
decay_results_per = []
undecay_results_per = []
account_results_per_baseline = []
delete_results_per_baseline = []
decay_results_per_baseline = []

with open('lobster_decay_baseline.csv','r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        decay_baseline = vals[2]/1000
        xs_decay.append(ndata)
        decay_results_all_baseline.append(decay_baseline)
        if ndata > 0:
            decay_results_per_baseline.append(decay_baseline/ndata)

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

        if ndata > 0:
            account_results_per.append(create_edna);
            account_results_per_baseline.append(create_baseline);
            delete_results_per.append(delete/ndata)
            delete_results_per_baseline.append(delete_baseline/ndata)
            restore_results_per.append(restore/ndata)
            decay_results_per.append(decay/ndata)
            undecay_results_per.append(undecay/ndata)

X = np.arange(5)
labels = ['Create Account',
        'Delete Story/\nComment',
        'Decay Story/\nComment',
        'Restore Deleted\nStory/Comment',
        'Restore Decayed\nStory/Comment']
ax = axes_flat[3]
ax.plot(xs, delete_results_all_baseline, color='g', linestyle="--", label="Delete/Decay Account, No Edna")
ax.plot(xs, delete_results_all, color='b', label="Delete Account, Edna")
#ax.plot(xs_decay, decay_results_all_baseline, color='r', linestyle="--", label="Decay, No Edna")
ax.plot(xs, decay_results_all, color='r', label="Decay Account, Edna")
ax.plot(xs, restore_results_all, color='b', linestyle=':', label="Restore Deleted Account, Edna")
ax.plot(xs, undecay_results_all, color='r', linestyle=':', label="Restore Decayed Account, Edna")
title = "Lobsters Operation Latencies vs. Amount of User Data"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0)
ax.set_xlabel('Number of User-Owned Stories and Comments')
ax.legend(loc='best');

ax = axes_flat[2]
ax.bar(X-barwidth/2, [
    statistics.mean(account_results_per_baseline),
    statistics.mean(delete_results_per_baseline),
    statistics.mean(decay_results_per_baseline),
    0,
    0],
    color='g', width=barwidth, label="No Edna")
add_labels((X-barwidth/2)[:3], [
       statistics.mean(account_results_per_baseline),
       statistics.mean(delete_results_per_baseline),
       statistics.mean(decay_results_per_baseline),
   ], ax, 'g')

ax.bar(X+barwidth/2, [
    statistics.mean(account_results_per),
    statistics.mean(delete_results_per),
    statistics.mean(decay_results_per),
    0,
    0], color='b', width=barwidth, label="Edna")
ax.bar(X, [
    0,
    0,
    0,
    statistics.mean(restore_results_per),
    statistics.mean(undecay_results_per)], color='b', width=barwidth)
add_labels((X+barwidth/2)[:3], [
    statistics.mean(account_results_per),
    statistics.mean(delete_results_per),
    statistics.mean(decay_results_per),
    ], ax, 'b')
add_labels(X[3:], [
     statistics.mean(restore_results_per),
    statistics.mean(undecay_results_per)], ax, 'b')

title = "Fine-Grained Lobsters Operation Latencies"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0)
ax.set_xticks(X)
ax.set_xticklabels(labels)
ax.legend(loc='best');

fig.tight_layout(h_pad=4)
plt.savefig('client_op_stats.pdf', dpi=300)
