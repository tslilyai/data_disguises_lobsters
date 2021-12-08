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

def add_text_labels(x,y,ax,color,offset):
    for i in range(len(x)):
        ax.text(x[i], offset, y[i], ha='center', color=color)

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10,12))
axes_flat = axes.flatten()
barwidth = 0.25
# positions
X = np.arange(6)
labels = ['Create Account', 'Edit\nUnanonymized\nData', 'Edit\nAnonymized\nData', 'Delete Account', 'Restore Deleted\nAccount', 'Anonymize Account']

# WEBSUBMIT RESULTS
for (i, ax) in enumerate(axes_flat[:2]):
    account_durs = []
    edit_durs_noanon = []
    anon_durs_batch = []
    delete_durs_batch = []
    restore_durs_batch = []
    edit_durs_batch = []
    delete_durs_batch_noanon = []
    restore_durs_batch_noanon = []

    account_durs_baseline = []
    anon_durs_baseline = []
    edit_durs_baseline = []
    delete_durs_baseline = []

    filename_baseline = "results/hotcrp_results/hotcrp_disguise_stats_450users_baseline.csv"
    filename_batch = "results/hotcrp_results/hotcrp_disguise_stats_450users_batch.csv"
    title = "HotCRP Reviewer Operation Latencies"
    offset = 2
    if i == 0:
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_batch_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users_batch.csv'.format(20, 100)
        title = "WebSubmit Operation Latencies"
        offset = 0.5
    with open(filename_batch,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs_batch = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs_batch = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs_batch = [float(x)/1000 for x in rows[3].strip().split(',')]
        restore_durs_batch = [float(x)/1000 for x in rows[4].strip().split(',')]
        edit_durs_noanon = [float(x)/1000 for x in rows[5].strip().split(',')]
        delete_durs_batch_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
        restore_durs_batch_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]

    with open(filename_baseline,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs_baseline = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs_baseline = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs_baseline = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs_baseline = [float(x)/1000 for x in rows[3].strip().split(',')]

    ################ add baseline closer to red line for anonymize
    ax.bar((X-barwidth/2)[:1],
            [statistics.median(account_durs_baseline)],
            yerr=get_yerr([account_durs_baseline]),
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[:1], [statistics.median(account_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth/2)[1:2], [statistics.median(edit_durs_baseline)],
            yerr=get_yerr([edit_durs_baseline]),
            color='g', capsize=5, width=barwidth, label="Manual Privacy Transformation (No Edna)")
    add_labels((X-barwidth/2)[1:2], [statistics.median(edit_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth/2)[3:4], [statistics.median(delete_durs_baseline)],
            yerr=get_yerr([delete_durs_baseline]),
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[3:4], [statistics.median(delete_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth/2)[5:], [statistics.median(anon_durs_baseline)],  color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth/2)[5:], [statistics.median(anon_durs_baseline)], ax, 'g', offset)

    add_text_labels((X-barwidth/2)[2:3], ["N/A"], ax, 'g', offset)
    add_text_labels((X-barwidth/2)[4:5], ["N/A"], ax, 'g', offset)

    ############### edna batched
    ax.bar((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(edit_durs_batch),
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
        statistics.median(anon_durs_batch),
    ],
    yerr=get_yerr([
        account_durs,
        edit_durs_noanon,
        edit_durs_batch,
        delete_durs_batch_noanon,
        restore_durs_batch_noanon,
        anon_durs_batch
    ]),
    color='m', capsize=5, width=barwidth, label="Edna")
    add_labels((X+barwidth/2),
    [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(edit_durs_batch),
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
        statistics.median(anon_durs_batch),
    ], ax, 'm', offset)
    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0, ymax=(np.percentile(restore_durs_batch_noanon, 95)*1.15))
    ax.set_xticks(X)
    ax.set_xticklabels(labels)

# LOBSTERS
account_durs = []
delete_durs = []
restore_durs = []
decay_durs = []
undecay_durs = []

delete_durs_batch = []
restore_durs_batch = []
decay_durs_batch = []
undecay_durs_batch = []

account_durs_baseline = []
delete_durs_baseline = []
decay_durs_baseline = []

with open('results/lobsters_results/lobsters_disguise_stats_baseline_batch.csv','r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        decay_baseline = vals[0]/1000
        decay_durs_baseline.append(decay_baseline)
        delete_durs_baseline.append(decay_baseline)

with open("results/lobsters_results/lobsters_disguise_stats_batch.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:]
    rowvec = [int(x.strip()) for x in rows[0].split(",")[:-1]]
    rows = [rowvec[i:i + 8] for i in range(0, len(rowvec), 8)]
    for vals in rows:
        #vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        vals = [x/1000 for x in vals]
        create_baseline = vals[2]
        create_edna = vals[3]
        decay = vals[4]
        undecay = vals[5]
        delete = vals[6]
        restore = vals[7]

        account_durs.append(create_edna);
        delete_durs_batch.append(delete)
        restore_durs_batch.append(restore)
        decay_durs_batch.append(decay)
        undecay_durs_batch.append(undecay)
        account_durs_baseline.append(create_baseline);

X = np.arange(5)
labels = ['Create Account',
        'Delete Account',
        'Decay Account',
        'Restore Deleted\nAccount',
        'Restore Decayed\nAccount']
ax = axes_flat[2]
offset = 7

######################## NO EDNA
ax.bar((X-barwidth/2)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline)
    ],
    yerr=get_yerr([account_durs_baseline, delete_durs_baseline, decay_durs_baseline]),
    capsize=5,
    color='g', width=barwidth, label="No Edna")
add_labels((X-barwidth/2)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline),
    ], ax, 'g', offset)

add_text_labels((X-barwidth/2)[3:], ["N/A", "N/A"], ax, 'g', offset)

######################## EDNA BATCH
ax.bar((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ],
    yerr=get_yerr([account_durs, delete_durs_batch, decay_durs_batch, restore_durs_batch, undecay_durs_batch]),
    capsize=5,
    color='m', width=barwidth, label="Edna")
add_labels((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ], ax, 'm', offset)

title = "Lobsters Operation Latencies"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0, ymax=np.percentile(delete_durs_batch,95)*1.1)
ax.set_xticks(X)
ax.set_xticklabels(labels)

# one legend per everything
axes_flat[0].legend(loc='upper left');
fig.tight_layout(h_pad=2)
plt.savefig('client_op_stats.pdf', dpi=300)
