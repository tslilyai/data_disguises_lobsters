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
    anon_durs = []
    edit_durs = []
    delete_durs = []
    restore_durs = []
    edit_durs_noanon = []
    delete_durs_noanon = []
    restore_durs_noanon = []

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

    filename = "results/hotcrp_results/hotcrp_disguise_stats_450users.csv"
    filename_baseline = "results/hotcrp_results/hotcrp_disguise_stats_450users_baseline.csv"
    filename_batch = "results/hotcrp_results/hotcrp_disguise_stats_450users_batch.csv"
    title = "HotCRP Reviewer Operation Latencies"
    offset = 50
    if i == 0:
        filename = 'results/websubmit_results/disguise_stats_{}lec_{}users.csv'.format(20, 100)
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users_batch.csv'.format(20, 100)
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

    with open(filename_batch,'r') as csvfile:
        rows = csvfile.readlines()
        anon_durs_batch = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs_batch = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs_batch = [float(x)/1000 for x in rows[3].strip().split(',')]
        restore_durs_batch = [float(x)/1000 for x in rows[4].strip().split(',')]
        delete_durs_batch_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
        restore_durs_batch_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]

    with open(filename_baseline,'r') as csvfile:
        rows = csvfile.readlines()
        account_durs_baseline = [int(x)/1000 for x in rows[0].strip().split(',')]
        anon_durs_baseline = [int(x)/1000/100 for x in rows[1].strip().split(',')]
        edit_durs_baseline = [float(x)/1000 for x in rows[2].strip().split(',')]
        delete_durs_baseline = [float(x)/1000 for x in rows[3].strip().split(',')]

    ################ add baseline closer to red line for anonymize
    ax.bar((X-barwidth)[:1],
            [statistics.median(account_durs_baseline)],
            yerr=get_yerr([account_durs_baseline]),
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth)[:1], [statistics.median(account_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth)[1:2], [statistics.median(edit_durs_baseline)],
            yerr=get_yerr([edit_durs_baseline]),
            color='g', capsize=5, width=barwidth, label="Manual Privacy Transformation (No Edna)")
    add_labels((X-barwidth)[1:2], [statistics.median(edit_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth)[3:4], [statistics.median(delete_durs_baseline)],
            yerr=get_yerr([delete_durs_baseline]),
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth)[3:4], [statistics.median(delete_durs_baseline)], ax, 'g', offset)

    ax.bar((X-barwidth)[5:], [statistics.median(anon_durs_baseline)],  color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth)[5:], [statistics.median(anon_durs_baseline)], ax, 'g', offset)

    add_text_labels((X-barwidth)[2:3], ["N/A"], ax, 'g', offset)
    add_text_labels((X-barwidth)[4:5], ["N/A"], ax, 'g', offset)

    ################ edna
    ax.bar((X), [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(edit_durs),
        statistics.median(delete_durs_noanon),
        statistics.median(restore_durs_noanon),
        statistics.median(anon_durs),
    ],
    yerr=get_yerr([
        account_durs,
        edit_durs_noanon,
        edit_durs,
        delete_durs_noanon,
        restore_durs_noanon,
        anon_durs
    ]),
    color='m', capsize=5, width=barwidth, label='Edna')
    add_labels((X),
    [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(edit_durs),
        statistics.median(delete_durs_noanon),
        statistics.median(restore_durs_noanon),
        statistics.median(anon_durs),
    ], ax, 'm', offset)

    ############### edna batched
    ax.bar((X+barwidth), [
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
    color='c', capsize=5, width=barwidth, label="Edna (Token Batching)")
    add_labels((X+barwidth),
    [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(edit_durs_batch),
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
        statistics.median(anon_durs_batch),
    ], ax, 'c', offset)
    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0, ymax=(np.percentile(restore_durs_noanon, 95)*1.15))
    ax.set_xticks(X)
    ax.set_xticklabels(labels)

# LOBSTERS
xs = []
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
xs_decay = []
decay_durs_baseline = []

with open('results/lobsters_results/lobster_decay_baseline.csv','r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        decay_baseline = vals[2]/1000
        xs_decay.append(ndata)
        decay_durs_baseline.append(decay_baseline)

with open("results/lobsters_results/lobsters_disguise_stats_batch.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        vals = [x/1000 for x in vals]
        decay = vals[4]
        undecay = vals[5]
        delete = vals[6]
        restore = vals[7]

        xs.append(ndata)
        delete_durs_batch.append(delete)
        restore_durs_batch.append(restore)
        decay_durs_batch.append(decay)
        undecay_durs_batch.append(undecay)

with open("results/lobsters_results/lobsters_disguise_stats.csv",'r') as csvfile:
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
        account_durs.append(create_edna);
        account_durs_baseline.append(create_baseline);
        delete_durs.append(delete)
        delete_durs_baseline.append(delete_baseline)
        restore_durs.append(restore)
        decay_durs.append(decay)
        undecay_durs.append(undecay)

X = np.arange(5)
labels = ['Create Account',
        'Delete Account',
        'Decay Account',
        'Restore Deleted\nAccount',
        'Restore Decayed\nAccount']
ax = axes_flat[2]
offset = 20

######################## NO EDNA
ax.bar((X-barwidth)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline)
    ],
    yerr=get_yerr([account_durs_baseline, delete_durs_baseline, decay_durs_baseline]),
    capsize=5,
    color='g', width=barwidth, label="No Edna")
add_labels((X-barwidth)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline),
    ], ax, 'g', offset)

add_text_labels((X-barwidth)[3:], ["N/A", "N/A"], ax, 'g', offset)

######################## EDNA
ax.bar((X), [
        statistics.median(account_durs),
        statistics.median(delete_durs),
        statistics.median(decay_durs),
        statistics.median(restore_durs),
        statistics.median(undecay_durs)
    ],
    yerr=get_yerr([account_durs, delete_durs, decay_durs, restore_durs, undecay_durs]),
    capsize=5,
    color='m', width=barwidth, label="Edna")
add_labels((X), [
        statistics.median(account_durs),
        statistics.median(delete_durs),
        statistics.median(decay_durs),
        statistics.median(restore_durs),
        statistics.median(undecay_durs)
    ], ax, 'm', offset)

######################## EDNA BATCH
ax.bar((X+barwidth), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ],
    yerr=get_yerr([account_durs, delete_durs_batch, decay_durs_batch, restore_durs_batch, undecay_durs_batch]),
    capsize=5,
    color='c', width=barwidth, label="Edna (Token Batching)")
add_labels((X+barwidth), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ], ax, 'c', offset)

title = "Lobsters Operation Latencies"
ax.set_title(title)
ax.set_ylabel('Time (ms)')
ax.set_ylim(ymin=0, ymax=np.percentile(restore_durs,95)*1.1)
ax.set_xticks(X)
ax.set_xticklabels(labels)

# one legend per everything
axes_flat[0].legend(loc='upper left');

print(title,
    statistics.median(delete_durs),
    statistics.median(restore_durs),
    statistics.median(delete_durs_batch),
    statistics.median(restore_durs_batch),
    statistics.median(decay_durs),
    statistics.median(undecay_durs),
    statistics.median(decay_durs_batch),
    statistics.median(undecay_durs_batch),
)


fig.tight_layout(h_pad=2)
plt.savefig('client_op_stats.pdf', dpi=300)
