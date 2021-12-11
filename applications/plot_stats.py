import matplotlib
import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('seaborn-deep')
plt.figure(figsize = (3.33, 1.5))

# plot styling for paper
matplotlib.rc('font', family='serif', size=9)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=8)
matplotlib.rc('figure', figsize=(3.33,1.5))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)

def add_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], y[i]+offset, "{0:.1f}".format(y[i]), ha='center', color=color, fontsize='x-small')

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset, y[i], ha='center', color=color, fontsize='x-small')

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

# positions
barwidth = 0.25
X = np.arange(6)
labels = [
        'Create\nAccount',
        'Edit\nPublic\nData',
        'Delete\nAccount',
        'Anonym.\nAccount',
        'Edit\nAnonym.\nData',
        'Restore\nDeleted\nAccount',
]

# WEBSUBMIT RESULTS
for i in range(2):
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

    app = "hotcrp"
    filename_baseline = "results/hotcrp_results/hotcrp_disguise_stats_3080users_baseline.csv"
    filename_batch = "results/hotcrp_results/hotcrp_disguise_stats_3080users_batch.csv"
    offset = 2
    if i == 0:
        app = "websubmit"
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_batch_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users_batch.csv'.format(20, 100)
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
    plt.bar((X-barwidth/2)[:4],
            [
                statistics.median(account_durs_baseline),
                statistics.median(edit_durs_baseline),
                statistics.median(delete_durs_baseline),
                statistics.median(anon_durs_baseline),
            ],
            yerr=get_yerr([
                account_durs_baseline,
                edit_durs_baseline,
                delete_durs_baseline,
                anon_durs_baseline,
            ]),
            color='g', capsize=5, width=barwidth, label="Manual (No Edna)")
    add_labels((X-barwidth/2)[:4], [
        statistics.median(account_durs_baseline),
        statistics.median(edit_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(anon_durs_baseline),
    ], plt, 'g', offset)
    add_text_labels((X-barwidth/2)[4:], ["N/A", "N/A"], plt, 'g', offset)

    ############### edna batched
    plt.bar((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(delete_durs_batch_noanon),
        statistics.median(anon_durs_batch),
        statistics.median(edit_durs_batch),
        statistics.median(restore_durs_batch_noanon),
    ],
    yerr=get_yerr([
        account_durs,
        edit_durs_noanon,
        delete_durs_batch_noanon,
        anon_durs_batch,
        edit_durs_batch,
        restore_durs_batch_noanon,
    ]),
    color='m', capsize=3, width=barwidth, label="Edna")
    add_labels((X+barwidth/2),
    [
        statistics.median(account_durs),
        statistics.median(edit_durs_noanon),
        statistics.median(delete_durs_batch_noanon),
        statistics.median(anon_durs_batch),
        statistics.median(edit_durs_batch),
        statistics.median(restore_durs_batch_noanon),
    ], plt, 'm', offset)
    plt.ylabel('Time (ms)')
    #plt.ylim(ymin=0, ymax=(np.percentile(restore_durs_batch_noanon, 95)*1.15))
    if app == "websubmit":
      plt.ylim(ymin=0, ymax=30)
      plt.yticks(range(0, 31, 10))
    else:
      plt.ylim(ymin=0, ymax=175)
      plt.yticks(range(0, 175, 50))
    plt.xticks(X, labels=labels)
    plt.legend(loc='upper left', frameon=False);
    plt.tight_layout(h_pad=0)
    plt.savefig("{}_op_stats.pdf".format(app))
    plt.clf()

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
        baseline_delete = vals[8]

        account_durs.append(create_edna);
        delete_durs_batch.append(delete)
        restore_durs_batch.append(restore)
        decay_durs_batch.append(decay)
        undecay_durs_batch.append(undecay)
        account_durs_baseline.append(create_baseline);
        delete_durs_baseline.append(baseline_delete);
        decay_durs_baseline.append(baseline_delete);

X = np.arange(5)
labels = ['Create\nAccount',
        'Delete\nAccount',
        'Decay\nAccount',
        'Restore\nDeleted\nAccount',
        'Restore\nDecayed\nAccount']
offset = 7

######################## NO EDNA
plt.bar((X-barwidth/2)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline)
    ],
    yerr=get_yerr([account_durs_baseline, delete_durs_baseline, decay_durs_baseline]),
    capsize=3,
    color='g', width=barwidth, label="Manual (No Edna)")
add_labels((X-barwidth/2)[:3], [
        statistics.median(account_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline),
    ], plt, 'g', offset)
add_text_labels((X-barwidth/2)[3:], ["N/A", "N/A"], plt, 'g', offset)

######################## EDNA BATCH
plt.bar((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ],
    yerr=get_yerr([account_durs, delete_durs_batch, decay_durs_batch, restore_durs_batch, undecay_durs_batch]),
    capsize=3,
    color='m', width=barwidth, label="Edna")
add_labels((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(delete_durs_batch),
        statistics.median(decay_durs_batch),
        statistics.median(restore_durs_batch),
        statistics.median(undecay_durs_batch)
    ], plt, 'm', offset)

plt.ylabel('Time (ms)')
#plt.ylim(ymin=0, ymax=np.percentile(restore_durs_batch,95)*1.1)
plt.ylim(ymin=0, ymax=250)
plt.xticks(X, labels=labels)

plt.legend(loc='upper left', frameon=False);
plt.tight_layout(h_pad=0)
plt.savefig('lobsters_op_stats.pdf', dpi=300)
