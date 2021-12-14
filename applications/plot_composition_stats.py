import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('seaborn-deep')

# plot styling for paper
matplotlib.rc('font', family='serif', size=8)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=8)
matplotlib.rc('figure', figsize=(1.65,1.2))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)

colors=['g', 'c', 'm']
labels = ["Manual\n(No Edna)", "Direct\nDisguise", "Disguise on\nDecorrelated Data"]
fig = plt.figure(figsize=(2.5, 0.2))
patches = [
    mpatches.Patch(color=color, label=label)
    for label, color in zip(labels, colors)]
fig.legend(patches, labels, mode='expand', ncol=3, loc='center', frameon=False, fontsize=7,
        handlelength=1)
plt.savefig("composition_legend.pdf", dpi=300)

plt.clf()
plt.figure(figsize = (1.65, 1.2))

def add_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        if y[i] < 0.1:
            label = "{0:.1g}".format(y[i])
        elif y[i] > 100:
            label = "{0:.0f}".format(y[i])
        else:
            label = "{0:.1f}".format(y[i])
        plt.text(x[i], y[i]+offset, label, ha='center', color=color, size=6)


def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset, y[i], ha='center', color=color, size=6)

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

barwidth = 0.25
# positions
X = np.arange(2)
labels = ['Delete\nAccount', 'Restore\nDeleted\nAccount']

# WEBSUBMIT/HOTCRP RESULTS
for i in range(2):
    delete_durs_baseline = []
    delete_durs_batch = []
    restore_durs_batch = []
    delete_durs_batch_noanon = []
    restore_durs_batch_noanon = []

    filename_baseline = "results/hotcrp_results/hotcrp_disguise_stats_3080users_baseline.csv"
    filename_batch = "results/hotcrp_results/hotcrp_disguise_stats_3080users_batch.csv"
    title = "hotcrp"
    offset = 100
    if i == 0:
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users.csv'.format(20, 100)
        title = "websubmit"
        offset = 12
    with open(filename_batch,'r') as csvfile:
        rows = csvfile.readlines()
        delete_durs_batch = [float(x)/1000 for x in rows[3].strip().split(',')]
        restore_durs_batch = [float(x)/1000 for x in rows[4].strip().split(',')]
        delete_durs_batch_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
        restore_durs_batch_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]

    with open(filename_baseline,'r') as csvfile:
        rows = csvfile.readlines()
        delete_durs_baseline = [float(x)/1000 for x in rows[3].strip().split(',')]

    ################ add baseline closer to red line for anonymize
    plt.bar((X-barwidth)[:1], [statistics.median(delete_durs_baseline)],
            yerr=get_yerr([delete_durs_baseline]),
            color='g', capsize=5, width=barwidth, label="Manual (No Edna)")
    add_labels((X-barwidth)[:1], [statistics.median(delete_durs_baseline)], plt, 'g', offset)
    add_text_labels((X-barwidth)[1:], ["N/A"], plt, 'g', offset)

    ############### edna w/out composition
    plt.bar((X), [
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
    ],
    yerr=get_yerr([
        delete_durs_batch_noanon,
        restore_durs_batch_noanon,
    ]),
    color='m', capsize=5, width=barwidth, label="Direct Disguise")
    add_labels((X),
    [
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
    ], plt, 'm', offset)

    ############### edna w/composition
    plt.bar((X+barwidth), [
        statistics.median(delete_durs_batch),
        statistics.median(restore_durs_batch),
    ],
    yerr=get_yerr([
        delete_durs_batch,
        restore_durs_batch,
    ]),
    color='c', capsize=5, width=barwidth, label="Disguise on Decorrelated Data")
    add_labels((X+barwidth),
    [
        statistics.median(delete_durs_batch),
        statistics.median(restore_durs_batch),
    ], plt, 'c', offset)

    plt.ylim(ymin=0, ymax=2500)
    plt.yticks(range(0, 2250, 1000))
    if i == 0:
        plt.ylim(ymin=0, ymax=275)
        plt.yticks(range(0, 275, 100))
        plt.ylabel('Time (ms)')
    plt.xticks(X, labels=labels)
    plt.subplots_adjust(left=0.25, right=1.0, bottom=0.4)
    #plt.tight_layout(h_pad=0)
    plt.savefig('composition_stats_{}.pdf'.format(title), dpi=300)
    plt.clf()

