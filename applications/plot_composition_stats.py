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
        plt.text(x[i], y[i]+offset, "{0:.1f}".format(y[i]), ha='center', color=color)

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset, y[i], ha='center', color=color)

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
labels = ['Delete Account', 'Restore Deleted\nAccount']

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
    offset = 50
    if i == 0:
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users.csv'.format(20, 100)
        title = "websubmit"
        offset = 10
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
            color='g', capsize=5, width=barwidth, label="Manual (No Edna"")")
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

    plt.ylabel('Time (ms)')
    plt.ylim(ymin=0, ymax=(np.percentile(restore_durs_batch, 95)*1.15))
    plt.xticks(X, labels=labels)
    plt.legend(loc='upper left');
    plt.tight_layout(h_pad=1)
    plt.savefig('composition_stats_{}.pdf'.format(title), dpi=300)
    plt.clf()
