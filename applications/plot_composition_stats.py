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

fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(5,5))
axes_flat = axes.flatten()
barwidth = 0.25
# positions
X = np.arange(2)
labels = ['Delete Account', 'Restore Deleted\nAccount']

# WEBSUBMIT/HOTCRP RESULTS
for (i, ax) in enumerate(axes_flat[:2]):
    delete_durs_baseline = []
    delete_durs_batch = []
    restore_durs_batch = []
    delete_durs_batch_noanon = []
    restore_durs_batch_noanon = []

    filename_baseline = "results/hotcrp_results/hotcrp_disguise_stats_3080users_baseline.csv"
    filename_batch = "results/hotcrp_results/hotcrp_disguise_stats_3080users_batch.csv"
    title = "HotCRP Reviewer Disguise Costs w/Composition"
    offset = 50
    if i == 0:
        filename_baseline = 'results/websubmit_results/disguise_stats_{}lec_{}users_baseline.csv'.format(20, 100)
        filename_batch = 'results/websubmit_results/disguise_stats_{}lec_{}users.csv'.format(20, 100)
        title = "WebSubmit Disguise Costs w/Composition"
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
    ax.bar((X-barwidth)[:1], [statistics.median(delete_durs_baseline)],
            yerr=get_yerr([delete_durs_baseline]),
            color='g', capsize=5, width=barwidth)
    add_labels((X-barwidth)[:1], [statistics.median(delete_durs_baseline)], ax, 'g', offset)
    add_text_labels((X-barwidth)[1:], ["N/A"], ax, 'g', offset)

    ############### edna w/out composition
    ax.bar((X), [
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
    ],
    yerr=get_yerr([
        delete_durs_batch_noanon,
        restore_durs_batch_noanon,
    ]),
    color='m', capsize=5, width=barwidth, label="No Composition")
    add_labels((X),
    [
        statistics.median(delete_durs_batch_noanon),
        statistics.median(restore_durs_batch_noanon),
    ], ax, 'm', offset)

    ############### edna w/composition
    ax.bar((X+barwidth), [
        statistics.median(delete_durs_batch),
        statistics.median(restore_durs_batch),
    ],
    yerr=get_yerr([
        delete_durs_batch,
        restore_durs_batch,
    ]),
    color='c', capsize=5, width=barwidth, label="Composed After Anonymization")
    add_labels((X+barwidth),
    [
        statistics.median(delete_durs_batch),
        statistics.median(restore_durs_batch),
    ], ax, 'c', offset)

    ax.set_title(title)
    ax.set_ylabel('Time (ms)')
    ax.set_ylim(ymin=0, ymax=(np.percentile(restore_durs_batch, 95)*1.15))
    ax.set_xticks(X)
    ax.set_xticklabels(labels)

# one legend per everything
axes_flat[0].legend(loc='upper left');
plt.tight_layout(h_pad=1)
plt.savefig('composition_stats.pdf', dpi=300)
