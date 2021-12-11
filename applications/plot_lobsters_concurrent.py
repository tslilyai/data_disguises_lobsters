import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

# plot styling for paper
matplotlib.rc('font', family='serif', size=9)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=8)
matplotlib.rc('figure', figsize=(3.33,1.5))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)

def add_labels(x,y,ax,color,offset):
    for i in range(len(x)):
        ax.text(x[i], y[i]+offset, "{0:.1f}".format(y[i]), ha='center', color=color, fontsize='x-small')

barwidth = 0.15
# positions
X = np.arange(2)
labels = ['Low Load', 'High Load']

# collect all results
op_results = defaultdict(list)
op_results_txn = defaultdict(list)
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(3.33, 2.22))

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

def get_data(filename, results, i, u):
    vals = []
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        ndisguises = int(rows[0].strip())
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            val = float(x[1])/1000
            vals.append(val)
        if len(vals) == 0:
            results[u].append(0)
        results[u].append(vals)

users = [1, 10]
disguiser = ['none', 'cheap', 'expensive']
for u in users:
    for d in disguiser:
        get_data('results/lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                op_results, 1, u)
        get_data('results/lobsters_results/concurrent_disguise_stats_{}users_{}_txn.csv'.format(u, d),
                op_results_txn, 1, u)

offset = 0.1

################ none
plt.bar((X-2*barwidth), [
    statistics.median(op_results[1][0]),
    statistics.median(op_results[10][0]),
],
yerr=get_yerr([
    op_results[1][0],
    op_results[10][0],

]),
color='g', capsize=3, width=barwidth, label="No Disguiser")
add_labels((X-2*barwidth),
[
    statistics.median(op_results[1][0]),
    statistics.median(op_results[10][0]),
], plt, 'g', offset)


################ cheap w/out txn
plt.bar((X-barwidth), [
    statistics.median(op_results[1][1]),
    statistics.median(op_results[10][1]),
],
yerr=get_yerr([
    op_results[1][1],
    op_results[10][1],

]),
color='c', capsize=3, width=barwidth, label="Random Disguiser")
add_labels((X-barwidth),
[
    statistics.median(op_results[1][1]),
    statistics.median(op_results[10][1]),
], plt, 'c', offset)

################ cheap w/txn
plt.bar((X), [
    statistics.median(op_results_txn[1][1]),
    statistics.median(op_results_txn[10][1]),
],
yerr=get_yerr([
    op_results_txn[1][1],
    op_results_txn[10][1],

]),
color='b', capsize=3, width=barwidth, label="Random Disguiser (Txn)")
add_labels((X),
[
    statistics.median(op_results_txn[1][1]),
    statistics.median(op_results_txn[10][1]),
], plt, 'b', offset)

################ expensive
plt.bar((X+barwidth), [
    statistics.median(op_results[1][2]),
    statistics.median(op_results[10][2]),
],
yerr=get_yerr([
    op_results[1][2],
    op_results[10][2],

]),
color='r', capsize=3, width=barwidth, label="Expensive Disguiser")
add_labels((X+barwidth),
[
    statistics.median(op_results[1][2]),
    statistics.median(op_results[10][2]),
], plt, 'r', offset)


################ expensive (Txn)
plt.bar((X+2*barwidth), [
    statistics.median(op_results_txn[1][2]),
    statistics.median(op_results_txn[10][2]),
],
yerr=get_yerr([
    op_results_txn[1][2],
    op_results_txn[10][2],

]),
color='m', capsize=3, width=barwidth, label="Expensive Disguiser (Txn)")
add_labels((X+2*barwidth),
[
    statistics.median(op_results_txn[1][2]),
    statistics.median(op_results_txn[10][2]),
], plt, 'm', offset)


plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=10)
plt.xticks(X, labels=labels)

plt.ylabel('Latency (ms)')
plt.legend(loc="upper left", frameon=False)
plt.tight_layout(h_pad=0)
plt.savefig('lobsters_concurrent_results.pdf')
