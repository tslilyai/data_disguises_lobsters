import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')
def add_labels(x,y,ax,color,offset):
    for i in range(len(x)):
        ax.text(x[i], y[i]+offset, "{0:.1f}".format(y[i]), ha='center', color=color)

barwidth = 0.25
# positions
X = np.arange(2)
labels = ['Low Load', 'High Load']#, '100 Users', '100 Users Txn']

# collect all results
op_results = defaultdict(list)
op_results_txn = defaultdict(list)
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6.66,3))

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
            if len(x) < 2:
                vals.append(0)
            else:
                val = float(x[1])/1000
                vals.append(val)
        results[u].append(vals)

users = [1, 30]
disguiser = [0, 1]
for u in users:
    for d in disguiser:
        get_data('results/websubmit_results/concurrent_{}users_0sleep_{}disguisers.csv'.format(u, d),
                op_results, 1, u)
        get_data('results/websubmit_results/concurrent_{}users_0sleep_{}disguisers_txn.csv'.format(u, d),
                op_results_txn, 1, u)

offset = 0.2

################ none
plt.bar((X-barwidth), [
    statistics.median(op_results[1][0]),
    statistics.median(op_results[30][0]),
],
yerr=get_yerr([
    op_results[1][0],
    op_results[30][0],
]),
color='g', capsize=5, width=barwidth, label="No Disguiser")
add_labels((X-barwidth),
[
    statistics.median(op_results[1][0]),
    statistics.median(op_results[30][0]),
], plt, 'g', offset)


################ disguiser
plt.bar((X), [
    statistics.median(op_results[1][1]),
    statistics.median(op_results[30][1]),
],
yerr=get_yerr([
    op_results[1][1],
    op_results[30][1],
]),
color='c', capsize=5, width=barwidth, label="1 Disguiser")
add_labels((X),
[
    statistics.median(op_results[1][1]),
    statistics.median(op_results[30][1]),
], plt, 'c', offset)

################ disguiser txn
plt.bar((X+barwidth), [
    statistics.median(op_results_txn[1][1]),
    statistics.median(op_results_txn[30][1]),
],
yerr=get_yerr([
    op_results_txn[1][1],
    op_results_txn[30][1],
]),
color='b', capsize=5, width=barwidth, label="1 Disguiser (Txn)")
add_labels((X+barwidth),
[
    statistics.median(op_results_txn[1][1]),
    statistics.median(op_results_txn[30][1]),
], plt, 'b', offset)


plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=8)
plt.xticks(X, labels=labels)

plt.ylabel('Latency (ms)')
plt.legend(loc="upper left")
plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results.pdf')
