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
X = np.arange(3)
labels = ['1 User', '30 Users', '50 Users']

# collect all results
op_results = defaultdict(list)
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

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
        results[u].append(vals)

users = [1, 30, 50]
disguiser = ['none', 'cheap', 'expensive']
for u in users:
    for d in disguiser:
        get_data('results/lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                op_results, 1, u)

offset = 2

################ none
plt.bar((X-barwidth), [
    statistics.median(op_results[1][0]),
    statistics.median(op_results[30][0]),
    statistics.median(op_results[50][0]),
],
yerr=get_yerr([
    op_results[1][0],
    op_results[30][0],
    op_results[50][0],
]),
color='g', capsize=5, width=barwidth, label="No Disguiser")
add_labels((X-barwidth),
[
    statistics.median(op_results[1][0]),
    statistics.median(op_results[30][0]),
    statistics.median(op_results[50][0]),
], plt, 'g', offset)


################ cheap
plt.bar((X), [
    statistics.median(op_results[1][1]),
    statistics.median(op_results[30][1]),
    statistics.median(op_results[50][1]),
],
yerr=get_yerr([
    op_results[1][1],
    op_results[30][1],
    op_results[50][1],
]),
color='b', capsize=5, width=barwidth, label="Cheap Disguiser")
add_labels((X),
[
    statistics.median(op_results[1][1]),
    statistics.median(op_results[30][1]),
    statistics.median(op_results[50][1]),
], plt, 'b', offset)

################ expensive
plt.bar((X+barwidth), [
    statistics.median(op_results[1][2]),
    statistics.median(op_results[30][2]),
    statistics.median(op_results[50][2]),
],
yerr=get_yerr([
    op_results[1][2],
    op_results[30][2],
    op_results[50][2],
]),
color='m', capsize=5, width=barwidth, label="Expensive Disguiser")
add_labels((X+barwidth),
[
    statistics.median(op_results[1][2]),
    statistics.median(op_results[30][2]),
    statistics.median(op_results[50][2]),
], plt, 'm', offset)

plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=(np.percentile(op_results[50][2], 95)*1.15))
plt.xticks(X, labels=labels)

plt.ylabel('Latency (ms)')
plt.title("Lobsters Op Latency")
plt.legend()
plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results.pdf')
