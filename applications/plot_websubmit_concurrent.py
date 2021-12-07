import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

sleeps = [100000, 0]
maxts = 150

# collect all results
edit_results = {}
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

def get_data(filename, results, i):
    vals = []
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        ndisguises = int(rows[0].strip())
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            val = float(x[1])/1000
            vals.append(val)
        results[ndisguises] = vals

for s in sleeps:
    get_data('results/websubmit_results/concurrent_disguise_stats_{}sleep_batch.csv'.format(s),
            edit_results, 1)

durs = []
xs = []
meds = []
for (ndisguises, results) in edit_results.items():
    xs.append((ndisguises/maxts))
    meds.append(statistics.median(results))
    durs.append(results)

myyerr = get_yerr(durs)
plt.plot(xs, meds)
plt.errorbar(xs, meds, yerr=myyerr, fmt="o")

plt.xlabel('Disguises/Sec')
plt.ylabel('Latency (ms)')
plt.ylim(ymin=0)
plt.xlim(xmin=0)
plt.title("WebSubmit Edit Latency vs. Disguises/Second")

plt.tight_layout(h_pad=4)
plt.savefig('websubmit_concurrent_results.pdf')
