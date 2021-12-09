import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

# collect all results
op_results = defaultdict(list)
delete_results = []

def get_opdata(filename, results, i, u):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            opdata[bucket].append(val)
        results[u].append(opdata)

def get_all_points(filename, results, i):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = {}
        for x in oppairs:
            key = float(x[0])
            val = float(x[1])/1000
            opdata[key] = val
        results.append(opdata)

users = [1, 30]
disguiser = ['cheap', 'expensive', 'none']
for u in users:
    for d in disguiser:
        get_opdata('results/lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                op_results, 1, u)

for u in users:
    for index in range(3):
        xs = list(op_results[u][index].keys())
        order = np.argsort(xs)
        xs = np.array(xs)[order]
        ys = [statistics.mean(x) for x in op_results[u][index].values()]
        ys = np.array(ys)[order]
        label ='{} Normal Users: {}'.format(u, disguiser[index])
        plt.plot(xs, ys, label=label)

#for index in range(3,6):
    #xs = list(op_results[index].keys())
    #order = np.argsort(xs)
    #xs = np.array(xs)[order]
    #ys = [statistics.mean(x) for x in op_results[index].values()]
    #ys = np.array(ys)[order]
    #label ='{} Normal Users: {}'.format(users[0], "No Disguiser")
    #if index == 4:
        ##label ='{} Normal Users: {}'.format(users[0], "Expensive Disguiser")
    #if index == 5:
        #label='{} Normal Users: {}'.format(users[0], "Cheap Disguiser")
    #plt.plot(xs, ys, label=label)

    #xs = list(delete_results[index].keys())
    #order = np.argsort(xs)
    #xs = np.array(xs)[order]
    #ys = [statistics.mean(x) for x in delete_results[index].values()]
    #ys = np.array(ys)[order]
    #label ='Delete {} Normal Users: {}'.format(users[1], "Expensive Disguiser")
    #if index == 3:
        #label='Delete {} Normal Users: {}'.format(users[1], "Cheap Disguiser")
    #plt.plot(xs, ys, label=label)

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0, ymax=40)
    plt.xlim(xmin=0, xmax=50)
    plt.legend(loc="upper right")
    plt.title("Lobsters Op Latency vs. Number Normal Users")

plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results_timeseries.pdf')
