import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')

sleeps = [100000, 0]
maxts = 150000
bucketwidth = 1000
nbuckets = int(maxts/bucketwidth)
buckets = [b * bucketwidth for b in range(nbuckets)]

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

# collect all results
op_results = []
delete_results = []

def get_opdata(filename, results, i):
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        opdata = defaultdict(list)
        for x in oppairs:
            bucket = int((float(x[0]))/bucketwidth)
            val = float(x[1])/1000
            opdata[bucket].append(val)
        results.append(opdata)

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

get_all_points('results/lobsters_results/concurrent_disguise_stats_1users_nodisguising.csv'.format(1), op_results, 1)
xs = op_results[0].keys()
ys = op_results[0].values()
label ='1 Normal Users: {}'.format("No Disguiser")
plt.scatter(xs, ys, label=label)
plt.ylim(ymin=0, ymax=200)
plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results_timeseries_nodisguiser.pdf')

plt.clf()
users = [1, 30]
for u in users:
    get_opdata('results/lobsters_results/concurrent_disguise_stats_{}users_expensive.csv'.format(u), op_results, 1)
    get_opdata('results/lobsters_results/concurrent_disguise_stats_{}users_cheap.csv'.format(u), op_results, 1)
    get_opdata('results/lobsters_results/concurrent_disguise_stats_{}users_expensive.csv'.format(u), delete_results, 2)
    get_opdata('results/lobsters_results/concurrent_disguise_stats_{}users_cheap.csv'.format(u), delete_results, 2)

for index in range(2):
    xs = list(op_results[index].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in op_results[index].values()]
    ys = np.array(ys)[order]
    label ='{} Normal Users: {}'.format(users[0], "Expensive Disguiser")
    if index == 1:
        label='{} Normal Users: {}'.format(users[0], "Cheap Disguiser")
    plt.plot(xs, ys, label=label)

for index in range(2,4):
    xs = list(op_results[index].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in op_results[index].values()]
    ys = np.array(ys)[order]
    label ='{} Normal Users: {}'.format(users[1], "Expensive Disguiser")
    if index == 3:
        label='{} Normal Users: {}'.format(users[1], "Cheap Disguiser")
    plt.plot(xs, ys, label=label)

    xs = list(delete_results[index].keys())
    order = np.argsort(xs)
    xs = np.array(xs)[order]
    ys = [statistics.mean(x) for x in delete_results[index].values()]
    ys = np.array(ys)[order]
    label ='Delete {} Normal Users: {}'.format(users[1], "Expensive Disguiser")
    if index == 3:
        label='Delete {} Normal Users: {}'.format(users[1], "Cheap Disguiser")
    #plt.plot(xs, ys, label=label)

    plt.xlabel('Benchmark Time (s)')
    plt.ylabel('Latency (ms)')
    plt.ylim(ymin=0, ymax=500)
    plt.xlim(xmin=0, xmax=100)
    plt.legend(loc="upper right")
    plt.title("Lobsters Op Latency vs. Number Normal Users")

plt.tight_layout(h_pad=4)
plt.savefig('lobsters_concurrent_results_timeseries.pdf')
