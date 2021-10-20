import numpy as np
import matplotlib.pyplot as plt
import csv

plt.style.use('seaborn-deep')
num_qtypes = 6

tests = ["decor", "shim_only", "shim_parse"]
names = ["Read", "Update", "Insert", "Unsub", "Resub", "Delete"]
ybounds = [1000000, 1000000, 1000000, 10000, 6000, 6000]

test2latencies = {}
bins = [None] * num_qtypes
max_latency_dict = {}

# collect all results, compute maximum latency over all tests + all query  types
for test in tests:
    with open('{}.csv'.format(test),'r') as csvfile:
        rows = csvfile.readlines()

        q2lats_all = []
        for (i, row) in enumerate(rows):
            # only reads/inserts/updates for now
            if i > 2:
                continue
            q2lats = [[] for _ in range(4)]
            pairs = row.split(';')[:-1]
            if len(pairs) == 0:
                continue
            for p in pairs:
                p = p.split(',')
                qs = int(p[0])
                latency = float(p[1])

                # skip the abnormal number of queries at the beginning
                if qs > 5:
                    continue
                q2lats[qs].append(latency)
            q2lats_all.append((q2lats, i))

        test2latencies[test] = q2lats_all

        for ai in range(len(q2lats_all)):
            qtype_index = q2lats_all[ai][1]
            max_latency = max([max(x, default=0) for x in q2lats_all[ai][0]], default=0)
            if qtype_index in max_latency_dict.keys():
                curmax = max_latency_dict[qtype_index]
                max_latency_dict[qtype_index] = max(curmax, max_latency)
            else:
                max_latency_dict[qtype_index] = max_latency

# create bins appropriately
print(max_latency_dict)
for qtype_index in max_latency_dict.keys():
    bins[qtype_index] = np.linspace(0, max_latency_dict[qtype_index], 400)

# actually plot the tests
for test in tests:
    fig, axes = plt.subplots(nrows=len(q2lats_all), ncols=1, figsize=(8,len(q2lats_all)*3))
    axes_flat = axes.flatten()
    q2lats_all = test2latencies[test]
    qtype_index = q2lats_all[ai][1]
    for ai in range(len(q2lats_all)):
        qtype_index = q2lats_all[ai][1]
        axes_flat[ai].hist(q2lats_all[ai][0],
                bins[qtype_index],
                #density=True, histtype='step', cumulative=True,
                stacked=True,
                label=[str(nqueries) + "x Query Mult." for nqueries in range(len(q2lats))])
        axes_flat[ai].legend(loc='upper right')
        axes_flat[ai].set_title(names[qtype_index]+" Queries Latency Histogram")
        axes_flat[ai].set_yscale('log')
        axes_flat[ai].set_ylim(ymax = ybounds[qtype_index])
        axes_flat[ai].set_xlabel('Per-Query Latency (us)')
        axes_flat[ai].set_ylabel('Number of Queries')
    fig.suptitle('{}'.format(test))
    fig.tight_layout(h_pad=4)
    plt.savefig('{}.png'.format(test), dpi=300)
