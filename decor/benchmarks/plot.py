import numpy as np
import matplotlib.pyplot as plt
import csv

plt.style.use('seaborn-deep')

tests = ["decor", "shim_only", "shim_parse"]
names = ["Read", "Update", "Insert", "Unsub", "Resub", "Delete"]
ybounds = [1000000, 1000000, 1000000, 10000, 6000, 6000]
bins = [np.linspace(0, 20000, 200),
    np.linspace(0, 20000, 400),
    np.linspace(0, 20000, 400),
    np.linspace(0, 20000, 400),
    np.linspace(0, 20000, 400),
    np.linspace(0, 20000, 400),
]

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
        fig, axes = plt.subplots(nrows=len(q2lats_all), ncols=1, figsize=(8,len(q2lats_all)*3))
        axes_flat = axes.flatten()
        for ai in range(len(q2lats_all)):
            i = q2lats_all[ai][1]
            axes_flat[ai].hist(q2lats_all[ai][0],
                    bins[i],
                    stacked=True,
                    label=[str(i) + "x Query Mult." for i in range(len(q2lats))])
            axes_flat[ai].legend(loc='upper right')
            axes_flat[ai].set_title(names[i]+" Queries Latency Histogram")
            axes_flat[ai].set_yscale('log')
            axes_flat[ai].set_ylim(ymax = ybounds[i])
            axes_flat[ai].set_xlabel('Per-Query Latency (us)')
            axes_flat[ai].set_ylabel('Number of Queries')
    fig.suptitle('{}'.format(test))
    fig.tight_layout(h_pad=4)
    plt.savefig('{}.png'.format(test), dpi=300)
