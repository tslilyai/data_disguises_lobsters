import numpy as np
import matplotlib.pyplot as plt
import csv

plt.style.use('seaborn-deep')

tests = ["decor", "shim_only", "shim_parse"]
names = ["Read", "Update", "Insert", "Other"]
ybounds = [15000, 2000, 1400, 600]
bins = [np.linspace(0, 14000, 200),
    np.linspace(0, 800, 200),
    np.linspace(0, 800, 200),
    np.linspace(0, 600, 200),
]

for test in tests:
    with open('{}1.csv'.format(test),'r') as csvfile:
        rows = csvfile.readlines()
        fig, axes = plt.subplots(nrows=len(rows), ncols=1, figsize=(6,8))
        axes_flat = axes.flatten()
        for (i, row) in enumerate(rows):
            q2lats = [[] for _ in range(4)]
            pairs = row.split(';')[:-1]
            for p in pairs:
                p = p.split(',')
                qs = int(p[0])
                latency = float(p[1])

                # skip the abnormal number of queries at the beginning
                if qs > 5:
                    continue
                q2lats[qs].append(latency)
            axes_flat[i].hist(q2lats,
                    bins[i],
                    stacked=True,
                    label=[str(i) + "x Query Mult." for i in range(len(q2lats))])
            axes_flat[i].legend(loc='upper right')
            axes_flat[i].set_title(names[i]+" Queries Latency Histogram")
            axes_flat[i].set_ybound(lower=0, upper=ybounds[i])
            axes_flat[i].set_yscale('log')
            axes_flat[i].set_xlabel('Per-Query Latency (us)')
            axes_flat[i].set_ylabel('Number of Queries')
    fig.tight_layout(h_pad=4)
    plt.savefig('{}.png'.format(test), dpi=300)
