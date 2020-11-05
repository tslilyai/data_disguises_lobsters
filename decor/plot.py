import numpy as np
import matplotlib.pyplot as plt
import csv

plt.style.use('seaborn-deep')

test = ["decor", "shim_only", "shim_parse"]
names = ["Read", "Update", "Insert", "Other"]
bins = np.linspace(0, 800, 100)
with open('decor1.csv','r') as csvfile:
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
        #plt.xscale('log')
        #ax.yscale('log')
        axes_flat[i].hist(q2lats,
                bins,
                stacked=True,
                label=[str(i) + "x Query Mult." for i in range(len(q2lats))])
        axes_flat[i].legend(loc='upper right')
        axes_flat[i].set_title(names[i]+" Queries Latency Histogram")
        axes_flat[i].set_xlabel('Per-Query Latency (us)')
        axes_flat[i].set_ylabel('Number of Queries')
fig.tight_layout(h_pad=4)
plt.savefig('{}.png'.format(test[0]), dpi=300)
plt.show()
