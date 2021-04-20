import numpy as np
import matplotlib.pyplot as plt
import csv

PC_UID = 300

nqueries = []
nqueries_vault = []
latencies = []

nqueries_pc = []
nqueries_vault_pc = []
latencies_pc = []

with open('out','r') as csvfile:
    rows = csvfile.readlines()
    for row in rows[2:]:
        p = row.split(',')
        if int(p[0]) > PC_UID:
            nqueries_pc.append(int(p[1]))
            nqueries_vault_pc.append(int(p[2]))
            latencies_pc.append(float(p[3]))
        else:
            nqueries.append(int(p[1]))
            nqueries_vault.append(int(p[2]))
            latencies.append(float(p[3]))

fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(8,9))
axes_flat = axes.flatten()
axes_flat[0].hist(nqueries, np.linspace(0, 250, 100), stacked=True, label="NonPC Members")
axes_flat[0].set_xlabel('#AppDB Queries Performed by Disguise')
axes_flat[0].set_ylabel('# GDPRRemoval Disguises')

axes_flat[1].hist(nqueries_vault, np.linspace(0, 250, 100), stacked=True)
axes_flat[1].set_xlabel('#Vault Queries Performed by Disguise')
axes_flat[1].set_ylabel('#GDPRRemoval Disguises')

axes_flat[2].hist(latencies, np.linspace(0, 400, 100), stacked=True)
axes_flat[2].set_xlabel('Latency of Disguise(ms)')
axes_flat[2].set_ylabel('#GDPRRemoval Disguises')

axes_flat[0].hist(nqueries_pc, np.linspace(0, 250, 100), stacked=True, color='red', label='PC members')
axes_flat[0].set_xlabel('#AppDB Queries Performed by Disguise')
axes_flat[0].set_ylabel('#GDPRRemoval Disguises')

axes_flat[1].hist(nqueries_vault_pc, np.linspace(0, 250, 100), stacked=True, color='red')
axes_flat[1].set_xlabel('#Vault Queries Performed by Disguise')
axes_flat[1].set_ylabel('#GDPRRemoval Disguises')

axes_flat[2].hist(latencies_pc, np.linspace(0, 400, 100), stacked=True, color='red')
axes_flat[2].set_xlabel('Latency of Disguise(ms)')
axes_flat[2].set_ylabel('#GDPRRemoval Disguises')

fig.tight_layout(h_pad=4)
fig.legend(loc='upper right')
plt.savefig('{}.pdf'.format("pc_users"), dpi=250)
