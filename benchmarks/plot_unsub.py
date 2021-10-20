import numpy as np
import matplotlib.pyplot as plt
import csv

ybounds = [1000000, 1000000, 1000000, 10000, 6000, 6000]

# collect all results, compute maximum latency over all tests + all query  types
nobjs = []
disg = []
rev = []

with open('{}.out'.format("decor_unsub"),'r') as csvfile:
    rows = csvfile.readlines()
    for row in rows:
        p = row.split(',')
        nobjs.append(int(p[1]))

with open('{}.csv'.format("decor_unsub_queries"),'r') as csvfile:
    rows = csvfile.readlines()
    disrow = rows[0]
    revrow = rows[1]

    pairs = disrow.split(';')[:-1]
    for p in pairs:
        p = p.split(',')
        disg.append(int(p[0]))

    pairs = revrow.split(';')[:-1]
    for p in pairs:
        p = p.split(',')
        rev.append(int(p[0]))

plt.figure(figsize=(5,3))
plt.plot(nobjs, disg, color='red', linestyle='--', marker="o", label="Disguise")
plt.plot(nobjs, rev, color='blue', marker="x", label="Reveal")
plt.xlabel("#Objects Owned by User")
plt.ylabel("#Queries")
plt.legend()
plt.tight_layout()
plt.savefig('{}.pdf'.format("perf"), dpi=300)
