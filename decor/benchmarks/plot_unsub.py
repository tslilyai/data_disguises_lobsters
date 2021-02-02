import numpy as np
import matplotlib.pyplot as plt
import csv

plt.style.use('seaborn-deep')
num_qtypes = 6

tests = ["decor_unsub"]
ybounds = [1000000, 1000000, 1000000, 10000, 6000, 6000]

# collect all results, compute maximum latency over all tests + all query  types
nobjs = []
disg = []
rev = []

for test in tests:
    with open('{}.out'.format(test),'r') as csvfile:
        rows = csvfile.readlines()
        for (i, row) in enumerate(rows):
            p = row.split(',')
            nobjs.append(int(p[1]))
            disg.append(int(p[2])/1000)
            rev.append(int(p[3])/1000)

    plt.figure(figsize=(5,3.5))
    plt.plot(nobjs, disg, color='red', linestyle='--', marker="o", label="Disguise")
    plt.plot(nobjs, rev, color='blue', marker="x", label="Reveal")
    plt.xlabel("#Objects Associated with User")
    plt.ylabel("Latency (ms)")
    plt.legend()
    plt.tight_layout()
    plt.savefig('{}.png'.format(test), dpi=300)
