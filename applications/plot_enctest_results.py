import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict

plt.style.use('seaborn-deep')
fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(6,4))

xs = []
encs = []
decs = []
filename = "results/enctest_results/enc_stats.csv"
with open(filename,'r') as csvfile:
    rows = csvfile.readlines()
    for r in rows:
        res = r.strip().split(',')
        xs.append(int(res[0]))
        encs.append(float(res[1])/1000)
        decs.append(float(res[2])/1000)

plt.plot(xs, encs, label="Encrypt")
plt.plot(xs, decs, label="Decrypt")
plt.xlabel('Size of Batch')
plt.ylabel('Latency (ms)')
plt.ylim(ymin=0)
plt.xlim(xmin=0)
plt.legend(loc="upper right")
plt.title("Encryption/Decryption Cost vs. Size of Data")
plt.tight_layout(h_pad=4)
plt.savefig("enctest_stats.pdf")
