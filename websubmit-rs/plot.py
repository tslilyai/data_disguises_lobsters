import numpy as np
import matplotlib.pyplot as plt
import csv
import statistics

plt.style.use('seaborn-deep')

tests = ["decor", "shim_only", "shim_parse"]
names = ["Read", "Update", "Insert", "Unsub", "Resub", "Delete"]
ybounds = [1000000, 1000000, 1000000, 10000, 6000, 6000]

nlec = [20, 40, 60]
nusers = [10, 30, 50, 70]

# collect all results, compute maximum latency over all tests + all query  types
for l in nlec:
    let account_results = []
    let anon_results = []
    let edit_results = []
    let delete_results = []
    let restore_results = []

    for u in nusers:
        with open('disguise_stats_{}lec_{}users.csv'.format(l, u),'r') as csvfile:
            rows = csvfile.readlines()
            let account_durs = [int(x) for x in rows[0].strip().split(',')]
            let anon_durs = [int(x) for x in rows[1].strip().split(',')]
            let edit_durs = [int(x) for x in rows[2].strip().split(',')]
            let delete_durs = [int(x) for x in rows[3].strip().split(',')]
            let restore_durs = [int(x) for x in rows[4].strip().split(',')]

            account_results.push(statistics.mean(account_durs))
            anon_results.push(statistics.mean(anon_durs))
            edit_results.push(statistics.mean(edit_durs))
            delete_results.push(statistics.mean(delete_durs))
            restore_results.push(statistics.mean(restore_durs))

    fig, axes = plt.subplots(nrows=len(nusers), ncols=1, figsize=(8,15))
    axes_flat = axes.flatten()
    axes_flat[0].plot(nusers, account_results)
    axes_flat[0].legend(loc='upper right')
    axes_flat[0].set_title("Average Time to Create Account")
    axes_flat[0].set_xlabel('Number of users')
    axes_flat[0].set_ylabel('Time (ms)')

    axes_flat[1].plot(nusers, anon_results)
    axes_flat[1].legend(loc='upper right')
    axes_flat[1].set_title("Time to Anonymize All Accounts")
    axes_flat[1].set_xlabel('Number of users')
    axes_flat[1].set_ylabel('Time (ms)')

    axes_flat[2].plot(nusers, edit_results)
    axes_flat[2].legend(loc='upper right')
    axes_flat[2].set_title("Average Time to Edit Answers to Lecture")
    axes_flat[2].set_xlabel('Number of users')
    axes_flat[2].set_ylabel('Time (ms)')

    axes_flat[3].plot(nusers, delete_results)
    axes_flat[3].legend(loc='upper right')
    axes_flat[3].set_title("Average Time to Delete Account")
    axes_flat[3].set_xlabel('Number of users')
    axes_flat[3].set_ylabel('Time (ms)')

    axes_flat[4].plot(nusers, restore_results)
    axes_flat[4].legend(loc='upper right')
    axes_flat[4].set_title("Average Time to Restore Account")
    axes_flat[4].set_xlabel('Number of users')
    axes_flat[4].set_ylabel('Time (ms)')

    fig.tight_layout(h_pad=4)
    plt.savefig('results_{}lec.png'.format(l), dpi=300)
