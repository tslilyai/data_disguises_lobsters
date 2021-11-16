import matplotlib.pyplot as plt
import csv
import statistics
import sys

plt.style.use('seaborn-deep')
nusers = [10, 20, 30, 50, 70, 100]
lec = sys.argv[1]

# collect all results, compute maximum latency over all tests + all query  types
account_results = []
anon_results = []
edit_results = []
delete_results = []
restore_results = []

account_results_baseline = []
anon_results_baseline = []
edit_results_baseline = []
delete_results_baseline = []
restore_results_baseline = []

for u in nusers:
    with open('disguise_stats_{}lec_{}users.csv'.format(lec, u),'r') as csvfile:
        rows = csvfile.readlines()
        account_durs = [int(x) for x in rows[0].strip().split(',')]
        anon_durs = [int(x) for x in rows[1].strip().split(',')]
        edit_durs = [int(x) for x in rows[2].strip().split(',')]
        delete_durs = [int(x) for x in rows[3].strip().split(',')]
        restore_durs = [int(x) for x in rows[4].strip().split(',')]

        account_results.append(statistics.mean(account_durs))
        anon_results.append(statistics.mean(anon_durs))
        edit_results.append(statistics.mean(edit_durs))
        delete_results.append(statistics.mean(delete_durs))
        restore_results.append(statistics.mean(restore_durs))

    with open('disguise_stats_{}lec_{}users_baseline.csv'.format(lec, u),'r') as csvfile:
        rows = csvfile.readlines()
        account_durs = [int(x) for x in rows[0].strip().split(',')]
        anon_durs = [int(x) for x in rows[1].strip().split(',')]
        edit_durs = [int(x) for x in rows[2].strip().split(',')]
        delete_durs = [int(x) for x in rows[3].strip().split(',')]
        restore_durs = [0]

        account_results_baseline.append(statistics.mean(account_durs))
        anon_results_baseline.append(statistics.mean(anon_durs))
        edit_results_baseline.append(statistics.mean(edit_durs))
        delete_results_baseline.append(statistics.mean(delete_durs))
        restore_results_baseline.append(statistics.mean(restore_durs))

fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(8,15))
axes_flat = axes.flatten()
axes_flat[0].plot(nusers, account_results, label='edna', color='r')
axes_flat[0].plot(nusers, account_results_baseline, label='baseline', color='b')
axes_flat[0].set_title("Average Time to Create Account")
axes_flat[0].set_xlabel('Number of users')
axes_flat[0].set_ylabel('Time (mus)')
axes_flat[0].set_ylim(ymin=0)
axes_flat[0].legend(loc='upper left');

axes_flat[1].plot(nusers, anon_results, label='edna', color='r')
axes_flat[1].plot(nusers, anon_results_baseline, label='baseline', color='b')
axes_flat[1].set_title("Time to Anonymize All anons")
axes_flat[1].set_xlabel('Number of users')
axes_flat[1].set_ylabel('Time (mus)')
axes_flat[1].set_ylim(ymin=0)

axes_flat[2].plot(nusers, edit_results, label='edna', color='r')
axes_flat[2].plot(nusers, edit_results_baseline, label='baseline', color='b')
axes_flat[2].set_title("Average Time to Edit Answers to Lecture")
axes_flat[2].set_xlabel('Number of users')
axes_flat[2].set_ylabel('Time (mus)')
axes_flat[2].set_ylim(ymin=0)

axes_flat[3].plot(nusers, delete_results, label='edna', color='r')
axes_flat[3].plot(nusers, delete_results_baseline, label='baseline', color='b')
axes_flat[3].set_title("Average Time to Delete Account")
axes_flat[3].set_xlabel('Number of users')
axes_flat[3].set_ylabel('Time (mus)')
axes_flat[3].set_ylim(ymin=0)

axes_flat[4].plot(nusers, restore_results, label='edna', color='r')
axes_flat[4].plot(nusers, restore_results_baseline, label='baseline', color='b')
axes_flat[4].set_title("Average Time to Restore Account")
axes_flat[4].set_xlabel('Number of users')
axes_flat[4].set_ylabel('Time (mus)')
axes_flat[4].set_ylim(ymin=0)

fig.tight_layout(h_pad=4)
plt.savefig('results_{}lec.png'.format(lec), dpi=300)
