import matplotlib.pyplot as plt

files = [
    "reads_baseline1",
    "reads_test1",
    "reads_test2",
    "reads_baseline2",
    "reads_baseline3",
    "reads_test3",

    "updates_baseline1",
    "updates_test1",
    "updates_test2",
    "updates_baseline2",
    "updates_baseline3",
    "updates_test3",

    "deletes_baseline1",
    "deletes_test1",
    "deletes_test2",
    "deletes_baseline2",
    "deletes_baseline3",
    "deletes_test3",
]

for filename in files:
    with open("{}.csv".format(filename), 'r') as f:
        y = []
        for line in f.readlines():
            print(line)
            y += [float(x)/1000 for x in line.strip('[]').split(' ')[:-1]]
        print(y)
        plt.hist(y, 5)
        plt.ylim((0,1000))
        plt.xlim((0,670))
        plt.xlabel('Request Completion Time (s)')
        plt.ylabel('Number of Requests')
        plt.savefig("{}.png".format(filename))
        plt.clf()
