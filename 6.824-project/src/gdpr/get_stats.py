import matplotlib.pyplot as plt
import statistics

def getStats():
    ops = ['reads', 'updates', 'deletes']
    is_tests = ['baseline', 'test']
    policies = ['Retain', 'Revoke', "RevokeDel"]
    for policy in policies:
        for op in ops:
            for is_test in is_tests:
                filename = "{}_{}_vote_{}_text_{}".format(op, is_test, policy, policy)
                print(filename)
                with open("{}.csv".format(filename), 'r') as f:
                    times = []
                    for line in f.readlines():
                        if line == '\n' or '[]' in line:
                            continue
                        times += ([float(x) for x in line.strip()[1:-1].split(' ')])
                    if len(times) == 0:
                        continue
                    print("\t{:d}: {:.0f}, {:.0f}, {:.0f} ({:.0f}-{:.0f})".format(len(times), statistics.mean(times), statistics.median(times), statistics.stdev(times), max(times), min(times)))

def plotFiles(files):
    for filename in files:
        with open("{}.csv".format(filename), 'r') as f:
            y = []

            for line in f.readlines():
                y += [float(x) for x in line.strip('[]').split(' ')[:-1]]
            plt.hist(y, 5)
            plt.ylim((0,1000))
            plt.xlim((0,670))
            plt.xlabel('Request Completion Time (s)')
            plt.ylabel('Number of Requests')
            plt.savefig("{}.png".format(filename))
            plt.clf()

getStats()
