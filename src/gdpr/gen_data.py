#!/usr/bin/python3

import random
import os
import numpy as np
import multiprocessing

ids = []
rng = random.SystemRandom()

def get_existing_ids(num_users):
        for user in range(num_users):
                filename = "data/{}/TextRecord.csv".format(user)
                try:
                        with open(filename, 'r') as f:
                                for line in f.readlines():
                                        ids.append(int(line.split(',')[0]))
                except:
                   return

def gen_user_articles(num_art, user):
        usrstr = "hello world from %d" % user
        version = 1
        i = 0
        filename = "data/{}/TextRecord.csv".format(user)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'a') as f:
                while i < num_art:
                        newid = rng.randint(0, 2**63-1)
                        ids.append(newid)
                        f.write("{},{},{},{}\n".format(newid, -1, version, usrstr))
                        version += 1
                        i += 1
        return version

def gen_user_comments_and_votes(count, user, version):
        version = gen_user_comments(count*2, user, version)
        gen_user_votes(count*3, user, version)

def gen_user_comments(num_com, user, version):
        usrstr = "comment from %d" % user
        i = 0
        filename = "data/{}/TextRecord.csv".format(user)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'a') as f:
                while i < num_com:
                        newid = rng.randint(0, 2**63-1)
                        parentid = random.sample(ids, 1)[0]
                        ids.append(newid)
                        f.write("{},{},{},{}\n".format(newid, parentid, version, usrstr))
                        version += 1
                        i += 1
        return version

def gen_user_votes(num_votes, user, version):
        i = 0
        filename = "data/{}/Vote.csv".format(user)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'a') as f:
                while i < num_votes:
                        parentid = random.sample(ids, 1)[0]
                        f.write("{},{},{}\n".format(parentid, version, random.choice([0, 1])))
                        version += 1
                        i += 1

def gen_user_shards(num_users):
        get_existing_ids(num_users)
        min_count = 1000
        user_counts = np.multiply(min_count, np.random.zipf(2.75, num_users))
        print(max(user_counts))
        user_counts[0] = 100000
        args = list(zip(user_counts, range(num_users)))
        with multiprocessing.Pool(processes=None) as pool:
            versions = pool.starmap(gen_user_articles, args)
            args = list(zip(user_counts, range(num_users), versions))
            pool.starmap(gen_user_comments_and_votes, args)

gen_user_shards(1000)
