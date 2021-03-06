import pandas as pd
import re
import numpy as np

# url = "http://people.cs.ksu.edu/~happystep/HPC/slurmCleaned"
# there is also a cleaned version for demographic, graph representation learning.
# further explanation of what makes this dataset different, need to consider using it for loading rows to neo4j
# this one has user name as a feature, but the other doesn't. Other differences between these two data-sets are instance
# and feature.
# You will see the details if you play with them, e.g. one has around 10 millions instances,
# other has around 3.7 millions.
# url = "http://people.cs.ksu.edu/~happystep/HPC/slurmUserbased"
# now there is a data-set that is parsed even further.
# 1. Survey feedback are added (q5, q6, q7);
# 2. AllocMemTRES is parsed from AllocTRES, ReqMemTRES is parsed from ReqTRES.
# url = "http://people.cs.ksu.edu/~happystep/HPC/slurmUserSurvey"
# okay last but not least.... we have slurmUserBasedMemory? lol this one I parsed to have, this now also includes dep,
# and other demo

# parsing mem from AllocTRES and ReqTRES
def getMem(col):
    mem = 0
    if type(col) is not float:
        for c in col.split(','):
            if 'mem=' in c:
                temp = c.split('mem=')[1]
                if 'M' in temp or 'm' in temp:
                    mem = np.float16(re.sub(r'M|m', '', temp)) / 1024
                elif 'T' in temp or 't' in temp:
                    mem = np.float16(re.sub(r'T|t', '', temp)) * 1024
                else:
                    mem = re.sub(r'G|g', '', temp)

    return mem




url = "http://people.cs.ksu.edu/~happystep/HPC/slurmUserBasedMemory.csv"


url = "http://people.cs.ksu.edu/~happystep/HPC/slurm_sample.csv"
df = pd.read_csv(url)
# sample = data.sample(1000000)
# # print(data)
# # print(data.columns)
# # # we will do this to understand the types of the entries, to then infer for reading into Neo4j
#
# sample.to_csv("slurm_sample.csv")
df['AllocMemTRES'] = df.AllocTRES.apply(lambda x: getMem(x))
df['ReqMemTRES'] = df.ReqTRES.apply(lambda x: getMem(x))
df.to_csv("slurm_sample_cleaned.csv")

# l = []
# file = open("columns.txt", "r")
# for i in file:
#     l.append(i)
# file.close()
#
# # we are going to instead print every item in l on its own line to read better
# # this code below is to create the index statments for the database
#
# string_set = []
#
# for i in l:
#     string_set.append("CREATE INDEX ON :USER ({0});".format(i))

# for i in string_set:
#     print(i)
