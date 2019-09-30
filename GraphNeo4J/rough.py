import pandas as pd

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
# okay last but not least.... we have slurmUserBasedMemory? lol this one I parsed to have
url = "http://people.cs.ksu.edu/~happystep/HPC/slurmUserBasedMemory.csv"

data = pd.read_csv(url, nrows=3)
# we will do this to understand the types of the entries, to then infer for reading into Neo4j
data.to_csv("slurm_three_rows.csv")

# data = pd.read_csv(url)
#
# sample = data[:3]
# file = open("sample.txt", "w+")
# for i in sample:
#     file.write(i)
#     file.write("\n")
# file.close()

# file = open("columns.txt", "w+")
# for i in data.columns.unique():
#     file.write(i + "\n")
# file.close()

# we are going to instead print every item in l on its own line to read better
#
# string_set = []
#
# for i in l:
#     string_set.append("CREATE INDEX ON :USER ({0});".format(i))
#
# for i in string_set:
#     print(i)
