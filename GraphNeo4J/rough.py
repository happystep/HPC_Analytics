import pandas as pd

# url = "http://people.cs.ksu.edu/~happystep/HPC/slurmCleaned"
# there is also a cleaned version for demographic, graph representation learning.
# further explanation of what makes this dataset different, need to consider using it for loading rows to neo4j
# this one has user name as a feature, but the other doesn't. Other differences between these two data-sets are instance
# and feature. You will see the details if you play with them, e.g. one has around 10 millions instances, other has around 3.7 millions.
url = "http://people.cs.ksu.edu/~huichen/hpc/data-set/slurmUserbased"

data = pd.read_csv(url)

file = open("columns.txt", "w+")

for i in data.columns.unique():
    file.write(i + "\n")


# we are going to instead print every item in l on its own line to read better
#
# string_set = []
#
# for i in l:
#     string_set.append("CREATE INDEX ON :USER ({0});".format(i))
#
# for i in string_set:
#     print(i)
