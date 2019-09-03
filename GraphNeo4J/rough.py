import pandas as pd

url = "http://people.cs.ksu.edu/~happystep/HPC/slurmCleaned"

data = pd.read_csv(url)

l = []
for i in data.columns.unique():
    l.append(i)

# we are going to instead print every item in l on its own line to read better
for i in l:
    print(i)

#
# string_set = []
#
# for i in l:
#     string_set.append("CREATE INDEX ON :USER ({0});".format(i))
#
# for i in string_set:
#     print(i)
