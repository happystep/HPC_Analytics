import pandas as pd

url = "http://people.cs.ksu.edu/~happystep/HPC/test_hpc"

data = pd.read_csv(url)

l = []
for i in data.columns.unique():
    l.append(i)

string_set = []

for i in l:
    string_set.append("CREATE INDEX ON :USER ({0});".format(i))

for i in string_set:
    print(i)
