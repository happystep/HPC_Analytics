import pandas as pd

headers_ = pd.read_csv('headers.csv')

headers_list = []

for head in headers_:
    headers_list.append(head) # just for me to know the headers.

# I NEED TO WORK ON STD AND CNT


data = pd.read_csv('16mInfo+reqTM.csv') #, chunksize=10, iterator=True  other parameters that I will not be using
# right away


stdcpu = data.groupby('owner', as_index=False)['cpu'].std()

print(stdcpu)

# stdmem = data.groupby('owner', as_index=False)['owner', 'maxvmem'].std()
#
# print(stdmem)