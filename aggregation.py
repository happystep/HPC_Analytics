import pandas as pd

df = pd.read_csv('16mInfo+reqTMcuma.csv')

df.shape
print(df.shape)
print(df.dtypes)

temp = df[['owner', 'failed', 'project', 'cpu', 'maxvmem', 'reqTime', 'reqMem', 'people','submission_time',
    'job_number']]
minM = temp.groupby('owner', as_index=False)['cpu', 'reqMem'].transform('min')
minM.columns = ['minCPU', 'minReqMem']

maxM = temp.groupby('owner', as_index=False)['cpu', 'reqMem'].transform('max')
maxM.columns = ['maxCPU', 'maxReqMem']

std = temp.groupby('owner', as_index=False)['cpu', 'reqMem'].std()
cnt = temp.groupby('owner', as_index=False)['job_number'].count(axis='columns')

df = df.join(std)
df = df.join(cnt)
df = df.join(minM)
df = df.join(maxM)
print(df.shape)
df.to_csv('16mInfo+reqTMcumamaxminstdcount.csv')

