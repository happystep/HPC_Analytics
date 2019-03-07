import pandas as pd

df = pd.read_csv('16mInfo+reqTMcuma.csv')

df.shape
print(df.shape)

temp = df[['owner', 'failed', 'project', 'cpu', 'maxvmem', 'reqTime', 'reqMem', 'people','submission_time', 'job_number']]
minM = temp.groupby('owner', as_index=False)['cpu', 'reqMem'].transform('min')
minM.columns = ['minCPU', 'minReqMem']

maxM = temp.groupby('owner', as_index=False)['cpu', 'reqMem'].transform('max')
maxM.columns = ['maxCPU', 'maxReqMem']

std = temp.groupby('owner')['cpu', 'reqMem'].transform('std')
std.columns = ['stdCPU', 'stdReqMem']
cnt = temp.groupby('owner', as_index=False)['job_number'].transform('count')
cnt.columns = ['cntUser']

df = df.join(minM)
df = df.join(maxM)
df = df.join(std)
df = df.join(cnt)

print(df)
df.to_csv('16mInfo+reqTMcumamaxminstdcnt.csv')