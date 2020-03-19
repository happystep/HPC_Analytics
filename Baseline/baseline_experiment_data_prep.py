import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

def getMem(col):
    mem = 0
    if type(col) is not float:
                temp = col[-2:]
                if 'M' in temp or 'm' in temp:
                    mem = int(col[:-2]) / 1024
                elif 'T' in temp or 't' in temp:
                    mem = int(col[:-2]) * 1024
                else:
                    mem = col[:-2]

    return mem



url = 'http://people.cs.ksu.edu/~happystep/HPC/slurm_sample_cleaned.csv'
df = pd.read_csv(url)

print(df.shape)
print(df.columns.values)


ddr = df[['State', 'ReqMem', 'Timelimit', 'User', 'Account']]

print(ddr.shape)
print(ddr.columns.values)


newdf = ddr

print(newdf.shape)

count = 0
new_col = {}
for x in newdf.State:
    y = x.split(' ')
    new_col[count] = y[0]
    count += 1

curr = newdf.drop(['State'], axis=1)

curr_data = pd.Series(new_col).to_frame('State')
newdf_ = pd.DataFrame(curr_data)

t5 = pd.concat([curr, newdf_], axis=1)
print(t5.State.unique())

t5.State[t5.State == 'COMPLETED'] = 0
t5.State[t5.State == 'TIMEOUT'] = 1
t5.State[t5.State == 'PREEMPTED'] = 1
t5.State[t5.State == 'REQUEUED'] = 1
t5.State[t5.State == 'FAILED'] = 1
t5.State[t5.State == 'RUNNING'] = 1
t5.State[t5.State == 'OUT_OF_MEMORY'] = 1
t5.State[t5.State == 'CANCELLED'] = 1
t5.State[t5.State == 'NODE_FAIL'] = 1
t5.State[t5.State == 'PENDING'] = 1

t8 = t5

print(t8.ReqMem.unique())

t8['ReqMem'] = df.ReqMem.apply(lambda x: getMem(x))

print(t8.ReqMem.unique())

t9 = t8
print(t9.Timelimit.unique())

# format of TimeLimit  [days-]hours:minutes:seconds[.microseconds]
new_frame_timelimit = {} # empty dictionary
count = 0
for x in t9.Timelimit:
    if type(x) == float:
        continue
    if '-' in x:
        y = x.split('-')
        hours = 24*(y[0])
        if len(y) >= 1:
            z = y[1].split(':')
            total_hours = hours + z[0]
            new_date_time = total_hours + ":" + z[1] + ":" + z[2]
            new_frame_timelimit[count] = new_date_time
    else:
        new_frame_timelimit[count] = x
    count += 1

# this should put it into seconds
for key, value in new_frame_timelimit.items():
    r = value.split(':')
    if len(r) == 1:
        if r[0] == '0M':
            new_frame_timelimit[key] = 0
        else:
            new_frame_timelimit[key] = float(r[0])
    elif len(r) == 2:
        new_frame_timelimit[key] = float(r[0]) * 60 + float(r[1])
    else:
        new_frame_timelimit[key] = float(r[0]) * 3600 + float(r[1]) * 60 + float(r[2])

t10 = t9.drop(['Timelimit'], axis=1)

data_timelimit = pd.Series(new_frame_timelimit).to_frame('Timelimit')
newdf_timelimit = pd.DataFrame(data_timelimit)

t11 = pd.concat([t10, newdf_timelimit], axis=1)


print(t11.Timelimit.unique())

t12 = t11

print(t12.shape)

t13 = t12.dropna()

t13.to_csv('baseline_experiment.csv')
