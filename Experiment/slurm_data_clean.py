import pandas as pd
import time
import numpy as np
import datetime
import re

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



url = 'http://people.cs.ksu.edu/~happystep/HPC/merged_set.csv'
df = pd.read_csv(url)

print("Total Dataset:")
print(df.shape)

gd = df.groupby('User', as_index=False)

t1 = gd.filter(lambda x : len(x) > 6) # mean or 6
# GAUSIAN DISTRIBUTION MEAN IS WHAT SHOULD BE USED HERE --- find this number - 6
print("Total Dataset after filter:")
print(t1.shape)

# reduce columns
t2 = t1

t3 = t2.dropna(axis=0, how='any', thresh=3) # I'm . going to try the tresh of 3/7 total features
print("Total Dataset after dropna:")
print(t3.shape)
t4 = t3.fillna('0M')

t5 = t4

count = 0
new_col = {}
for x in t5.State:
    y = x.split(' ')
    new_col[count] = y[0]
    count += 1

curr = t5.drop(['State'], axis=1)

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

print(t5)


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

print(t11)
print(t11.Timelimit.unique())


t12 = t11

# roles need to be cleaned

print(t12.role.unique())

count = 0
new_col = {}
for x in t12.role:
    #y = x.split(' ')
    new_col[count] = x
    count += 1

curr = t12.drop(['role'], axis=1)

curr_data = pd.Series(new_col).to_frame('role')
newdf_ = pd.DataFrame(curr_data)

t13 = pd.concat([curr, newdf_], axis=1)
print(t13.role.unique())

t13.role[t13.role == 'role_0'] = 0
t13.role[t13.role == 'role_1'] = 1
t13.role[t13.role == 'role_2'] = 2
t13.role[t13.role == '0M'] = np.nan

print(t13.role.unique())

t13.to_csv('../Data/slurm_role_cleaned.csv')