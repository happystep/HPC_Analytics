import pandas as pd
import time
import numpy as np
import datetime
import re

path = './data/slurmData.txt'
df = pd.read_csv(path, sep='|', encoding='ISO-8859-1')

print("Total Dataset:")
print(df.shape)

gd = df.groupby('User', as_index=False)

t1 = gd.filter(lambda x : len(x) > 6) # mean or 6
# GAUSIAN DISTRIBUTION MEAN IS WHAT SHOULD BE USED HERE --- find this number - 6
print("Total Dataset after filter:")
print(t1.shape)

# reduce columns
t2 = t1[['User', 'State', 'Account', 'TotalCPU', 'MaxVMSize','ReqMem', 'Timelimit']]

t3 = t2.dropna(axis=0, how='any', thresh=3) # I'm . going to try the tresh of 3/7 total features
t3.shape
t4 = t3.fillna('0M')

t4['MaxVMSize'] = t4.MaxVMSize.apply(lambda x : (re.sub("\D",'',x)))

t4.drop(t4.index[t4['MaxVMSize'] == ''], inplace = True)

t4['MaxVMSize'] = t4['MaxVMSize'].astype(float)

average = t4.groupby('User', as_index=False)['MaxVMSize'].mean() # 'TotalCPU' also needs to be dealt with 
average.columns = ['User','aMaxVMSize']

t5 = pd.merge(t4, average, on=['User'])


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

# format of TOTAL CPU  [days-]hours:minutes:seconds[.microseconds]

new_frame = {} # empty dictionary
count = 0
for x in t5.TotalCPU:
    if '-' in x:
        y = x.split('-')
        hours = 24*(y[0])
        if len(y) >= 1:
            z = y[1].split(':')
            total_hours = hours + z[0]
            new_date_time = total_hours + ":" + z[1] + ":" + z[2]
            new_frame[count] = new_date_time
    else:
        new_frame[count] = x
    count += 1

# this should put it into seconds
for key, value in new_frame.items():
    r = value.split(':')
    if len(r) == 1:
        if r[0] == '0M':
            new_frame[key] = 0
        else:
            new_frame[key] = float(r[0])
    elif len(r) == 2:
        new_frame[key] = float(r[0]) * 60 + float(r[1])
    else:
        new_frame[key] = float(r[0]) * 3600 + float(r[1]) * 60 + float(r[2])

t6 = t5.drop(['TotalCPU'], axis=1)

data = pd.Series(new_frame).to_frame('TotalCPU')
newdf = pd.DataFrame(data)

t7 = pd.concat([t6, newdf], axis=1)

average_cpu = t7.groupby('User', as_index=False)['TotalCPU'].mean() # 'TotalCPU'
average_cpu.columns = ['User','aTotalCPU']
t8 = pd.merge(t7, average_cpu, on=['User'])
t8.columns

t8['ReqMem'] = t8.ReqMem.apply(lambda x : (re.sub("\D",'',x)))

t8.drop(t8.index[t8['ReqMem'] == ''], inplace = True)

t8['ReqMem'] = t8['ReqMem'].astype(float)

average_reqmem = t8.groupby('User', as_index=False)['ReqMem'].mean() #
average_reqmem.columns = ['User','aReqMem']

t9 = pd.merge(t8, average_reqmem, on=['User'])

print(t9.columns)
# expected is the following >
# 'User', Status', 'TotalCPU', 'MaxVMSize', 'Timelimit', 'ReqMem', 'aTotalCPU', 'aMaxVMSize', 'aTimelimit', 'aReqMem'

# format of TimeLimit  [days-]hours:minutes:seconds[.microseconds]

new_frame_timelimit = {} # empty dictionary
count = 0
for x in t9.Timelimit:
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

average_timelimit = t11.groupby('User', as_index=False)['Timelimit'].mean()
average_timelimit.columns = ['User','aTimelimit']
t12 = pd.merge(t11, average_timelimit, on=['User'])
t13 = t12.drop(['Account'], axis=1)

# here we are going to write the data out to a csv file
t13.to_csv('clean_slurm.csv') #so we can continue to clean it in a different file?
