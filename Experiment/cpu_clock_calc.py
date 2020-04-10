import pandas as pd

total_url = "http://people.cs.ksu.edu/~happystep/HPC/totalset_cpu.csv"
experiment_url = "http://people.cs.ksu.edu/~happystep/HPC/experiment_cpu.csv"

data_tf = pd.read_csv(total_url)
print(data_tf.columns.values)

## change timelimt to total cpu

t9 = data_tf

# format of TimeLimit  [days-]hours:minutes:seconds[.microseconds]
new_frame_timelimit = {} # empty dictionary
count = 0
for x in t9.TotalCPU:
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

t10 = t9.drop(['TotalCPU'], axis=1)

data_timelimit = pd.Series(new_frame_timelimit).to_frame('TotalCPU')
newdf_timelimit = pd.DataFrame(data_timelimit)

t11 = pd.concat([t10, newdf_timelimit], axis=1)

print(t11)
print(t11.TotalCPU.unique())


