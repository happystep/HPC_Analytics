# wea re going to get some basic statistics
import pandas as pd

df = pd.read_csv('./clean_slurm.csv')
 # this SHOULD ensure that we have no null values

cats = ['State', 'TotalCPU', 'MaxVMSize', 'ReqMem', 'Timelimit','aTotalCPU', 'aMaxVMSize', 'aTimelimit', 'aReqMem']

# descriptive statistics
# count, mean, std, min, 25%, 50%, 75%, max 
for i in cats:
    print(df[i].describe())

print(df.corr())

print(df.cov())

# print the number of failed jobs versus not failed, over total

failed = 0
passed = 0 
total_rows = df.shape[0]

for i in df.State:
    if i == 1:
        failed += 1
    else:
        passed += 1

passed_percentage = passed/total_rows * 100
failed_percentage = failed/total_rows * 100

print("Total Passed Jobs = " + str(passed))
print("Total Failed Jobs = " + str(failed))
print("Percentage Passed Jobs = " + str(passed_percentage) + "%")
print("Percentage Failed Jobs = " + str(failed_percentage) + "%")
