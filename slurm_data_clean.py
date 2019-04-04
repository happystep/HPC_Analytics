import pandas as pd

data = pd.read_csv("./slurmData.txt", sep='|', encoding='ISO-8859-1')

print(data.shape)

col = data.columns.tolist()  # list of the strings for all of the columns in this data

for col in data:
    print(col)
    print()
    print(data[col].unique())
    print()



### FIELDS WE WANT TO MAP TO

 #SGE TO SLURM

 # ['owner', 'failed', 'project', 'cpu', 'maxvmem', 'reqTime', 'reqMem', 'people']


 # owner = User
 # failed = Status
 # project = Account
 # cpu = TotalCPU                # The cpu time usage in seconds.
 # slurm has two different calculations, not sure which one to use
 # TotalCPU = The sum of the SystemCPU and UserCPU time used by the job or job step.
 # SystemCPU = The amount of system CPU time used by the job or job step. The format of the output is identical to that of the Elapsed field.
 # NOTE: SystemCPU provides a measure of the task's parent process and does not include CPU time of child processes.
 # The amount of user CPU time used by the job or job step. The format of the output is identical to that of the Elapsed field.
 # NOTE: UserCPU provides a measure of the task's parent process and does not include CPU time of child processes.
 # I chose totalCPU because it seems to be a sum of all time, which seems closer to the description from sge.
 # maxvmem = MaxVMSize
