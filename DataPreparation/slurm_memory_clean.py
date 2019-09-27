import pandas as pd
import numpy as np
import re

df = pd.read_csv("http://people.cs.ksu.edu/~huichen/hpc/data-set/slurmUserbased.csv")
print(df.shape)


# parsing mem from AllocTRES and ReqTRES
def getMem(col):
    mem = 0
    if type(col) is not float:
        for c in col.split(','):
            if 'mem=' in c:
                temp = c.split('mem=')[1]
                if 'M' in temp or 'm' in temp:
                    mem = np.float16(re.sub(r'M|m', '', temp)) / 1024
                elif 'T' in temp or 't' in temp:
                    mem = np.float16(re.sub(r'T|t', '', temp)) * 1024
                else:
                    mem = re.sub(r'G|g', '', temp)

    return mem


df['AllocMemTRES'] = df.AllocTRES.apply(lambda x: getMem(x))
df['ReqMemTRES'] = df.ReqTRES.apply(lambda x: getMem(x))
print(df.shape)
df.to_csv("slurmUserBasedMemory.csv")
