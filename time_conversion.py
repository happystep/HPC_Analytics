import pandas as pd
from datetime import datetime

def timechange(a):
    datetime.fromtimestamp(a).strftime('%Y-%m-%d %H:%M:%S')


df = pd.read_csv('16mInfo+reqTMcumamaxminstdcnt.csv')

df.shape
print(df.shape)

temp = df[['owner','submission_time','start_time','end_time']]
sub_time = temp['submission_time'].apply(timechange, axis=1)