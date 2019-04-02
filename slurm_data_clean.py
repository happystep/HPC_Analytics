import pandas as pd

# location of data that is | delimited
# /Volumes/Data

data = pd.read_csv("./slurmData.txt", sep='|', encoding='ISO-8859-1')

print(data)

col = data.columns.tolist() # list of the strings for all of the columns in this data

print(col)
