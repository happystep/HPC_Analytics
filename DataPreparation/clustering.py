# Luis Bobadilla
# HPC Analytics Project
# Clustering based on Demographics or Survey. 

# resources
# https://stackabuse.com/hierarchical-clustering-with-python-and-scikit-learn/
# https://scikit-learn.org/stable/modules/generated/sklearn.cluster.AgglomerativeClustering.html#sklearn.cluster.AgglomerativeClustering
# Huichen's code
# https://www.geeksforgeeks.org/replacing-strings-with-numbers-in-python-for-data-analysis/

import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
from sklearn.neighbors import kneighbors_graph
from sklearn.cluster import AgglomerativeClustering



df = pd.read_csv("HumanDate+survey.csv")
df.shape

# filter the users whose submission jobs are less than 200 
# this is first done by using the groupby function on the dataframe and setting that equal to another variable
# then the filter function is run.
gd = df.groupby('owner')
t1 = gd.filter(lambda x: len(x) > 200)
t1.shape

# drop NAN values, what this means is that we want to drop all NULL values which are represented as NaN in Python
t2 = t1.dropna()
t2.shape

# lets make the data frames smaller by making one with demographic information and one with survey information

demographics = t2[['dep', 'university', 'reqTime', 'reqMem', 'people']]
demographics.shape


survey = t2[['q5_experience', 'q6_proficiency', 'q7_training']]
survey.shape

temp = t2[['owner', 'failed', 'project', 'cpu', 'maxvmem','mem']]
temp.shape

# average usage of cpu , maxvmem, requested time, requested memory for each user. Basically what we are creating here is
# another dataframe with aggregations (in this case we are calculating the mean accross all rows for the given columns)
average = temp.groupby('owner', as_index=False)['cpu','maxvmem','mem'].mean()
average.columns = ['owner','aCPU','aMaxvmem', 'aMem']

sge_log = pd.merge(temp, average, on=['owner'])
sge_log.shape

sge_log_demographics = sge_log.join(demographics)
sge_log_demographics.shape

sge_log_demographics.columns

# sanity check to make sure our data looks the way we expect.
sge_log_demographics['failed'] = (sge_log_demographics['failed'] > 0).astype(int)
sge_log_demographics.head(2)

# experiment 1
# we need to assign integer valueto demopgrahic types.
# 'ComputerScience' = 0
# 'ChemicalEngineering' = 1
# 'Agronomy' = 2
# 'JamesRMacdonaldLaboratory' = 3
# 'AnatomyandPhysiology' = 4
# 'MechanicalNuclearEngineering' = 5
# 'Physics' = 6
# 'Economics' = 7
# 'PlantPathology' = 8
# 'Chemistry' = 9
# 'Biology' = 10
# 'defaultdepartment' = 11


sge_log_demographics.dep[sge_log_demographics.dep == 'ComputerScience'] = 0
sge_log_demographics.dep[sge_log_demographics.dep == 'ChemicalEngineering'] = 1
sge_log_demographics.dep[sge_log_demographics.dep == 'Agronomy'] = 2
sge_log_demographics.dep[sge_log_demographics.dep == 'JamesRMacdonaldLaboratory'] = 3
sge_log_demographics.dep[sge_log_demographics.dep == 'AnatomyandPhysiology'] = 4
sge_log_demographics.dep[sge_log_demographics.dep == 'MechanicalNuclearEngineering'] = 5
sge_log_demographics.dep[sge_log_demographics.dep == 'Physics'] = 6
sge_log_demographics.dep[sge_log_demographics.dep == 'Economics'] = 7
sge_log_demographics.dep[sge_log_demographics.dep == 'PlantPathology'] = 8
sge_log_demographics.dep[sge_log_demographics.dep == 'Chemistry'] = 9
sge_log_demographics.dep[sge_log_demographics.dep == 'Biology'] = 10
sge_log_demographics.dep[sge_log_demographics.dep == 'defaultdepartment'] = 11

# now we do the same on university field
# 'KansasStateUniersity' = 0
# 'defaultunivercity' = 1

sge_log_demographics.university[sge_log_demographics.university == 'KansasStateUniversity'] = 0
sge_log_demographics.university[sge_log_demographics.university == 'defaultunivercity'] = 1

# lastly we also need to assign numerical value to the people attribute
# 'ResearchAssociate' = 0
# 'Faculty' = 1
# 'Graduate' = 2
# 'Staff' = 3
# 'PostDoctoralResearcher' = 4
# 'Undergraduate' = 5
# 'Unknowing' = 6

sge_log_demographics.people[sge_log_demographics.people == 'ResearchAssociate'] = 0
sge_log_demographics.people[sge_log_demographics.people == 'Faculty'] = 1
sge_log_demographics.people[sge_log_demographics.people == 'Graduate'] = 2
sge_log_demographics.people[sge_log_demographics.people == 'Staff'] = 3
sge_log_demographics.people[sge_log_demographics.people == 'PostDoctoralResearcher'] = 4
sge_log_demographics.people[sge_log_demographics.people == 'Undergraduate'] = 5
sge_log_demographics.people[sge_log_demographics.people == 'Unknowing'] = 6

# Metric used to compute the linkage: Eucledian
# linkage criterion used: Ward
HAC = AgglomerativeClustering()
test = sge_log_demographics[['failed', 'aCPU', 'aMaxvmem', 'aMem', 'dep', 'university', 'people']]
small = test.dropna()
experiment1 = small.sample(n=100000) #87500 works but takes its sweet time 2999.8902530670166 (50 minutes) to be exact
attempt1 = np.array(experiment1)


start = time.time()
HAC.fit(attempt1)
print(HAC.labels_)


end = time.time()
print(end - start)
