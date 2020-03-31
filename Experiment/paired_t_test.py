#  we will compare the two NB algorithms performance based on different data sets.

from mlxtend.evaluate import paired_ttest_kfold_cv
from sklearn.naive_bayes import GaussianNB
import pandas as pd
import numpy as np
import time
from numpy import float32
import matplotlib.pyplot as plt
from sklearn import model_selection
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import roc_curve
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression

org = 'http://people.cs.ksu.edu/~happystep/HPC/slurm_role_cleaned.csv'
df = pd.read_csv(org)

print(df.shape)

fdf = df.dropna()

t1 = fdf.sample(n=60000)

count = 0
for i in t1.User.unique():
    count += 1
print("number of users")
print(count)

temp = t1[['State', 'ReqMem', 'Timelimit', 'User']]

newdf = temp

print(newdf.shape)

fdf = newdf
# classification for State = 1, failed

xt = fdf[['State', 'ReqMem', 'Timelimit']]
xt.fillna(0)

a = xt.sample(frac=0.1)
xt = preprocessing.StandardScaler().fit_transform(a)

x = xt[:, 1:3]
y = xt[:, 0]

y = y.astype('int')

### 2

df_ = pd.read_csv('http://people.cs.ksu.edu/~happystep/HPC/slurm_role_cleaned.csv')
t10= df_.dropna() # this SHOULD ensure that we have no null values

count = 0
for i in t10.User.unique():
    count+=1
print("number of users")
print(count)



temp_ = t10[['State', 'ReqMem', 'Timelimit','role']]

newdf_ = temp_

fdf_ = newdf_
# classification for State = 1, failed

xt_ = fdf_[['State', 'ReqMem', 'Timelimit','role']]
xt_.fillna(0)

a_ = xt_.sample(frac=0.1)
xt_ = preprocessing.StandardScaler().fit_transform(a_)

x_ = xt_[:,1:3]
y_ = xt_[:, 0]

y_=y_.astype('int')

clf2 = LogisticRegression()
clf1 = GaussianNB()

t, p = paired_ttest_kfold_cv(estimator1=clf1,
                              estimator2=clf2,
                              X=x, y=y,
                              random_seed=1)


print('t statistic: %.3f' % t)
print('p value: %.3f' % p)
