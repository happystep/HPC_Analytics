# Code Modified originally written by Huichen 

# general stats
# Total Passed Jobs = 32612
# Total Failed Jobs = 27740
# Percentage Passed Jobs = 54.0363202545069%
# Percentage Failed Jobs = 45.96367974549311%

#=================start classification=================
import pandas as pd
import numpy as np
import time
from numpy import float32
import matplotlib.pyplot as plt

from sklearn import model_selection
from sklearn.metrics import r2_score
from sklearn import linear_model
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier

from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score

from sklearn import preprocessing
from sklearn import utils

df = pd.read_csv('http://people.cs.ksu.edu/~happystep/HPC/slurm_role_cleaned.csv')
t1 = df.dropna() # this SHOULD ensure that we have no null values

temp = t1[['State', 'ReqMem', 'Timelimit','role']]

newdf = temp

fdf = newdf
# classification for State = 1, failed

xt = fdf[['State', 'ReqMem', 'Timelimit','role']]
xt.fillna(0)

a = xt.sample(frac=0.1)
xt = preprocessing.StandardScaler().fit_transform(a)

x = xt[:,1:3]
y = xt[:, 0]

y=y.astype('int')

x_train, x_validation, y_train, y_validation = model_selection.train_test_split(x, y, test_size=0.2, random_state=7)

lab_enc = preprocessing.LabelEncoder()
training_scores_encoded = lab_enc.fit_transform(y_train)

# # prepare configuration for cross validation test harness
seed = 7
# # prepare models
models = []
models.append(('LR', LogisticRegression()))
models.append(('CART', DecisionTreeClassifier()))
models.append(('NB', GaussianNB()))
models.append(('RF', RandomForestClassifier()))

# evaluate each model in turn
results = []
names = []
times = []
scoring = 'accuracy'
for name, model in models:
    s = time.time()
    kfold = model_selection.KFold(n_splits=10, random_state=seed)
    cv_results = model_selection.cross_val_score(model, x, y, cv=kfold, scoring=scoring)
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
    
    model.fit(x_train, training_scores_encoded)
    predictions = model.predict(x_validation)
    print('accuracy score: ', accuracy_score(y_validation, predictions))
    e = time.time()
    print(msg)
    times.append(e - s)
    print('time: ',e - s)
    s = 0
    e = 0

#=================end classification=================