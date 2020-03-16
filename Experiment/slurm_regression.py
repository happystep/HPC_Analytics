# Code Modified originally written by Huichen 

#===============start regression===============
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import time


from sklearn import model_selection
from sklearn.metrics import r2_score
from sklearn import linear_model
from sklearn import preprocessing

from sklearn.linear_model import LinearRegression

from sklearn.linear_model import Lasso
from sklearn.linear_model import LassoCV
from sklearn.linear_model import LassoLars
from sklearn.linear_model import LassoLarsCV
from sklearn.linear_model import LassoLarsIC

from sklearn.linear_model import ElasticNet
from sklearn.linear_model import ElasticNetCV

from sklearn.linear_model import Ridge
from sklearn.linear_model import RidgeCV

from sklearn.linear_model import OrthogonalMatchingPursuit
from sklearn.linear_model import OrthogonalMatchingPursuitCV

from sklearn.linear_model import MultiTaskLasso
from sklearn.linear_model import MultiTaskLassoCV
from sklearn.linear_model import MultiTaskElasticNet
from sklearn.linear_model import MultiTaskElasticNetCV
from sklearn.linear_model import ARDRegression

from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR


df = pd.read_csv('http://people.cs.ksu.edu/~happystep/HPC/slurm_role_cleaned.csv')
t1 = df.dropna() # this SHOULD ensure that we have no null values

temp = t1[['State', 'ReqMem', 'Timelimit','role']]

newdf = temp

fdf = newdf

models = []
models.append(('LR', LinearRegression()))
models.append(('LassoLarsIC', LassoLarsIC()))
models.append(('ElasticNetCV', ElasticNetCV()))
models.append(('Ridge', Ridge()))
models.append(('CART', DecisionTreeRegressor()))

# memory 
xt = fdf[['ReqMem', 'Timelimit','role']]

xt = preprocessing.StandardScaler().fit_transform(xt.sample(frac=0.1))
x = xt[:,1:2]
y = xt[:,0]

x_train, x_validation, y_train, y_validation = model_selection.train_test_split(x, y, test_size=0.2, random_state=7)

#m = Lasso(alpha=0.15, fit_intercept=False, tol=0.00000000000001, max_iter=1000000, positive=True)
#m = DecisionTreeRegressor()
m = DecisionTreeRegressor()
m.fit(x_train, y_train)
print(r2_score(y_validation, m.predict(x_validation)))

results = []
names = []
r2 = []
times = []
for name, model in models:
 
  kfold = model_selection.KFold(n_splits=10, random_state=7)
  cv_results = model_selection.cross_val_score(model, x_train, y_train, cv=kfold, scoring='neg_mean_squared_error')
   
  results.append(cv_results)
  names.append(name)
  msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
  print(msg)
  s = time.time()    
  model.fit(x_train, y_train)
  y_pred_test = model.predict(x_validation)
  r2s = r2_score(y_validation, y_pred_test)
  e = time.time()
  t = e - s
  times.append(t)
  print('time: ', t)
  r2.append(r2s)
  print('r2: ', r2s)

  #===============end regression===============