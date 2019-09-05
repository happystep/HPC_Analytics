import pandas as pd
import time
df = pd.read_csv("./slurmData.txt", sep='|', encoding='ISO-8859-1')

print(df.shape)

### FIELDS WE WANT TO MAP TO

 #SGE TO SLURM

 # ['owner', 'failed', 'project', 'cpu', 'maxvmem', 'reqTime', 'reqMem', 'people']


 # owner = User
 # failed = State
 # project = Account
 # cpu = TotalCPU                # The cpu time usage in seconds.
 # slurm has two different calculations, not sure which one to use
 # TotalCPU = The sum of the SystemCPU and UserCPU time used by the job or job step.
 # SystemCPU = The amount of system CPU time used by the job or job step. The format of the output is identical to that of the Elapsed field.
 # NOTE: SystemCPU provides a measure of the task's parent process and does not include CPU time of child processes.
 # The amount of user CPU time used by the job or job step. The format of the output is identical to that of the Elapsed field.
 # NOTE: UserCPU provides a measure of the task's parent process and does not include CPU time of child processes.
 # I chose totalCPU because it seems to be a sum of all time, which seems closer to the description from sge.
 # maxvmem = MaxVMSize -- I will worry about these later...

# the following code is originally from Huichen, I have modified it to fit new data.
gd = df.groupby('User')
t1 = gd.filter(lambda x: len(x) > 200)
t1.shape

#drop NAN values
t2 = t1.dropna()
t2.shape

#reduce columns
temp = t2[['User', 'State', 'Account', 'TotalCPU', 'MaxVMSize']] #need to add a few more

# %%time = LETS CALCULATE HOW LONG THIS TAKES
#average usage of cpu , maxvmem,
start = time.time()

average = temp.groupby('User', as_index=False)['TotalCPU','MaxVMSize'].mean()
end = time.time()
diff = end-start
print("average CPU,MaxVmem built in Mean- Wall time:" + str(diff))

#rename column name
average.columns = ['User','aCPU','aMaxvmem']

t3 = pd.merge(temp, average, on=['User'])
t3.columns

newdf = t3.sample(n=1000001)
newdf.shape

#change value of attribute failed to binary
newdf['State'] = (newdf['State'] > 0).astype(int)

#add unique columns as features, id by owner, project_id by project(similar with department)
newdf['id'] = pd.factorize(newdf.User)[0]
newdf['project_id'] = pd.factorize(newdf.Account)[0]

newdf.columns

fdf = newdf.drop(['User','Account'], axis=1)
fdf.columns

fdf.head(5)


#===============start regression===============

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

# we are going to create a list of models that we will add all of the different SKlearn models we will run so we can
# easily loop and run all the different tests.

models = []
models.append(('LR', LinearRegression()))

#models.append(('LassoCV', LassoCV()))
#models.append(('LassoLarsCV', LassoLarsCV()))
models.append(('LassoLarsIC', LassoLarsIC()))

models.append(('ElasticNetCV', ElasticNetCV()))

models.append(('Ridge', Ridge()))
#models.append(('RidgeCV', RidgeCV()))

#models.append(('OrthogonalMatchingPursuitCV', OrthogonalMatchingPursuitCV()))

#models.append(('KNN', KNeighborsRegressor()))
models.append(('CART', DecisionTreeRegressor()))
##models.append(('SVR', SVR()))


# this is going to be our base 'x' data set
xt = fdf[['State', 'TotalCPU', 'MaxVMSize', 'id', 'project_id', \
          'aCPU', 'aMaxvmem', 'aReqtime', 'aReqmem']]
xt.shape

a = xt.sample(frac=0.1) # only added this line because for some reason it was changing XT to a ndarray which doesn't have the
# attribute sample which was crashing the code.
xt = preprocessing.StandardScaler().fit_transform(a)

# we are going to use python list splicing to create data sets for x and y.

#array = xt.values
x = xt[:,1:16]
y = xt[:, 0]

# we are going to run a built in model selection to create x's traning and validation set as well as y's.
x_train, x_validation, y_train, y_validation = model_selection.train_test_split(x, y, test_size=0.2, random_state=7)

# we will run Decision tree regressor
# the information about this function and what it does can be found on
# http://scikit-learn.org/stable/modules/generated/sklearn.tree.DecisionTreeRegressor.html

#m = Lasso(alpha=0.15, fit_intercept=False, tol=0.00000000000001, max_iter=1000000, positive=True)
#m = DecisionTreeRegressor()
m = DecisionTreeRegressor()
m.fit(x_train, y_train)
print(r2_score(y_validation, m.predict(x_validation)))

# here comes the loop that will run all of the models that we had added to the model. We will print out the reults of the time
# the r2 and algorithm scores.

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

### code stolen from Brandon's CIS 890 code for pasrsing requested memory
def mem_parse(mem):
    if(mem[-1] == 'G' or mem[-1] == 'g'):
        return str(int(float(mem[:-1]) * 1024))
    elif(mem[-1] == 'M' or mem[-1] == 'm'):
        return str(int(float(mem[:-1])))
    else:
        return "1024"

# we actually need to loop thru the rows in the dataframe and select ReqMem
for index, row in df.iterrows():
    if row['ReqMem'][-1] == 'c':
        req_mem_per_cpu = mem_parse(row['ReqMem'][:-1])
    else:
        req_mem = mem_parse(row['ReqMem'][:-1])
