# =================start classification=================
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
from sklearn.metrics import f1_score
from sklearn.metrics import roc_curve
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_auc_score

from sklearn import preprocessing
from sklearn import utils

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

x_train, x_validation, y_train, y_validation = model_selection.train_test_split(x, y, test_size=0.2, random_state=7)

lab_enc = preprocessing.LabelEncoder()
training_scores_encoded = lab_enc.fit_transform(y_train)

# # prepare configuration for cross validation test harness
seed = 7
# # prepare models
models = []
#models.append(('LR', LogisticRegression()))
#models.append(('CART', DecisionTreeClassifier()))
models.append(('NB', GaussianNB()))
#models.append(('RF', RandomForestClassifier()))

# evaluate each model in turn
results = []
names = []
times = []
scoring = 'f1'
for name, model in models:
    s = time.time()
    kfold = model_selection.KFold(n_splits=10, random_state=seed)
    cv_results = model_selection.cross_val_score(model, x, y, cv=kfold, scoring=scoring)
    results.append(cv_results)
    names.append(name)
    print(cv_results)
    msg = "%s: %f (%f) " % (name, cv_results.mean(), cv_results.std())
    model.fit(x_train, training_scores_encoded)
    predictions = model.predict(x_validation)
    print('precision score: ', precision_score(y_validation, predictions))
    print('recall score: ', recall_score(y_validation, predictions))
    print('accuracy score: ', accuracy_score(y_validation, predictions))
    print('f1 score: ', f1_score(y_validation, predictions))
    # boxplot algorithm comparison

   #  fig = plt.figure(figsize=(15, 10))
   # # fig.suptitle('Algorithm Comparison Baseline')
   #  ax = fig.add_subplot(111)
   #  plt.xlabel('Algorithm')
   #  plt.ylabel('F1 Score')
   #  plt.boxplot(results)
   #  ax.set_xticklabels(names)
   #  # plt.savefig('classification1m.jpg')
   #  plt.show()

    fpr_rf, tpr_rf, _ = roc_curve(y_validation, predictions)
    plt.figure(1)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.plot(fpr_rf, tpr_rf, label=name)
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.title('ROC curve Baseline v Roles')
    plt.legend(loc='best')
    plt.show()
    print(roc_auc_score(y_validation, predictions))
    print(confusion_matrix(y_validation, predictions))
    print(classification_report(y_validation, predictions))
    e = time.time()
    print(msg)
    times.append(e - s)
    print('time: ', e - s)
    s = 0
    e = 0

# =================end classification=================