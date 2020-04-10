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

temp = t1[['State', 'ReqMem', 'Timelimit', 'User', 'role']]

newdf = temp

print(newdf.shape)

fdf = newdf
# classification for State = 1, failed

xt = fdf[['State', 'ReqMem', 'Timelimit']]
xt.fillna(0)


# THIS IS FOR BASELINE
a = xt.sample(frac=0.1)
xt = preprocessing.StandardScaler().fit_transform(a)

x = xt[:, 1:2]
y = xt[:, 0]

y = y.astype('int')

x_train, x_validation, y_train, y_validation = model_selection.train_test_split(x, y, test_size=0.2, random_state=7)

# THIS IS FOR ROLES

xt_roles = fdf[['State', 'ReqMem', 'Timelimit','role']]
xt_roles.fillna(0)

a_roles = xt_roles.sample(frac=0.1)
xt_roles = preprocessing.StandardScaler().fit_transform(a)

x_roles = xt_roles[:,1:3]
y_roles = xt_roles[:, 0]

y_roles = y_roles.astype('int')

x_train_roles, x_validation_roles, y_train_roles, y_validation_roles = model_selection.train_test_split(x_roles, y_roles, test_size=0.2, random_state=7)
########

#baseline

lab_enc = preprocessing.LabelEncoder()
training_scores_encoded = lab_enc.fit_transform(y_train)

# roles

lab_enc_roles = preprocessing.LabelEncoder()
training_scores_encoded_roles = lab_enc_roles.fit_transform(y_train_roles)






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
    # = time.time()
    models_roles = model
    kfold = model_selection.KFold(n_splits=10, random_state=seed)
    cv_results = model_selection.cross_val_score(model, x, y, cv=kfold, scoring=scoring)
    results.append(cv_results)
    names.append(name)
    #print(cv_results)
    #msg = "%s: %f (%f) " % (name, cv_results.mean(), cv_results.std())
    model.fit(x_train, training_scores_encoded)
    predictions = model.predict(x_validation)

    models_roles.fit(x_train_roles, training_scores_encoded_roles)
    predictions_roles = model.predict(x_validation_roles)
    #print('precision score: ', precision_score(y_validation, predictions))
    #print('recall score: ', recall_score(y_validation, predictions))
    #print('accuracy score: ', accuracy_score(y_validation, predictions))
    #print('f1 score: ', f1_score(y_validation, predictions))
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

    auc_score = roc_auc_score(y_validation, predictions)
    auc_score_roles = roc_auc_score(y_validation_roles, predictions_roles)
    print(auc_score)
    print(auc_score_roles)
    fpr_rf, tpr_rf, _ = roc_curve(y_validation, predictions)
    fpr_rf_roles, tpr_rf_roles, _ = roc_curve(y_validation_roles, predictions_roles)
    plt.figure(1)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.plot(fpr_rf, tpr_rf, label="ROC curve of NB Baseline (area = {})".format(np.around(auc_score, 4)))
    plt.plot([0, 1], [0, 1], 'k--')
    plt.plot(fpr_rf_roles, tpr_rf_roles, label="ROC curve of NB Roles (area = {})".format(np.around(auc_score_roles, 4)))
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.title(' Baseline v.s. Roles')
    plt.legend(loc='lower right')
    plt.show()

    #print(confusion_matrix(y_validation, predictions))
    #print(classification_report(y_validation, predictions))
    e = time.time()
    #print(msg)
    # times.append(e - s)
    # print('time: ', e - s)
    # s = 0
    # e = 0

# =================end classification=================