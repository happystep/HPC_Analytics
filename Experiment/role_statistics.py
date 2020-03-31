import pandas as pd

node_role_assignment_url = 'http://people.cs.ksu.edu/~happystep/HPC/transposed_roles.csv'
node_role_membership_url = 'http://people.cs.ksu.edu/~happystep/HPC/node_role_membership_by_percentage.csv'

df_role = pd.read_csv(node_role_assignment_url)
df_membership = pd.read_csv(node_role_membership_url)

print(df_role.columns.values)
print(df_membership.columns.values)

print(df_role.shape)


total_count = 0
role_0 = 0
role_1 = 0
role_2 = 0
role_3 = 0
undefined = 0

for i in df_role.itertuples():

    if i[2] == 'role_0':
        role_0 += 1
        total_count += 1
    if i[2] == 'role_1':
        role_1 += 1
        total_count += 1
    if i[2] == 'role_2':
        role_2 += 1
        total_count += 1
    if i[2] == 'role_3':
        role_3 += 1
        total_count += 1
    if i[2] == 'Unnamed: 0':
        undefined += 1


print(total_count)
print(role_0)
print(role_1)
print(role_2)
print(role_3)
print(undefined)


### work with membership starts here

print(df_membership.role_3.unique())