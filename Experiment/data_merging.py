# this file will merge node assignments with data from the nodes and decrease the feature size

import pandas as pd

# URL Links to be kept when code published
full_set_url = "http://people.cs.ksu.edu/~happystep/HPC/full_set.csv"  #not used
reduced_set_url = "http://people.cs.ksu.edu/~happystep/HPC/reduced_set.csv"
node_role_assignment_url = "http://people.cs.ksu.edu/~happystep/HPC/node_role_assignment.csv"
features_extracted_url = "http://people.cs.ksu.edu/~happystep/HPC/features_extracted.csv"
node_role_membership_by_percentage_url = "http://people.cs.ksu.edu/~happystep/HPC/node_role_membership_by_percentage.csv"
transposed_roles_url = "http://people.cs.ksu.edu/~happystep/HPC/transposed_roles.csv"


# LOCAL FILES TO BE DELETED, not in repo
node_role_assignment_local = "../Data/node_role_assignment.csv"
reduced_set_local = "../Data/reduced_set.csv"
transposed_roles_local = "../Data/transposed_roles.csv"

# transposed_roles = pd.read_csv(transposed_roles_url)
transposed_roles = pd.read_csv(transposed_roles_local)  # change to URL above
print(transposed_roles)

# reduced_set = pd.read_csv(reduced_set_url)
reduced_set = pd.read_csv(reduced_set_local)  # CHANGE TO URL above
print(reduced_set)


merged_set = reduced_set.set_index('ID(n)').join(transposed_roles.set_index('node'))
print(merged_set)

merged_set.to_csv("../Data/merged_set.csv")

