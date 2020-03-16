# this file will merge node assignments with data from the nodes and decrease the feature size

import pandas as pd

# URL Links to be kept when code published
full_set_url = "http://people.cs.ksu.edu/~happystep/HPC/full_set.csv"  #not used
reduced_set_url = "http://people.cs.ksu.edu/~happystep/HPC/reduced_set.csv"
node_role_assignment_url = "http://people.cs.ksu.edu/~happystep/HPC/node_role_assignment.csv"
features_extracted_url = "http://people.cs.ksu.edu/~happystep/HPC/features_extracted.csv"
node_role_membership_by_percentage_url = "http://people.cs.ksu.edu/~happystep/HPC/node_role_membership_by_percentage.csv"
transposed_roles_url = "http://people.cs.ksu.edu/~happystep/HPC/transposed_roles.csv"

transposed_roles = pd.read_csv(transposed_roles_url)
print(transposed_roles)

reduced_set = pd.read_csv(reduced_set_url)
print(reduced_set)


merged_set = reduced_set.set_index('ID(n)').join(transposed_roles.set_index('node'))
print(merged_set)

merged_set.to_csv("../Data/merged_set.csv")

