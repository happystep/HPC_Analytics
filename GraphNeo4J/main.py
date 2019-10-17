from GraphNeo4J import testneo4j as ts
import pandas as pd

uri = "bolt://localhost:7687"

user = "neo4j"
password = "12345"

session = ts.HPCUserDatabase(uri, user, password)

# session.print_greeting("hi this is testing the connection to the database")
# session.print_user("happystep")

# session.load_slurm_data()
# session.load_slurm_sample_data()
session.users_create_relationships()

#session.create_slurm_index()
session.close()  #

# headers = pd.read_csv('../humanHead.csv')
#
# list_of_head = list(headers) #don't over think... python is simple
# print(list_of_head)
#
# list_of_queries = []
# query = ''
#
# for head in headers:
#     query = ":User(" + str(head) + ")"
#     list_of_queries.append(query)
#
# for i in list_of_queries:
#     print(i)

# query = ''

# for head in headers:
#     query = query + head + ": csvLine." + head + ","
#
# print(query)

# session.print_user("Luis")

# filename = '../humanDate+survey.csv'

# session.delete_database()
# session.load_data()
#
# session.load_test_data() # this will load test_csv that is located on CSLINUX
# print("loading data complete\n")


### LOADING OF INDEXES

# session.create_index()
# print("loading indexes completed\n")


# for test
# df = pd.read_csv('../test_hpc.csv')
# print(df['owner'])
# print(df['job_number'])

# for real
# df = pd.read_csv('../humanDate+survey.csv')
# for column in df:
#     print(column + str(type(df[column][0])))
#
