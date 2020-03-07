from GraphNeo4J import testneo4j as ts
import pandas as pd

uri = "bolt://localhost:7687"

user = "neo4j"
password = "12345"

session = ts.HPCJobDatabase(uri, user, password)

# session.print_greeting("hi this is testing the connection to the database")
# session.print_user("happystep")

# session.load_slurm_data()
#session.load_slurm_sample_data()
#session.users_create_relationships()
records = session.user_count()  # currently returns list of dictionary's of record retrieved from the BoltStatementReturn

accepted = []
# can we limit to the names of users that 10gb of ram can handle? or should I use as much ram as I want?, lets test Jwryan on KAOS4
for i in records:  # iterating thru the dictionaries in the list of records
    if i['freq'] > 2047 or i['freq'] == 1:  # if its higher than the amount allowed by 10gb java heap size, ignore,
        continue
    if i['n.User'] == 'happystep' or i['n.User'] == 'nyine' or i['n.User'] == 'antariksh':
        continue
    else:
        accepted.append(i)

# number of users
print(accepted)
print(len(accepted))

#  ^^^ code is sloppy? can I use a filter function on a dictionary?

for j in accepted:
    name = j['n.User']
    result = session.users_create_relationships(name)
    print(str(name) + " relationships added")
    print(result)


# should I consider deleting the users that have too many jobs for 10gb or ram? or should I really be doing this on KAOSfff

#session.create_slurm_index()   # creates indexes using native b-trees for the different features
session.close()

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
