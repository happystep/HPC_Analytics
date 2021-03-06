from GraphNeo4J import neo4j as ts
import pandas as pd

uri = "bolt://localhost:7687"

user = "neo4j"
password = "12345"

session = ts.HPCJobDatabase(uri, user, password)

# session.print_greeting("hi this is testing the connection to the database")
# session.print_user("happystep")
count = 0
# session.load_slurm_data()
#session.load_slurm_sample_data()
#session.users_create_relationships()
records = session.user_count()  # currently returns list of dictionary's of record retrieved from the BoltStatementReturn
print("record data")
record_count = len(records)
print(len(records))
print(records)

record_rows = 0
for e in records:
    record_rows += e['freq']

mean = record_rows/record_count
print("Mean of frequency is " + str(mean))

print(record_rows)
# resultsingleton = []
#
#
#
accepted = []
# can we limit to the names of users that 10gb of ram can handle? or should I use as much ram as I want?, lets test Jwryan on KAOS4
for i in records:  # iterating thru the dictionaries in the list of records
    if i['freq'] > 2047 or i['freq'] == 1:  # if its higher than the amount allowed by 10gb java heap size, ignore,
        continue
    # if i['n.User'] == 'happystep' or i['n.User'] == 'nyine' or i['n.User'] == 'antariksh':
    #     count+=1
    #     continue
    else:
        accepted.append(i)

print("accepted data")
accepted_rows = 0
print(len(accepted))
for i in accepted:
    accepted_rows += i['freq']

print(accepted_rows)

# for j in accepted:
#     name = j['n.User']
#     result = session.users_create_relationships(name)
#     print(str(name) + " relationships added")
#     print(result)
# # number of users
# print(accepted)
# print(len(accepted))
# print(count)
#  ^^^ code is sloppy? can I use a filter function on a dictionary?
#
# for j in accepted:
#     name = j['n.User']
#     result = session.users_create_relationships(name)
#     print(str(name) + " relationships added")
#     print(result)
#
# singleton = []

# for e in records:
#     if e['freq'] == 1:
#         singleton.append(e)
# print(len(singleton))
#
# # should I consider deleting the users that have too many jobs for 10gb or ram? or should I really be doing this on KAOS
large = []
print()
# print("Users with larger large frequencies")
# for d in records:
#     if d['freq'] > 2047:
#         large.append(d)
#
# print(len(large))
# for i in large:
#     print(i)
#
# print()
# print("lets delete large")
# for j in large:
#     name = j['n.User']
#     session.delete_user(name)

# singleton = []
# print("Users with small frequencies")
# for d in records:
#     if d['freq'] < 4:
#         large.append(d)
#
# print(len(singleton))
# for i in singleton:
#     print(i)
#
# print()
# print("lets delete large")
# for j in large:
#     name = j['n.User']
#     session.delete_user(name)



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
