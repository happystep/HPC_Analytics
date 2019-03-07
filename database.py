from pymongo import MongoClient
client = MongoClient()

client = MongoClient('localhost', 27017)

db = client.pymongo_test


# we ended up not creating a database... just using the CSV Files instead.. we might use a DB later.