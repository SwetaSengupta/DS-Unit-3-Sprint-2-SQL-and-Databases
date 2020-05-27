#client = pymongo.MongoClient("mongodb+srv://swetasengupta:<password>@cluster0-fk9rp.mongodb.net/test?retryWrites=true&w=majority")
#db = client.test

import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("MONGO_USER")
DB_PASSWORD = os.getenv("MONGO_PASSWORD")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)

db = client.test_database # "test_database" or whatever you want to call it
print("----------------")
print("DB:", type(db), db)

collection = db.pokemon_test # "pokemon_test" or whatever you want to call it
print("----------------")
print("COLLECTION:", type(collection), collection)

print("----------------")
print("COLLECTIONS:")
print(db.list_collection_names())

collection.insert_one({
    "name": "Pikachu",
    "level": 30,
    "exp": 76000000000,
    "hp": 400,
})
print("DOCS:", collection.count_documents({})) #SELECT count(__) as row_count FROM my_table
print(collection.count_documents({"name": "Pikachu"}))#this also has the where condition




#query the collection, get results, then loop throughthe results
pikas = list(collection.find({"name": "Pikachu"}))
for pika in pikas:
    print(pika["name"])


    #insert multiple documents
pelipper = {
        "name": "Pelipper",
        "level": 100,
        "exp": 100000,
        "hp":323
}

tyraniter = {
        "name": "Tyraniter",
        "level": 77,
        "exp" :48148,
        "hp" :264

}

psyduck ={
      "name" :"Psyduck",
      "level" : 20,
      "exp" :23000,
      "hp" : 100


}
team = [pelipper,tyraniter, psyduck]
collection.insert_many(team)
print("DOCS:", collection.count_documents({})) #select count(__) as row count from my_table

high_levels = list(collection.find({"level":{"$gte":70}}))
for doc in high_levels:
    print(doc["name"])