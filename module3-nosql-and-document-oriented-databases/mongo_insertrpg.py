#link used: https://stackoverflow.com/questions/49221550/pandas-insert-a-dataframe-to-mongodb

# I like sqlite best. I like the structure of it. I think and understand quicker when I can see rows and columns.

import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv
import pymongo


# Create your connection.
DB_FILEPATH = os.path.join(os.path.dirname(__file__),"..","module1-introduction-to-sql", "rpg_db.sqlite3")
con = sqlite3.connect(DB_FILEPATH)

data = pd.read_sql_query("SELECT * FROM charactercreator_character;", con) #converting into a dataframe
    
load_dotenv() #loading info from env file

DB_USER = os.getenv("MONGO_USER")
DB_PASSWORD = os.getenv("MONGO_PASSWORD")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_uri)

db = client.rpg_database # "test_database" or whatever you want to call it
collection = db.rpg_test # "pokemon_test" or whatever you want to call it


df_dict = data.to_dict("records") #covert to dictionary
collection.insert_many(df_dict)

print("DOCS:", collection.count_documents({})) #SELECT count(__) as row_count FROM my_table
print(collection.count_documents({"name": "Aliquid iste optio reiciendi"}))#this also has the where condition