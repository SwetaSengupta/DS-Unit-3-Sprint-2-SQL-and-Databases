import pandas as pd
import sqlite3
import pymongo
import os
from dotenv import load_dotenv


# Create your connection.
DB_FILEPATH = os.path.join(
    os.path.dirname(__file__), "..", "module1-introduction-to-sql", "rpg_db.sqlite3"
)
con = sqlite3.connect(DB_FILEPATH)

# converting into a dataframe
data1 = pd.read_sql_query("SELECT * FROM charactercreator_character;", con)
data2 = pd.read_sql_query("SELECT * FROM charactercreator_character_inventory;", con)
data3 = pd.read_sql_query("SELECT * FROM armory_item;", con)
data4 = pd.read_sql_query("SELECT * FROM armory_weapon;", con)
data5 = pd.read_sql_query("SELECT * FROM charactercreator_cleric;", con)
data6 = pd.read_sql_query("SELECT * FROM charactercreator_fighter;", con)
data7 = pd.read_sql_query("SELECT * FROM charactercreator_mage;", con)
data8 = pd.read_sql_query("SELECT * FROM charactercreator_necromancer;", con)
data9 = pd.read_sql_query("SELECT * FROM charactercreator_thief;", con)

load_dotenv()  # loading info from env file

DB_USER = os.getenv("MONGO_USER")
DB_PASSWORD = os.getenv("MONGO_PASSWORD")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_uri)

db = client.rpg_database  # "test_database" or whatever you want to call it
col_char = db.rpg_char  # "pokemon_test" or whatever you want to call it
col_inv = db.rpg_inv
col_ar_it = db.rpg_ar_it
col_ar_w = db.rpg_ar_w
col_cl = db.rpg_cl
col_fi = db.rpg_fi
col_ma = db.rpg_ma
col_necro = db.rpg_necro
col_thief = db.rpg_thief

# covert to dictionary
df_dict1 = data1.to_dict("records")
df_dict2 = data2.to_dict("records")
df_dict3 = data3.to_dict("records")
df_dict4 = data4.to_dict("records")
df_dict5 = data5.to_dict("records")
df_dict6 = data6.to_dict("records")
df_dict7 = data7.to_dict("records")
df_dict8 = data8.to_dict("records")
df_dict9 = data9.to_dict("records")

# inserting the dictionaries
col_char.insert_many(df_dict1)
col_inv.insert_many(df_dict2)
col_ar_it.insert_many(df_dict3)
col_ar_w.insert_many(df_dict4)
col_cl.insert_many(df_dict5)
col_fi.insert_many(df_dict6)
col_ma.insert_many(df_dict7)
col_necro.insert_many(df_dict8)
col_thief.insert_many(df_dict9)

# print("DOCS:", col_char.count_documents({}))
# print(col_char.count_documents({"name": "Aliquid iste optio reiciendi"}))

# Total Characters
print("Total Characters are: ", len(db.rpg_char.distinct("character_id")))

# How many total Items?
print("Total items are: ", len(col_ar_it.distinct("item_id")))

# - How many of the Items are weapons? How many are not?
print("Total items are weapons: ", len(col_ar_w.distinct("item_ptr_id")))

print(
    "Total items not weapons: ",
    len(col_ar_it.distinct("item_id")) - len(col_ar_w.distinct("item_ptr_id")),
)

# - How many Items does each character have? (Return first 20 rows)
