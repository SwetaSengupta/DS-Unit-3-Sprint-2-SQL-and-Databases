import os
import sqlite3

# construct a path to wherever your database exists
#DB_FILEPATH = "chinook.db"
DB_FILEPATH = os.path.join(os.path.dirname(__file__), ".", "rpg_db.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

query = "SELECT  count(distinct character_id) as character_count FROM charactercreator_character"

#result = cursor.execute(query)
#print("RESULT", result) #> returns cursor object w/o results (need to fetch the results)

result = cursor.execute(query).fetchall()
print("Total Characters:  ", result) #How many total Characters are there?


#How many of each specific subclass?

querya = "SELECT count(distinct character_ptr_id) as cleric_count FROM charactercreator_cleric"
resulta = cursor.execute(querya).fetchall()
print("Number of clerics: ", resulta)

queryb = "SELECT count(distinct character_ptr_id) as fighter_count FROM charactercreator_fighter"
resultb = cursor.execute(queryb).fetchall()
print("Number of fighters: ", resultb)

queryc = "SELECT count(distinct character_ptr_id) as mage_count FROM charactercreator_mage"
resultc = cursor.execute(queryc).fetchall()
print("Number of mage: ", resultc)

queryd = "SELECT count(distinct mage_ptr_id) as necroma_count FROM charactercreator_necromancer"
resultd = cursor.execute(queryd).fetchall()
print("Number of necromancer: ", resultd)

querye = "SELECT count(distinct character_ptr_id) as thief_count FROM charactercreator_thief"
resulte = cursor.execute(querye).fetchall()
print("Number of thief: ", resulte)


# How many total Items?

query2 = "SELECT count(*) FROM armory_item"
result2 = cursor.execute(query2).fetchall()
print("Total items: ", result2) 

#How many of the Items are weapons? How many are not?---this is not working
#query_3 = "SELECT count(*) as total_count FROM armory_weapon"
#result_3 = cursor.execute(query_3).fetchall()
#result3 = result2[0] - result_3[0]
#print("Number of items which are weapons: ", result_3)
#print("Number of items which are not weapon: ", result3)


#How many Items does each character have? (Return first 20 rows)
query4 = "SELECT character_id, count(item_id) as item_count FROM charactercreator_character_inventory GROUP BY character_id LIMIT 20"
result4 = cursor.execute(query4).fetchall()
print("Total items each character have(only first 20 rows): ", result4)

#How many Weapons does each character have? (Return first 20 rows)



#On average, how many Items does each Character have?

query5 = "SELECT character_id, item_id, AVG(item_id) as item1 FROM charactercreator_character_inventory GROUP BY character_id "
result5 = cursor.execute(query5).fetchall()
print("Average number of items each character have: ", result5)

