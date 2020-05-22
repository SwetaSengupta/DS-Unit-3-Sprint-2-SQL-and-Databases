import os
import pandas as pd
import sqlite3

conn = sqlite3.connect("buddymove_holidayiq.sqlite3")
df = pd.read_csv("./buddymove_holidayiq.csv")
#df.to_sql("review", conn)
c = conn.cursor()

query = '''
    SELECT count(*) FROM review
'''

results = c.execute(query).fetchall()
for row in results:
    print('Total number of rows: ',row[0])


query2 = '''
SELECT COUNT(*) FROM review
 WHERE Nature >= 100
   AND Shopping >= 100
'''

results2 = c.execute(query2).fetchall()
for row in results2:
    print('No of users with atleast 100 shopping and nature reviews: ',row[0])