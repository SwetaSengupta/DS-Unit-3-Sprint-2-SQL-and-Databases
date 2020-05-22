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

result = c.execute(query).fetchall()
print('Total number of rows: ',result)


query2 = '''
SELECT COUNT(*) FROM review
 WHERE Nature >= 100
   AND Shopping >= 100
'''

result2 = c.execute(query2).fetchall()
print('No of users with atleast 100 shopping and nature reviews: ',result2)