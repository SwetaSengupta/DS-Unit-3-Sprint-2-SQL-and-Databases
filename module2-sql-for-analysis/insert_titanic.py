#link used: https://stackoverflow.com/questions/30050097/copy-data-from-csv-to-postgresql-using-python

import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()#reads the contents from .env file


DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST") 

### Connect to ElephantSQL-hosted PostgreSQL
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASSWORD, host=DB_HOST)

print("CONNECTION", type(conn))                       
### A "cursor", a structure to iterate over db records to perform queries
cur = conn.cursor()

f = open('C:/tmp/titanic.csv', 'r')
cur.copy_from(f, 'titanic', sep=',')
f.close()
conn.commit()

cur.execute('SELECT * From titanic WHERE age < 15;')
### Note - nothing happened yet! We need to actually *fetch* from the cursor
result = cur.fetchall()
for row in result:
    print("-----------------------------------------------------------------------------")
    print(row)

