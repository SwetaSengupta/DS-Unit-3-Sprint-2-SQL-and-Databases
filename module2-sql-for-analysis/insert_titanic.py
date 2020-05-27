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



"""
for future reference
import os
import pandas


CSV_FILEFATH = os.path.join(os.path.dirname(__file__), ".", "data","titanic.csv")
df = pd.read_csv(CSV_FILEPATH)

cloud.cursor().execute("""
#CREATE TABLE Titanic (
    #Survived            INT,
    #Pclass              INT,
    #Name                varchar(120),
    #Sex                 varchar(10),
    #Age                 INT,
    #SiblingsSpouses     INT,
    #ParentsChildren     INT,
    #Fare                INT);
""")
for row in titanic.values:
    cloud.cursor().execute("""
    #INSERT INTO Titanic
    #(Survived, Pclass, Name, Sex, Age, SiblingsSpouses, ParentsChildren, Fare)
    #VALUES %s;
    """, tuple(row)))
cloud.commit()






"""
