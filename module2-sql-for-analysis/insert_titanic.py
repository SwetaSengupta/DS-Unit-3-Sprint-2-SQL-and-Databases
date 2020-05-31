# link used: https://stackoverflow.com/questions/30050097/copy-data-from-csv-to-postgresql-using-python
# f = open('C:/tmp/titanic.csv', 'r')
# cur.copy_from(f, 'titanic', sep=',')
# f.close()
# conn.commit()
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()  # reads the contents from .env file

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

### Connect to ElephantSQL-hosted PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
)

print("CONNECTION", type(conn))
### A "cursor", a structure to iterate over db records to perform queries
cur = conn.cursor()
sql = """
DROP TABLE IF EXISTS passengers;
CREATE TABLE IF NOT EXISTS passengers (
    id SERIAL PRIMARY KEY,
    survived boolean,
    pclass int4,
    full_name text,
    gender text,
    age int4,
    sib_spouse_count int4,
    parent_child_count int4,
    fare float8
);
"""
cur.execute(sql)

# READ PASSENGER DATA FROM THE CSV FILE

CSV_FILEFATH = os.path.join(os.path.dirname(__file__), ".", "titanic.csv")
df = pd.read_csv(CSV_FILEFATH)
print(df.columns.tolist())
# print(df.dtypes)
# print(df.head())

# to avoid PG insertion errors related to datatype mismatches (psycopg2.ProgrammingError: can't adapt type 'numpy.int64'),
# ... we need to convert np.int64 columns to normal integers
# ... https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
# ... https://stackoverflow.com/questions/34838378/dataframe-values-tolist-datatype
# ... https://stackoverflow.com/questions/47423930/how-to-convert-pandas-dataframe-columns-to-native-python-data-types

df["Survived"] = df["Survived"].values.astype(
    bool
)  # do this before converting to native types, because this actually converts to np.bool
df = df.astype(
    "object"
)  # converts numpy dtypes to native python dtypes (avoids psycopg2.ProgrammingError: can't adapt type 'numpy.int64')

# INSERT DATA INTO THE PASSENGERS TABLE
# how to convert dataframe to a list of tuples?
list_of_tuples = list(df.to_records(index=False))
insertion_query = f"INSERT INTO passengers (survived, pclass, full_name, gender, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
execute_values(
    cur, insertion_query, list_of_tuples
)  # third param: data as a list of tuples!
# CLEAN UP

conn.commit()  # actually save the records / run the transaction to insert rows
# cur.close()
# conn.close()


# - How many passengers survived, and how many died?
# query1 = "SELECT count(Survived) FROM titanic WHERE Survived = 1;"
# result1 = cur.execute(query1).fetchall()
# for row in result1:
# print(row)

# - How many passengers were in each class?
# query2 = "SELECT count(pclass) FROM titanic GROUP BY pclass"


query2 = "SELECT count(pclass) FROM titanic GROUP BY pclass"
cur.execute(query2)
result2 = cur.fetchall()
print("Number of passengers in each class: ", result2)

# - How many passengers survived/died within each class?
query3 = "SELECT count(survived) FROM titanic  where survived = 1 GROUP BY pclass"
cur.execute(query3)
result3 = cur.fetchall()
print("Number of passengers  survived in each class: ", result2)

# - What was the average age of survivors vs nonsurvivors?
querya = "SELECT AVG(age) FROM titanic WHERE survived = 1"
cur.execute(querya)
resulta = cur.fetchall()
for row1 in resulta:
    print(row1[0])
queryb = "SELECT AVG(age) FROM titanic WHERE survived = 0"
cur.execute(queryb)
resultb = cur.fetchall()
for row2 in resulta:
    print(row2[0])
print("the average age of survivors vs nonsurvivors: ", row1[0] / row2[0])

# - What was the average age of each passenger class?
query4 = "SELECT AVG(age) FROM titanic GROUP BY pclass"
cur.execute(query4)
result4 = cur.fetchall()
print("Avg age of passengers  of each class: ", result4)

# - What was the average fare by passenger class? By survival?
query5 = "SELECT AVG(fare) FROM titanic GROUP BY pclass"
cur.execute(query5)
result5 = cur.fetchall()
print("Avg fare of passengers  of each class: ", result5)

# - How many siblings/spouses aboard on average, by passenger class? By survival?

query6 = "SELECT AVG(siblings_spouses_aboard) FROM titanic where survived = 1 GROUP BY pclass"
cur.execute(query6)
result6 = cur.fetchall()
print("Avg fare of passengers  of each class: ", result6)

# - How many parents/children aboard on average, by passenger class? By survival?
query7 = "SELECT AVG(parents_children_aboard) FROM titanic where survived = 1 GROUP BY pclass"
cur.execute(query7)
result7 = cur.fetchall()
print("Avg fare of passengers  of each class: ", result7)

# - Do any passengers have the same name?
