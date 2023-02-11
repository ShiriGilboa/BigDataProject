import sqlite3

from pyspark.sql import SparkSession

# conn = sqlite3.connect('example.db')
# cursor = conn.cursor()
#
# # Create the table if it doesn't exist
# cursor.execute('''
#     CREATE TABLE IF NOT EXISTS items (
#         id INTEGER PRIMARY KEY,
#         name TEXT,
#         value INTEGER
#     )
# ''')
#
# # Define the item you want to insert/update
# item = (2, 'item_1', 5)
#
# # Use an UPSERT statement to insert or update the item
# cursor.execute('''
#     INSERT INTO items (id, name, value)
#     VALUES (?,?,?)
#     ON CONFLICT(id)
#     DO UPDATE SET value=excluded.value
# ''', item)
#
# item = (2, 'item_4', 7)
#
# cursor.execute('''
#     INSERT INTO items (id, name, value)
#     VALUES (?,?,?)
#     ON CONFLICT(id)
#     DO UPDATE SET value=excluded.value
# ''', item)
#
# res = cursor.execute("SELECT name, value FROM items WHERE id=2")
# print(res.fetchone())
#
# conn.commit()
# conn.close()
#

from pyspark.conf import SparkConf
import pyspark.pandas as ps

conf = SparkConf()  # create the configuration
conf.set("spark.jars", "sqlite-jdbc-3.40.1.0.jar")  # set the spark.jars

...
spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local") \
        .appName("SQLite JDBC")\
        .config("spark.driver.extraClassPath","sqlite-jdbc-3.40.1.0.jar")\
        .config("spark.executor.extraClassPath","sqlite-jdbc-3.40.1.0.jar")\
        .getOrCreate()
#
df = ps.read_sql("Games", con="jdbc:sqlite:NBA_DATA_BIG_DATA_PROJECT.db")
print("Hi")