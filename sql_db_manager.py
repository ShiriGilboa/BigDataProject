import os
import sqlite3 as sql

DB_FILE_NAME = "NBA_DB_2.db"


def get_sql_con(db_from_scratch=False, db_name=DB_FILE_NAME):
    if db_from_scratch or not os.path.exists(db_name):
        if os.path.exists(db_name):
            print("DB with path {} already exists, Going to remove it first".format(db_name))
            os.remove(db_name)
        db = sql.connect(db_name)
        print("New DB with name {} Created!".format(db_name))
        return db
    else:
        return sql.connect(db_name)
