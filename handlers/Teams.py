import sqlite3

from handlers.BaseHandler import BaseHandler


class TeamsHandler(BaseHandler):
    def __init__(self, name, db, data):
        super().__init__(name, db, data)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                        ID BingInt PRIMARY KEY,
                        TeamName VARCHAR(100) NOT NULL,
                        TeamCode VARCHAR(15) NOT NULL,
                        City VARCHAR(100) NOT NULL
                    ); '''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements(self.data)