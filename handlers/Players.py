import sqlite3

from handlers.BaseHandler import BaseHandler


class PlayersHandler(BaseHandler):
    def __init__(self, name, db, data, cache=None):
        self.key_attr = ["ID", "YearsInLeague", "Country" ]
        super().__init__(name, db, data, cache)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                        ID BingInt , 
                        FirstName VARCHAR(100) NOT NULL,
                        LastName VARCHAR(100) NOT NULL,
                        DateOfBirth DATE NOT NULL,
                        Position VARCHAR(100) NOT NULL,
                        Height VARCHAR(100),
                        Weight VARCHAR(100),
                        YearsInLeague INT,
                        Country VARCHAR(100),
                        PRIMARY KEY (ID, Position, YearsInLeague)
                    );'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()
        self.update_data()
