import sqlite3

from handlers.BaseHandler import BaseHandler, get_columns_from_prop, get_values_from_element


class GamesHandler(BaseHandler):
    def __init__(self, name, db, data):
        super().__init__(name, db, data)
        table_spec = '''CREATE TABLE IF NOT EXISTS Games (
                        ID VARCHAR(100) PRIMARY KEY,
                        Date DATE NOT NULL,
                        HomeTeamID BingInt NOT NULL,
                        VisitorTeamID BingInt NOT NULL,
                        LiveStatus VARCHAR(100) NOT NULL);'''.format(self.name)
        self.create_table(table_spec)
        self.handle_updated("ID", ["LiveStatus"])
        self.insert_elements(self.data)
