import sqlite3

from handlers.base_handler import BaseHandler, get_columns_from_prop, get_values_from_element


class GamesHandler(BaseHandler):
    def __init__(self, name, db, data, cache):
        self.key_attr = ["ID"]
        super().__init__(name, db, data, cache)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                        ID VARCHAR(100) PRIMARY KEY,
                        Date DATE NOT NULL,
                        HomeTeamID BingInt NOT NULL,
                        VisitorTeamID BingInt NOT NULL,
                        LiveStatus VARCHAR(100) NOT NULL);'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()
        self.update_data()

    def where_condition(self, element):
        return "ID = {}".format(element.ID)