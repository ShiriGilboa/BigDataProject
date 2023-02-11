from handlers.BaseHandler import BaseHandler


class TeamRostersHandler(BaseHandler):
    def __init__(self, name, db, data, cache=None):
        super().__init__(name, db, data, cache)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                                TeamID BingInt NOT NULL,
                                PlayerID BingInt NOT NULL);'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()
        self.update_data()
