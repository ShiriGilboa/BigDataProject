from handlers.base_handler import BaseHandler


class GamesLineUpHandler(BaseHandler):
    def __init__(self, name, db, data, cache=None):
        super().__init__(name, db, data, cache)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                            GameID VARCHAR(100) NOT NULL,
                            TeamID BingInt NOT NULL,
                            PlayerID BingInt NOT NULL);'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()