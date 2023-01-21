from handlers.BaseHandler import BaseHandler


class GamesLineUpHandler(BaseHandler):
    def __init__(self, name, db, data):
        super().__init__(name, db, data)
        table_spec = '''CREATE TABLE IF NOT EXISTS {} (
                            GameID VARCHAR(100) NOT NULL,
                            TeamID BingInt NOT NULL,
                            PlayerID BingInt NOT NULL);'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements(self.data)