from handlers.BaseHandler import BaseHandler


class TeamsGameScores(BaseHandler):
    def __init__(self, name, db, data):
        super().__init__(name, db, data)
        table_spec = '''CREATE TABLE {} (
        GameID VARCHAR(100) NOT NULL,
        TeamID BingInt NOT NULL,
        ScoreQOne INT,
        ScoreQTwo INT,
        ScoreQThree INT,
        ScoreQFour INT,
        FGA INT,
        FGM INT,
        FGPER FLOAT,
        ThreePA INT,
        ThreePM INT,
        ThreePPER FLOAT,
        FTA INT,
        FTM INT,
        FTPER FLOAT,
        OREB INT,
        DREB INT,
        AST INT,
        STL INT,
        BLK INT,
        TournOvers INT,
        PTS INT,
        PRIMARY KEY (GameID, TeamID));'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()
        self.update_data()
class PlayersGameScores(BaseHandler):
    def __init__(self, name, db, data, cache=None):
        super().__init__(name, db, data)
        table_spec = '''CREATE TABLE {} (
        GameID varchar(100) NOT NULL,
        PlayerID BingInt NOT NULL,
        PlayTimeInSec INT,
        PlusMinusRank INT,
        FGA INT,
        FGM INT,
        FGPER FLOAT,
        ThreePA INT,
        ThreePM INT,
        ThreePPER FLOAT,
        FTA INT,
        FTM INT,
        FTPER FLOAT,
        OREB INT,
        DREB INT,
        AST INT,
        STL INT,
        BLK INT,
        TournOvers INT,
        PTS INT ,
        PRIMARY KEY (GameID, PlayerID));'''.format(self.name)
        self.create_table(table_spec)
        self.insert_elements()
        self.update_data()