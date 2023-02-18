import sqlite3

from NBA_data_types import Player, Game, TeamRoster, GameLineUp, GamePlayerScore, GameTeamScore, Team


class NBAFeedFromDB:
    def __init__(self, db):
        print("Creating cache......for teams, players, games")
        self.update_cache(db)
        print("Great! we loaded our cache from DB..")

    def update_cache(self, db):
        '''
        Load data from DB to local vairables.
        '''
        try:
            self.players = self.get_players(db)
            self.games = self.get_games(db)
            self.teams = self.get_teams(db)
            self.teamrosters = self.get_team_rosters(db)
            self.gameslineups = self.get_all_game_lineups(db)
        except Exception as ex:
            print("Failed to get data from DB {}".format(ex))
            raise OSError("Couldn't get any data from DB.. no cache will be used")

    def get_players(self, db):
        '''
        Get All Players data stored in DB
        '''
        players = []
        try:
            cursor = db.execute(f'''select * from Players''')
        except Exception as ex:
            print("Failed to get Players from DB..")
            return None

        for row in cursor:
            ID, FirstName, LastName, DateOfBirth, Position, Height, Weight, YearsInLeague, Country = row
            player = Player(ID, FirstName, LastName, DateOfBirth, Position, Height, Weight, YearsInLeague, Country)
            players.append(player)
        return players

    def get_teams(self, db):
        teams = []
        try:
            cursor = db.execute(f'''select * from Teams''')
        except Exception as ex:
            print("Failed to get {} from DB, want use cache for this data...".format("Teams"))
            return None
        for row in cursor:
            ID, TeamName, TeamCode, City = row
            team = Team(ID, TeamName, TeamCode, City)
            teams.append(team)
        return teams

    def get_games(self, db):
        games = []
        try:
            cursor = db.execute(f'''select * from Games''')
        except Exception as ex:
            print("Failed to get {} from DB, want use cache for this data...".format("Games"))
            return None

        for row in cursor:
            ID, Date, HomeTeamID, VisitorTeamID, LiveStatus = row
            game = Game(ID, Date, HomeTeamID, VisitorTeamID, LiveStatus)
            games.append(game)
        return games

    # Get TeamRosters:
    def get_team_rosters(self, db):
        rosters = []
        try:
            cursor = db.execute(f'''select * from TeamRosters''')
        except Exception as ex:
            print("Failed to get {} from DB, wםnt use cache for this data...".format("TeamRosters"))
            return None
        for row in cursor:
            TeamID, PlayerID = row
            roster = TeamRoster(TeamID, PlayerID)
            rosters.append(roster)

        return rosters

    # Get GameLineUp:
    def get_all_game_lineups(self, db):
        lineups = []
        try:
            cursor = db.execute(f'''select * from GameLineUps''')
        except Exception as ex:
            print("Failed to get {} from DB, want use cache for this data...".format("GameLineUps"))
            return None

        for row in cursor:
            GameID, TeamID, PlayerID = row
            lineup = GameLineUp(GameID, TeamID, PlayerID)
            lineups.append(lineup)

        return lineups

    def get_game_lineups(self, db ,game_id):
        lineups = []
        cursor = db.execute(f'''select * from GameLineUps where GameId = {game_id}''')

        for row in cursor:
            GameID, TeamID, PlayerID = row
            lineup = GameLineUp(GameID, TeamID, PlayerID)
            lineups.append(lineup)

        return lineups

    # Get Player Scores:
    def get_all_player_scores(self, db):
        player_scores = []
        cursor = db.execute(f'''select * from GamePlayerScores''')

        for row in cursor:
            GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
            player_score = GamePlayerScore(GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA,
                                           ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers,
                                           PTS)
            player_scores.append(player_score)

        return player_scores

    def get_player_scores(self, db, player_id):
        player_scores = []
        cursor = db.execute(f'''select * from GamePlayerScores where PlayerID = {player_id}''')

        for row in cursor:
            GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
            player_score = GamePlayerScore(GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA,
                                           ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers,
                                           PTS)
            player_scores.append(player_score)

        return player_scores

    # Get Team Scores:
    def get_all_team_scores(self, db):
        team_scores = []
        cursor = db.execute(f'''select * from GameTeamScores''')

        for row in cursor:
            GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
            team_score = GameTeamScore(GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA,
                                       FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL,
                                       BLK, TournOvers, PTS)
            team_scores.append(team_score)

        return team_scores

    def get_team_scores(self, db ,game_id):
        team_scores = []
        cursor = db.execute(f'''select * from GameTeamScores where GameId = {game_id}''')

        for row in cursor:
            GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
            team_score = GameTeamScore(GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER,
                                       ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK,
                                       TournOvers, PTS)
            team_scores.append(team_score)

        return team_scores
    @staticmethod
    def get_final_games_ids(db_connection):
        try:
            ret = db_connection.execute('''SELECT ID FROM Games WHERE LiveStatus='Final' ''')
            final_game_ids = ret.fetchall()
            print(final_game_ids)
            return final_game_ids
        except Exception as e:
            print(f"Error: {e}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    manager = NBA_data_manager("NBA_DB_2.db")
    with manager as e:
        for game in manager.get_games():
            print(game.ID)
