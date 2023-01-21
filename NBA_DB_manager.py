import sqlite3

from NBA_data_types import Player, Game, TeamRoster, GameLineUp, GamePlayerScore, GameTeamScore


def get_players():
    players = []
    cursor = self.db.execute(f'''select * from Players''')

    for row in cursor:
        ID, FirstName, LastName, DateOfBirth, Position, Height, Weight, YearsInLeague, Country = row
        player = Player(ID, FirstName, LastName, DateOfBirth, Position, Height, Weight, YearsInLeague, Country)
        players.append(player)
    return players


# Get Games:
def get_games():
    games = []
    cursor = self.db.execute(f'''select * from Games''')

    for row in cursor:
        ID, Date, HomeTeamID, VisitorTeamID, LiveStatus = row
        game = Game(ID, Date, HomeTeamID, VisitorTeamID, LiveStatus)
        games.append(game)
    return games


# Get TeamRosters:
def get_team_rosters():
    rosters = []
    cursor = self.db.execute(f'''select * from TeamRosters''')

    for row in cursor:
        TeamID, PlayerID = row
        roster = TeamRoster(TeamID, PlayerID)
        rosters.append(roster)

    return rosters


# Get GameLineUp:
def get_all_game_lineups():
    lineups = []
    cursor = self.db.execute(f'''select * from GameLineUps''')

    for row in cursor:
        GameID, TeamID, PlayerID = row
        lineup = GameLineUp(GameID, TeamID, PlayerID)
        lineups.append(lineup)

    return lineups


def get_game_lineups(game_id):
    lineups = []
    cursor = self.db.execute(f'''select * from GameLineUps where GameId = {game_id}''')

    for row in cursor:
        GameID, TeamID, PlayerID = row
        lineup = GameLineUp(GameID, TeamID, PlayerID)
        lineups.append(lineup)

    return lineups


# Get Player Scores:
def get_all_player_scores():
    player_scores = []
    cursor = self.db.execute(f'''select * from GamePlayerScores''')

    for row in cursor:
        GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
        player_score = GamePlayerScore(GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA,
                                       ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS)
        player_scores.append(player_score)

    return player_scores


def get_player_scores(game_id):
    player_scores = []
    cursor = self.db.execute(f'''select * from GamePlayerScores where GameId = {game_id}''')

    for row in cursor:
        GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
        player_score = GamePlayerScore(GameID, PlayerID, PlayTimeInSec, PlusMinusRank, FGA, FGM, FGPER, ThreePA,
                                       ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS)
        player_scores.append(player_score)

    return player_scores


# Get Team Scores:
def get_all_team_scores():
    team_scores = []
    cursor = self.db.execute(f'''select * from GameTeamScores''')

    for row in cursor:
        GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
        team_score = GameTeamScore(GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA,
                                   FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL,
                                   BLK, TournOvers, PTS)
        team_scores.append(team_score)

    return team_scores


def get_team_scores(game_id):
    team_scores = []
    cursor = self.db.execute(f'''select * from GameTeamScores where GameId = {game_id}''')

    for row in cursor:
        GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER, ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK, TournOvers, PTS = row
        team_score = GameTeamScore(GameID, TeamID, ScoreQOne, ScoreQTwo, ScoreQThree, ScoreQFour, FGA, FGM, FGPER,
                                   ThreePA, ThreePM, ThreePPER, FTA, FTM, FTPER, OREB, DREB, AST, STL, BLK,
                                   TournOvers, PTS)
        team_scores.append(team_score)

    return team_scores


def get_final_games_ids(cursor):
    try:
        ret = cursor.execute('''SELECT ID FROM Games WHERE LiveStatus='Final' ''')
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
