import sqlite3

from NBA_DB_manager import get_final_games_ids
from handlers import Games, Teams, Players
import handlers.Teams
import parsers.Games
import parsers.Players
import parsers.Team
from handlers.GameScoresHandler import TeamsGameScores, PlayersGameScores
from handlers.GamesLineUp import GamesLineUpHandler
from parsers.GameBoxScore import GameBoxScoresTeams, GameBoxScoresPlayers
from parsers.GamesLinesUp import GamesLineUp
from readers import data_reader

from readers.data_reader import GameLineUpReader, GameBoxScoreReader
from sql_db_manager import get_sql_con

DEFAULT_DB_NAME = "NBA_DATA_BIG_DATA_PROJECT.db"
sql_con = get_sql_con(db_name=DEFAULT_DB_NAME)

data_types = {
    "Games": {"Reader": data_reader.BaseReader, "url": "league/00_full_schedule.json", "Parser": parsers.Games.Games,
              "Handler": handlers.Games.GamesHandler},
    "Players": {"Reader": data_reader.BaseReader, "url": "players/00_player_info.json",
                "Parser": parsers.Players.Players, "Handler": handlers.Players.PlayersHandler},
    "Teams": {"Reader": data_reader.BaseReader, "url": "teams/00_team_info.json", "Parser": parsers.Team.Teams,
              "Handler": handlers.Teams.TeamsHandler},
}


def ingest_naive_data(db_connection):
    '''
    This method will handle the ETL pipline for the naive data types (Games, Players, Teams) means, data type that
    doesn't require pre-processing manipulation
    :param db_connection: a reference to an open local DB
    :return: None
    '''
    for data_name, data_pipline in data_types.items():
        json_data = data_pipline["Reader"](data_pipline["url"]).get_data()
        data = data_pipline["Parser"](json_data)
        data_pipline["Handler"](data_name, db_connection, data)
        db_connection.commit()


def ingest_games_data(db_connection):
    final_games = [t[0] for t in get_final_games_ids(db_connection.cursor())]
    for game in final_games:
        print(game)
        data = GameLineUpReader(game).get_data()
        games_line_up_data = GamesLineUp(data)
        GamesLineUpHandler("GameLineUps", db_connection, games_line_up_data)

        scores_data = GameBoxScoreReader(game).get_data()
        team_scores = GameBoxScoresTeams(scores_data)
        TeamsGameScores("GameTeamScores", db_connection, team_scores)

        players_scores = GameBoxScoresPlayers(scores_data)
        PlayersGameScores("GamePlayerScores", db_connection, players_scores)


try:
    ingest_naive_data(sql_con)
    ingest_games_data(sql_con)
except Exception as e:
    print("Caught exception during data ingestion {}".format(e))
finally:
    sql_con.close()
