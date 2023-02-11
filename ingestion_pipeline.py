from handlers.teams_rosters_handler import TeamRostersHandler
from parsers.team_roster import TeamRosters
from db_cache_manager import NBAFeedFromDB
from handlers import games, teams, players
import handlers.teams
import parsers.game_parser
import parsers.players_parser
import parsers.team_parser
from handlers.game_score_handler import TeamsGameScores, PlayersGameScores
from handlers.games_line_up import GamesLineUpHandler
from parsers.game_box_score import GameBoxScoresTeams, GameBoxScoresPlayers
from parsers.games_lines_up import GamesLineUp
from readers import data_reader
from readers.data_reader import GameLineUpReader, GameBoxScoreReader


class IngestionPipeLineManager:
    '''
    Class purpose is to manage all pipleline operation and build/update the DB.
    '''

    # This is configuration for each of the main types , each type has its own reader, url (entry point) ,
    # Parser and Handler.
    data_types = {
        "Games": {"Reader": data_reader.BaseReader, "url": "league/00_full_schedule.json",
                  "Parser": parsers.game_parser.Games,
                  "Handler": handlers.games.GamesHandler},
        "Players": {"Reader": data_reader.BaseReader, "url": "players/00_player_info.json",
                    "Parser": parsers.players_parser, "Handler": handlers.players.PlayersHandler},
        "Teams": {"Reader": data_reader.BaseReader, "url": "teams/00_team_info.json", "Parser": parsers.team_parser.Teams,
                  "Handler": handlers.teams.TeamsHandler},
    }

    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.cache = None
        self.load_cache()

    def run(self):
        self.ingest_naive_data()
        self.ingest_team_roster()
        self.ingest_games_data()
        if not self.cache:
            self.load_cache()
        else:
            self.cache.update_cache()
        self.db_connection.close()

    def load_cache(self):
        try:
            self.cache = NBAFeedFromDB(self.db_connection)
        except OSError as ex:
            print("Caught exception when trying to load cache from DB. Not going to use cache {}".format(ex))
            self.cache = None

    def ingest_naive_data(self):
        '''
        This method will handle the ETL pipline for the naive data types (Games, Players, Teams) means, data type that
        doesn't require pre-processing manipulation
        :param db_connection: a reference to an open local DB
        :return: None
        '''
        for data_name, data_pipline in self.data_types.items():
            json_data = data_pipline["Reader"](data_pipline["url"]).get_data()
            data = data_pipline["Parser"](json_data)
            data_pipline["Handler"](data_name, self.db_connection, data, self.cache)
            self.db_connection.commit()

    def ingest_team_roster(self):
        try:
            json_data = self.data_types["Players"]["Reader"](self.data_types["Players"]["url"]).get_data()
            data = TeamRosters(json_data)
            TeamRostersHandler("TeamRosters", self.db_connection, data, self.cache)
        except Exception as ex:
            print("Caught exception when trying to get team roster {}".format(ex))
        finally:
            self.db_connection.commit()

    def ingest_games_data(self):
        # Get the most updated final games from the DB
        updated_final_games = [t[0] for t in NBAFeedFromDB.get_final_games_ids(self.db_connection)]
        cached_final_games = [game.ID for game in
                              filter(lambda x: x.LiveStatus == "Final", self.cache.games)] if self.cache and self.cache.games else []
        new_finals = list(set(updated_final_games) - set(cached_final_games))
        # Here, we are going to iterate only on new final games. not all if them.
        for game in new_finals:
            print(game)
            data = GameLineUpReader(game).get_data()
            games_line_up_data = GamesLineUp(data)
            GamesLineUpHandler("GameLineUps", self.db_connection, games_line_up_data)

            scores_data = GameBoxScoreReader(game).get_data()
            team_scores = GameBoxScoresTeams(scores_data)
            TeamsGameScores("GameTeamScores", self.db_connection, team_scores)

            players_scores = GameBoxScoresPlayers(scores_data)
            PlayersGameScores("GamePlayerScores", self.db_connection, players_scores)
