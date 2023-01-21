from parsers.parser import Parser
from NBA_data_types import GameLineUp


class GamesLineUp(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)

    def parse(self, raw_data):
        # Access the 'data' list within the json
        lineups_teams_data = raw_data['data']
        # Extract game_id
        game_id = lineups_teams_data['gameId']
        # Initialize an empty list to hold the players
        game_lineup_list = []
        # Extract only the home team
        home_players_lineups = lineups_teams_data['home']
        team_lineup_id = home_players_lineups['teamId']
        # Iterate only on the players
        for home_player_lineup in home_players_lineups['players']:
            team_id = team_lineup_id
            player_id = home_player_lineup['personId']
            game_lineup_list.append(GameLineUp(game_id, team_id, player_id))

        # Extract only the visitor team
        visitor_players_lineups = lineups_teams_data['visitor']
        team_lineup_id = visitor_players_lineups['teamId']
        # Iterate only on the players
        for home_player_lineup in visitor_players_lineups['players']:
            # Extract the relevant information from the json and create a new TeamRoster object
            team_id = team_lineup_id
            player_id = home_player_lineup['personId']
            game_lineup_list.append(GameLineUp(game_id, team_id, player_id))

        self.data = game_lineup_list
