from parsers.parser import Parser
from NBA_data_types import Game


class Games(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)

    def parse(self, raw_data):
        # Access the 'lscd' list within the json
        games_data = raw_data['data']['lscd']

        # Initialize an empty list to hold the players
        games_list = []

        # Iterate through each month in the games_data list
        for games_month in games_data:
            game_data_raw = games_month['mscd']['g']
            for game in game_data_raw:
                # Extract the relevant information from the json and create a new Game object
                id = game['gid']
                date = game['gdtutc']
                home_team_id = game['h']['tid']
                visitors_team_id = game['v']['tid']
                live_status = game['stt']
                games_list.append(Game(id, date, home_team_id, visitors_team_id, live_status))
        #
        # for game in games_list:
        #     # print(game)
        self.data = games_list
