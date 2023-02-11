from parsers.parser import Parser
from NBA_data_types import Player


class Players(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)


    def parse(self, raw_data):
        # Access the 'pl' list within the json
        players_data = raw_data['data']['pls']['pl']

        # Initialize an empty list to hold the players
        players_list = []

        # Iterate through each player in the players_data list
        for player in players_data:
            # Extract the relevant information from the json and create a new Players object
            id = player['pid']
            first_name = player['fn']
            last_name = player['ln']
            date_of_birth = player['dob']
            position = player['pos']
            height = player['ht']
            weight = player['wt']
            years_in_league = player['y']
            country = player['co']
            players_list.append(
                Player(id, first_name, last_name, date_of_birth, position, height, weight, years_in_league, country))

        for player_p in players_list:
            print(player_p)
        self.data = players_list
