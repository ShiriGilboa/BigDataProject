from parsers.parser import Parser
from NBA_data_types import TeamRoster


class TeamRosters(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)


    def parse(self, raw_data):
        # Access the 'pl' list within the json
        players_data = raw_data['data']['pls']['pl']

        # Initialize an empty list to hold the players
        team_roster_list = []

        # Iterate through each player in the players_data list
        for player in players_data:
            # Extract the relevant information from the json and create a new TeamRoster object
            team_id = player['tid']
            player_id = player['pid']
            team_roster_list.append(TeamRoster(team_id, player_id))

        print("Parsed {} rosters".format(len(team_roster_list)))
        self.data = team_roster_list
