from parsers.parser import Parser
from NBA_data_types import Team

class Teams(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)

    def parse(self, data):
        # Access the 't' list within the json
        teams_data = data['data']['tms']['t']

        # Initialize an empty list to hold the teams
        teams_list = []

        # Iterate through each team in the teams_data list
        for team in teams_data:
            # Extract the relevant information from the json and create a new Teams object
            id = team['tid']
            name = team['tn']
            code = team['ta']
            city = team['tc']
            teams_list.append(Team(id, name, code, city))

        for team_p in teams_list:
            print(team_p)
        self.data = teams_list
