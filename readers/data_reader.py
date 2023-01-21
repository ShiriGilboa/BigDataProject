import os.path

import requests
import json

BASE_URL = "https://data.nba.com/data/v2022/json/mobile_teams/nba"


def send_request(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except requests.exceptions.ConnectionError as conn_err:
        print(f'Error Connecting: {conn_err}')
    except requests.exceptions.Timeout as time_err:
        print(f'Timeout Error: {time_err}')
    except requests.exceptions.RequestException as req_err:
        print(f'Something went wrong: {req_err}')
    except Exception as ex:
        print(f'Something went wrong: {ex}')
    return None


class BaseReader:
    def __init__(self, data_url=None, base_url=BASE_URL, year="2022"):
        self.base_url = base_url
        self.year = year
        if data_url:
            self.full_url = self.get_url_by_data_type(data_url)

    def get_url_by_data_type(self, data_type):
        return os.path.join(self.base_url, self.year, data_type)

    def get_data(self):
        response = send_request(self.full_url)
        return response


class GameLineUpReader(BaseReader):
    def __init__(self, game_id):
        super().__init__()
        self.full_url = self.get_url_by_data_type("scores/roster_lineup/{}_roster_lineup.json".format(game_id))



class GameBoxScoreReader(BaseReader):
    def __init__(self, game_id):
        super().__init__()
        self.full_url = self.get_url_by_data_type("scores/gamedetail/{}_gamedetail.json".format(game_id))

#
#
# class TeamRosterReader(BaseReader):
#     def __init__(self, team_name):
#         super().__init__()
#         self.full_url = self.get_url_by_data_type("teams/{}_roster.json".format(team_name))

