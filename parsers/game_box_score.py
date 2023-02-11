from NBA_data_types import GamePlayerScore, GameTeamScore
from parsers.parser import Parser


def get_percentage(numerator, denominator) -> int:
    if not all([numerator, denominator]):
        return 0
    else:
        return int((numerator / denominator) * 100)


class GameBoxScoresTeams(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)

    # Game Box Score Parser
    def parse(self, raw_data):
        # Access the 'data' list within the json
        box_score_data = raw_data['data']['g']
        # Extract game_id
        game_id = box_score_data['gid']

        # Initialize an empty lists to hold the scores
        game_teams_score_list = []

        # Extract visitor team score
        visitor_scores = box_score_data['vls']
        # called the team_score_helper Func to parse the relevant fields
        game_teams_score_list.append(self.game_team_score_parser_helper(visitor_scores, game_id))
        # Extract Home team score
        home_scores = box_score_data['hls']
        # called the team_score_helper Func to parse the relevant fields
        game_teams_score_list.append(self.game_team_score_parser_helper(home_scores, game_id))

        self.data = game_teams_score_list

    # Game Team Score Parser helper
    def game_team_score_parser_helper(self, team_score_json, game_id) -> GameTeamScore:
        team_points = team_score_json['s']
        team_id = team_score_json['tid']
        q1 = team_score_json['q1']
        q2 = team_score_json['q2']
        q3 = team_score_json['q3']
        q4 = team_score_json['q4']

        team_stat = team_score_json['tstsg']
        fga = team_stat['fga']
        fgm = team_stat['fgm']
        fg_per = get_percentage(team_stat['fgm'], team_stat['fga'])
        three_pa = team_stat['tpa']
        three_pm = team_stat['tpm']
        three_per = get_percentage(team_stat['tpm'], team_stat['tpa'])
        fta = team_stat['fta']
        ftm = team_stat['ftm']
        ft_per = get_percentage(team_stat['ftm'], team_stat['fta'])
        o_reb = team_stat['oreb']
        d_reb = team_stat['dreb']
        ast = team_stat['ast']
        stl = team_stat['stl']
        blk = team_stat['blk']
        to = team_stat['tov']
        pts = team_points

        return GameTeamScore(game_id, team_id, q1, q2, q3, q4, fga, fgm, fg_per, three_pa, three_pm, three_per, fta,
                             ftm,
                             ft_per, o_reb, d_reb, ast, stl, blk, to, pts)


class GameBoxScoresPlayers(Parser):
    def __init__(self, raw_data):
        self.data = []
        super().__init__(raw_data)

    # Game Box Score Parser
    def parse(self, raw_data) :
        # Access the 'data' list within the json
        box_score_data = raw_data['data']['g']
        # Extract game_id
        game_id = box_score_data['gid']

        # Initialize an empty lists to hold the scores
        game_players_score_list = []

        # Extract visitor team score
        visitor_scores = box_score_data['vls']

        # Iterate over the players stats and extract the relevant stat with the game_player_score Func
        for player_stats in visitor_scores['pstsg']:
            game_players_score_list.append(self.game_player_score_parser_helper(player_stats, game_id))

        # Extract Home team score
        home_scores = box_score_data['hls']
        # Iterate over the players stats and extract the relevant stat with the game_player_score Func
        for player_stats in home_scores['pstsg']:
            game_players_score_list.append(self.game_player_score_parser_helper(player_stats, game_id))

        self.data = game_players_score_list

    # Game Player Score Parser helper
    def game_player_score_parser_helper(self, player_stat, game_id) -> GamePlayerScore:
        fga = player_stat['fga']
        fgm = player_stat['fgm']
        fg_per = get_percentage(player_stat['fgm'], player_stat['fga'])
        three_pa = player_stat['tpa']
        three_pm = player_stat['tpm']
        three_per = get_percentage(player_stat['tpm'], player_stat['tpa'])
        fta = player_stat['fta']
        ftm = player_stat['ftm']
        ft_per = get_percentage(player_stat['ftm'], player_stat['fta'])
        o_reb = player_stat['oreb']
        d_reb = player_stat['dreb']
        ast = player_stat['ast']
        stl = player_stat['stl']
        blk = player_stat['blk']
        to = player_stat['tov']
        pts = player_stat['pts']
        player_id = player_stat['pid']
        play_time_in_sec = player_stat['totsec']
        plus_minus_rank = player_stat['pm']

        return GamePlayerScore(game_id, player_id, play_time_in_sec, plus_minus_rank, fga, fgm, fg_per, three_pa,
                               three_pm, three_per, fta, ftm, ft_per, o_reb, d_reb, ast, stl, blk, to, pts)
