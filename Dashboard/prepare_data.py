import findspark
findspark.init()

import random
import os
import numpy as np
from pyspark.sql import SparkSession
import pyspark.pandas as ps

from dash import Dash, dash_table
from dash import dcc
from dash import html
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

spark = SparkSession.builder.appName('treatmeant').config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load all tables
teams = spark.read.csv("./BigDataProject/Data analysis/data/Teams.csv", inferSchema=True, header=True)
teams = teams.to_pandas_on_spark()
players = spark.read.csv("./BigDataProject/Data analysis/data/Players.csv", inferSchema=True, header=True)
players = players.to_pandas_on_spark()
games = spark.read.csv("./BigDataProject/Data analysis/data/Games.csv", inferSchema=True, header=True)
games = games.to_pandas_on_spark()
team_rosters = spark.read.csv("./BigDataProject/Data analysis/data/TeamRosters.csv", inferSchema=True, header=True)
team_rosters = team_rosters.to_pandas_on_spark()
game_line_up = spark.read.csv("./BigDataProject/Data analysis/data/GameLineUp.csv", inferSchema=True, header=True)
game_line_up = game_line_up.to_pandas_on_spark()
game_team_score = spark.read.csv("./BigDataProject/Data analysis/data/GameTeamScore.csv", inferSchema=True, header=True)
game_team_score = game_team_score.to_pandas_on_spark()
game_player_score = spark.read.csv("./BigDataProject/Data analysis/data/GamePlayerScore.csv", inferSchema=True, header=True)
game_player_score = game_player_score.to_pandas_on_spark()

# Create dictioneries of teams and players for easy converting between ID's and names
teams_dict = {row['ID']: row['City'] + " " + row['TeamName'] for index, row in teams.iterrows()}
team_names_dict = {row['City'] + " " + row['TeamName']: row['ID']  for index, row in teams.iterrows()}
players_dict = {row['ID']: row['FirstName'] + " " + row['LastName'] for index, row in players.iterrows()}

# Remove unnecessary data
games = games.loc[(games['HomeTeamID'].isin(teams_dict.keys())) & (games['VisitorTeamID'].isin(teams_dict.keys())), :]
game_line_up = game_line_up.loc[game_line_up['GameID'].isin(games['ID'].to_numpy())]
game_team_score = game_team_score.loc[game_team_score['TeamID'].isin(teams_dict.keys()), :]
game_player_score = game_player_score.loc[game_player_score['PlayerID'].isin(players_dict.keys()), :]
finished_games = games.loc[games['LiveStatus'] == 'Final', :]

# Add relevant fields
game_team_score['REB'] = game_team_score['OREB'] + game_team_score['DREB']
game_player_score['REB'] = game_player_score['OREB'] + game_player_score['DREB']

# Create teams ranking table
game_team_score_grouped = game_team_score.groupby(['TeamID']).mean().reset_index()
teams_wins = {teamID: 0 for teamID in teams_dict.keys()}    
teams_losses = {teamID: 0 for teamID in teams_dict.keys()}

for i, row in finished_games.iterrows():        
    visitor_team_pts = game_team_score.loc[(game_team_score['GameID'] == row['ID']) & (game_team_score['TeamID'] == row['VisitorTeamID']), 'PTS'].to_numpy()[0]
    home_team_pts = game_team_score.loc[(game_team_score['GameID'] == row['ID']) & (game_team_score['TeamID'] == row['HomeTeamID']), 'PTS'].to_numpy()[0]
    
    if visitor_team_pts > home_team_pts:
        teams_wins[row['VisitorTeamID']] += 1
        teams_losses[row['HomeTeamID']] += 1

    else:
        teams_wins[row['HomeTeamID']] += 1
        teams_losses[row['VisitorTeamID']] += 1

for key, value in teams_wins.items():
    game_team_score_grouped.loc[game_team_score_grouped['TeamID'] == key, 'wins'] = value

game_team_score_grouped['Power Score'] = 0.2 * game_team_score_grouped['wins'] + \
                                        0.2 * game_team_score_grouped['PTS'] + \
                                        0.2 * game_team_score_grouped['AST'] + \
                                        0.1 * game_team_score_grouped['STL'] + \
                                        0.1 * game_team_score_grouped['BLK'] + \
                                        0.2 * game_team_score_grouped['REB']

game_team_score_grouped.sort_values('Power Score', ascending = False, inplace= True)

game_team_score_grouped['Team'] = [teams_dict[teamID] for teamID in game_team_score_grouped['TeamID'].to_numpy()]

game_team_score_grouped.loc[:, ['Team', 'Power Score']].to_csv('BigDataProject/Dashboard/game_team_score_grouped_sorted_teamScore')

# Create players ranking table
game_player_score_grouped = game_player_score.groupby(['PlayerID']).mean().reset_index()
game_player_score_grouped['Power Score'] = 0.4 * game_player_score_grouped['PTS'] + \
                                            0.2 * game_player_score_grouped['AST'] + \
                                            0.1 * game_player_score_grouped['STL'] + \
                                            0.1 * game_player_score_grouped['BLK'] + \
                                            0.2 * game_player_score_grouped['REB']

game_player_score_grouped['Player'] = [players_dict[playerID] for playerID in game_player_score_grouped['PlayerID'].to_numpy()]

game_player_score_grouped.sort_values('Power Score', ascending = False, inplace = True)

game_player_score_grouped.loc[:, ['Player', 'Power Score']].to_csv('BigDataProject/Dashboard/game_player_score_grouped_sorted_playerScore')

# Create last games table

last_games = finished_games.sort_values('Date', ascending = False).iloc[:10, :]
last_games['Home Team'] = [teams_dict[teamID] for teamID in last_games['HomeTeamID'].to_numpy()]
last_games['Visitor Team'] = [teams_dict[teamID] for teamID in last_games['VisitorTeamID'].to_numpy()]

home_teams_results = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['HomeTeamID'].to_numpy())), ['GameID', 'TeamID', 'PTS']].rename(columns={"PTS": "Home Team score"})
visitor_teams_results = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['VisitorTeamID'].to_numpy())), ['GameID', 'TeamID', 'PTS']].rename(columns={"PTS": "Visitor Team score"})

home_teams_FG = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['HomeTeamID'].to_numpy())), ['GameID', 'TeamID', 'FGPER']].rename(columns={"FGPER": "Home Team FG%"})
visitor_teams_FG = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['VisitorTeamID'].to_numpy())), ['GameID', 'TeamID', 'FGPER']].rename(columns={"FGPER": "Visitor Team FG%"})

home_teams_3FG = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['HomeTeamID'].to_numpy())), ['GameID', 'TeamID', 'ThreePPER']].rename(columns={"ThreePPER": "Home Team 3FG%"})
visitor_teams_3FG = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['VisitorTeamID'].to_numpy())), ['GameID', 'TeamID', 'ThreePPER']].rename(columns={"ThreePPER": "Visitor Team 3FG%"})

home_teams_FT = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['HomeTeamID'].to_numpy())), ['GameID', 'TeamID', 'FTPER']].rename(columns={"FTPER": "Home Team FT%"})
visitor_teams_FT = game_team_score.loc[(game_team_score['GameID'].isin(last_games['ID'].to_numpy())) & (game_team_score['TeamID'].isin(last_games['VisitorTeamID'].to_numpy())), ['GameID', 'TeamID', 'FTPER']].rename(columns={"FTPER": "Visitor Team FT%"})

last_games = ps.merge(last_games, home_teams_results, left_on=['ID', 'HomeTeamID'], right_on=['GameID', 'TeamID'])
last_games = ps.merge(last_games, visitor_teams_results, left_on=['ID', 'VisitorTeamID'], right_on=['GameID', 'TeamID']).drop(columns=['GameID_x', 'TeamID_x', 'GameID_y', 'TeamID_y'])
last_games = ps.merge(last_games, home_teams_FG, left_on=['ID', 'HomeTeamID'], right_on=['GameID', 'TeamID'])
last_games = ps.merge(last_games, visitor_teams_FG, left_on=['ID', 'VisitorTeamID'], right_on=['GameID', 'TeamID']).drop(columns=['GameID_x', 'TeamID_x', 'GameID_y', 'TeamID_y'])
last_games = ps.merge(last_games, home_teams_3FG, left_on=['ID', 'HomeTeamID'], right_on=['GameID', 'TeamID'])
last_games = ps.merge(last_games, visitor_teams_3FG, left_on=['ID', 'VisitorTeamID'], right_on=['GameID', 'TeamID']).drop(columns=['GameID_x', 'TeamID_x', 'GameID_y', 'TeamID_y'])
last_games = ps.merge(last_games, home_teams_FT, left_on=['ID', 'HomeTeamID'], right_on=['GameID', 'TeamID'])
last_games = ps.merge(last_games, visitor_teams_FT, left_on=['ID', 'VisitorTeamID'], right_on=['GameID', 'TeamID']).drop(columns=['GameID_x', 'TeamID_x', 'GameID_y', 'TeamID_y'])

last_games.loc[:, ['Home Team', "Home Team score", 'Home Team FG%', 'Home Team 3FG%', 'Home Team FT%', 'Visitor Team', "Visitor Team score", "Visitor Team FG%", "Visitor Team 3FG%", "Visitor Team FT%"]].to_csv('BigDataProject/Dashboard/last_games')
