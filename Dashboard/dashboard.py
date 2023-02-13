import findspark
findspark.init()

import random
import os
import numpy as np
from pyspark.sql import SparkSession

from dash import Dash, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import dcc
from dash import html
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import glob

def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])


spark = SparkSession.builder.appName('treatmeant').config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = "BigDataProject/Dashboard/game_team_score_grouped_sorted_teamScore/"
csv_files = glob.glob(os.path.join(path, "*.csv"))
# loop over the list of csv files
for f in csv_files:
      
    # read the csv file
    game_team_score_grouped = spark.read.csv(f, inferSchema = True, header = True)
    game_team_score_grouped = game_team_score_grouped.pandas_api()

path = "BigDataProject/Dashboard/game_player_score_grouped_sorted_playerScore/"
csv_files = glob.glob(os.path.join(path, "*.csv"))
# loop over the list of csv files
for f in csv_files:
      
    # read the csv file
    game_player_score_grouped = spark.read.csv(f, inferSchema = True, header = True)
    game_player_score_grouped = game_player_score_grouped.pandas_api()
    df = pd.read_csv(f)

path = "BigDataProject/Dashboard/last_games/"
csv_files = glob.glob(os.path.join(path, "*.csv"))
# loop over the list of csv files
for f in csv_files:
      
    # read the csv file
    last_games = spark.read.csv(f, inferSchema = True, header = True)
    last_games = last_games.pandas_api()
    df = pd.read_csv(f)


game_player_score = pd.read_csv("./BigDataProject/Data analysis/data/GamePlayerScore.csv")
# game_player_score = game_player_score.toPandas()
# print(game_player_score.columns)

# game_team_score_grouped = spark.read.csv("BigDataProject/Dashboard/game_team_score_grouped_sorted_teamScore.csv", inferSchema = True, header = True)
# game_team_score_grouped = game_team_score_grouped.pandas_api()

# game_player_score_grouped = spark.read.csv("BigDataProject/Dashboard/game_player_score_grouped_sorted_playerScore.csv", inferSchema = True, header = True)
# game_player_score_grouped = game_player_score_grouped.pandas_api()

app = Dash(external_stylesheets=[dbc.themes.SLATE])

app.layout = html.Div(
    children=[
        dbc.Row(
            html.H1(children="NBA Analytics - BDP final project 2023", style={'textAlign': 'center'})
        ),
        dbc.Row(html.H3(
            children="Analyze NBA data from is “NBA Mobile Stats Feed” ( NBA Stats DATA, Games, Livescore, Standings, Players, Statistics)", style={'textAlign': 'center'})
        ),
        dbc.Tabs(
            [
                dbc.Tab(label="General statistics", tab_id="tab-1"),
                dbc.Tab(label="Teams statistics", tab_id="tab-2"),
            ],
            id="tabs",
            active_tab="tab-1",
        ),
        html.Div(id='content')
    ]
)

tab1_content = dbc.Card(
    dbc.CardBody(
        [
            html.Div(
                children = [
                    dbc.Row([
                        dbc.Col(html.H5(children="Leading Teams", style={'textAlign': 'center'}), width = 3, style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'}),
                        dbc.Col(html.H5(children="Leading Players", style={'textAlign': 'center'}), width = 3, style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'}),
                        dbc.Col(html.H5(children="Last Games Results", style={'textAlign': 'center'}), width = 3, style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'})

                    ]),
                    dbc.Row([
                        dbc.Col(dbc.Table.from_dataframe(game_team_score_grouped.iloc[:10, :].round({'Score': 2}), bordered=True, hover=True),
                                width = 3,
                                style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'}),
                        dbc.Col(dbc.Table.from_dataframe(game_player_score_grouped.iloc[:10, :].round({'Score': 2}), bordered=True, hover=True),
                                width = 3,
                                style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'}),
                        dbc.Col(dbc.Table.from_dataframe(last_games, bordered=True, hover=True),
                                style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'})
                    ]),
                    dbc.Row([
                        dbc.Col(dcc.Graph(id="graph1")),
                        dbc.Col(dbc.Table.from_dataframe(game_player_score_grouped.iloc[:10, :].round({'Score': 2}), bordered=True, hover=True),
                                width = 3,
                                style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'}),
                        dbc.Col(dbc.Table.from_dataframe(last_games, bordered=True, hover=True),
                                style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px'})
                    ])
                ]
            )
        ]
    ),
    className="mt-3",
)

tab2_content = dbc.Card(
    dbc.CardBody(
        html.Div(
            children = [
                dbc.DropdownMenu(
                [
                    dbc.DropdownMenuItem(
                        "A button", id="dropdown-button", n_clicks=0
                    ),
                    dbc.DropdownMenuItem(
                        "Internal link", href="/docs/components/dropdown_menu"
                    ),
                    dbc.DropdownMenuItem(
                        "External Link", href="https://github.com"
                    ),
                    dbc.DropdownMenuItem(
                        "External relative",
                        href="/docs/components/dropdown_menu",
                        external_link=True,
                    ),
                ],
                label="Menu",
                ),
                html.P(id="item-clicks", className="mt-3"),
            ]
)

    ),
    className="mt-3",
)


@app.callback(Output("content", "children"), [Input("tabs", "active_tab")])
def switch_tab(at):
    if at == "tab-1":
        return tab1_content
    elif at == "tab-2":
        return tab2_content
    return html.P("This shouldn't ever be displayed...")

@app.callback(Output("graph1", "figure"), [Input("tabs", "active_tab")])
def tab_content1(active_tab):
    if active_tab is not None:
        if active_tab == "tab-1":
            fig1 = go.Figure()
            fig1.add_trace(
                px.histogram(
                    game_player_score,
                    x = 'ThreePPER'
                )
            )
            # set background color
            fig1.update_layout(
                plot_bgcolor="black", autosize=False, width=1000, height=550
            )
            return fig1
    return go.Figure()
game_player_score


if __name__ == '__main__':
    app.run_server(debug=True)