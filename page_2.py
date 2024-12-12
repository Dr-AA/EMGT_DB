from navbar import create_navbar
from dash import dcc, html
import pandas as pd
from sqlalchemy import create_engine
import urllib
import dash_bootstrap_components as dbc
from datetime import date

### ETABLISSEMENT DES CONNEXIONS ================================================================
#MSSQLServer.energymgt.ch\MSSQL_EMGT_I02
access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                    'server': 'SRV-NTD-MSQL-01',
                    'user': 'EMGT_Access',
                    'pwd': '12-NRJ-28'}

quoted_general = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+
                                     access_token['server']+ ';UID='+access_token['user']+
                                         ';PWD='+ access_token['pwd'])

engine_general = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_general))
with engine_general.connect() as con:
    requete = "SELECT name FROM sys.databases;"
    try:
        df_db = pd.read_sql_query(requete, con)
    except:
        print('Erreur Connexion')
        exit(1)

df_db = df_db.drop(axis=0,index=df_db.loc[df_db['name']=='master'].index, errors='ignore')
df_db = df_db.drop(axis=0,index=df_db.loc[df_db['name']=='model'].index, errors='ignore')
df_db = df_db.drop(axis=0,index=df_db.loc[df_db['name']=='msdb'].index, errors='ignore')
df_db = df_db.drop(axis=0,index=df_db.loc[df_db['name']=='tempdb'].index, errors='ignore')
df_db = df_db.drop(axis=0,index=df_db.loc[df_db['name']=='Z_ARCHIVES'].index, errors='ignore')

liste_db = df_db['name'].to_list()
##### ==============================================================================================

nav = create_navbar()

header = html.H4("Visualisation des données")

### RENVOI DE LA PAGE TOTALE
def create_page_2():
    layout = html.Div([
        # Barre de navigation
        dbc.Row(nav),
        dbc.Row(html.Div(header, style={'marginTop': 10, 'marginLeft': 15, 'marginBottom': 10,'justify-content':'center'})),
        dbc.Row([
            dbc.Col(
                [
                    # Dropdowns for columns (populated dynamically)
                    html.Label("1 - Choisissez le Projet",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='db-dropdown', options=liste_db, placeholder="Choose a project",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),

                    # Dropdowns for columns (populated dynamically)
                    html.Label("2 - Choisissez la Table ou est stocké le tag",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='table-dropdown', placeholder="Choose a table",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10})]),
            dbc.Col(
                [
                    # Dropdowns for columns (populated dynamically)
                    html.Label("3 - Choisissez le Tag à visualiser: ",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='tag-dropdown', placeholder="Choose a Tag",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),

                    html.Label("4 - Choisissez le 2è Tag à visualiser: ",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='tag-dropdown-2', placeholder="Choose a 2nd Tag",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10})

                ]),

            dbc.Col(
                [
                    html.Label("5 - Définir les dates à visualiser: ",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    html.Div([
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date(2030, 1, 1),
                            initial_visible_month=date(2023, 1, 1),
                            end_date=date(2025, 1, 1),
                            style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}
                        ),
                        html.Div(id='output-date-picker-range')])
                ]
            )
        ]
        ),
        dbc.Row(
            # Graph to display trend
            dcc.Graph(id='trend-graph')
        )

    ])

    return layout



