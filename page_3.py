from navbar import create_navbar
from dash import dcc, html
import pandas as pd
from sqlalchemy import create_engine
import urllib
import dash_bootstrap_components as dbc
from datetime import date

### ETABLISSEMENT DES CONNEXIONS ================================================================
#MSSQLServer.energymgt.ch\MSSQL_EMGT_I02
access_token = {'driver': 'ODBC Driver 18 for SQL Server',
                    'server': 'SRV-NTD-MSQL-01',
                    'user': 'EMGT_Access',
                    'pwd': '12-NRJ-28'}

constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
             f"{access_token['server']}/{"master"}?driver={access_token['driver']}&TrustServerCertificate=yes")

engine_general = create_engine(constring)
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

header = html.H4("Visualisation de données de consommation ")

### RENVOI DE LA PAGE TOTALE
def create_page_3():
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
                    dcc.Dropdown(id='conso-db-dropdown', options=liste_db, placeholder="Choose a project",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),

                    # Dropdowns for columns (populated dynamically)
                    html.Label("2 - Choisissez la Table ou est stocké le tag",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='conso-table-dropdown', placeholder="Choose a table",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10})]),
            dbc.Col(
                [
                    # Dropdowns for columns (populated dynamically)
                    html.Label("3 - Choisissez le Tag à visualiser: ",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='conso-tag-dropdown', placeholder="Choose a Tag",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10})
                ]),

            dbc.Col(
                [
                    html.Label("5 - Définir les dates à visualiser: ",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    html.Div([
                        dcc.DatePickerRange(
                            id='conso-date-picker-range',
                            min_date_allowed=date(2015, 1, 1),
                            max_date_allowed=date(2030, 1, 1),
                            initial_visible_month=date(2024, 12, 1),
                            end_date=date(2025, 1, 1),
                            style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}
                        ),
                        ])
                ]
            )
        ]
        ),
        dbc.Row([
            dcc.Graph(id='conso-graph'),
            dcc.RadioItems(
                ['Horaire', 'Journalier', 'Mensuel', 'Annuel'],
                'Journalière', id='yaxis-type', inline=True,
                style = {
                    'fontSize': '20px',  # Style for the entire component
                    'color': 'black',
                    'justify': 'center'
                },
                inputStyle = {
                        'margin-right': '15px',  # Style for the input (radio button)
                        'margin-left': '15px',
                        'margin-top': '10px',
                        'margin-bottom': '10px'
                }
            )
        ])

    ])

    return layout



