from navbar import create_navbar
from dash import dcc, html
import pandas as pd
from sqlalchemy import create_engine
import urllib

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

### RENVOI DE LA PAGE TOTALE
def create_page_2():
    layout = html.Div([

        nav,

        html.Div(html.H4("Visualisation des données"),style={'marginTop': 15,'marginLeft':15}),

        # Dropdown to select table
        html.Label("Choix du Projet:",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),
        dcc.Dropdown(
            id='db-dropdown',
            options=[{'label': db_name, 'value': db_name} for db_name in liste_db],
            placeholder="Choose a database",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}
        ),

        # Dropdowns for columns (populated dynamically)
        html.Label("Choix de la Table de Données:",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),
        dcc.Dropdown(id='table-dropdown', placeholder="Choose a table",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),

        html.Label("Choix du Tag:",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),
        dcc.Dropdown(id='tag-dropdown', placeholder="Choose a tag",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),

        html.Label("Choix d'un 2è Tag:",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),
        dcc.Dropdown(id='tag-dropdown-2', placeholder="Choose a 2nd tag",
                   style={'width': "60%",'marginTop': 15,'marginLeft':15,'marginBotttom':15}),

        # Graph to display trend
        dcc.Graph(id='trend-graph')

    ],style={'marginTop': 10})



    return layout



