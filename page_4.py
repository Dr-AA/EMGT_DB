from dash import html, dcc
from navbar import create_navbar
from sqlalchemy import create_engine
import urllib
import pandas as pd
import dash_bootstrap_components as dbc
from datetime import date

nav = create_navbar()

header = html.H4("Fonctionnalité de nettoyage des données de compteurs ")

### ETABLISSEMENT DES CONNEXIONS ================================================================
#MSSQLServer.energymgt.ch\MSSQL_EMGT_I02
#### EXTRACTION DES USERS POSSIBLES
access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                    'server': 'SERV-OVH04-LDB.energymgt.ch\EMGT',
                    'user': 'EMGT_Access',
                    'pwd': '12-NRJ-28',
                    'database':'TEST_AAU_PROG'}

quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' +
                                         access_token['server'] + ';DATABASE=' + access_token['database'] +
                                         ';UID=' + access_token['user'] + ';PWD=' + access_token['pwd'])
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
with engine.connect() as con:
    requete = "SELECT * FROM dbo.Users"
    try:
        df_out = pd.read_sql_query(requete, con)
    except:
        print('Erreur pour acceder aux tag de la table')
        #exit(1)
liste_users = df_out['email'].to_list()

#### EXTRACTION DES NOMS DES PROJETS
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

def create_page_4():
    layout = html.Div([
        # Barre de navigation
        dbc.Row(nav),
        dbc.Row([
            dbc.Col(
                [
                    html.Div(header,style={'marginTop': 10,'marginLeft':15,'marginBottom':10}),

                    # Dropdowns for columns (populated dynamically)
                    html.Label("1 - Indiquez votre adresse email de Chef de Projet",
                               style={'width': "60%",'marginTop': 10,'marginLeft':15,'marginBotttom':10}),
                    dcc.Dropdown(id='nettoyage-user-dropdown', options=liste_users, placeholder="Choose a user",
                                 style={'width': "60%",'marginTop': 10,'marginLeft':15,'marginBotttom':10}),

                    # Dropdowns for columns (populated dynamically)
                    html.Label("2 - Choisissez le Projet",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='nettoyage-db-dropdown', options=liste_db, placeholder="Choose a project",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),

                    # Dropdowns for columns (populated dynamically)
                    html.Label("3 - Choisissez la Table ou est stocké le tag",
                               style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),
                    dcc.Dropdown(id='nettoyage-table-dropdown', placeholder="Choose a project",
                                 style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10})]),
            dbc.Col(
            [
                # Dropdowns for columns (populated dynamically)
                html.Label("4 - Choisissez le Tag/Index à nettoyer: ",
                       style={'width': "60%", 'marginTop': 30, 'marginLeft': 15, 'marginBotttom': 10}),
                dcc.Dropdown(id='nettoyage-tag-dropdown', placeholder="Choose a project",
                         style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}),

                html.Label("5 - Choisissez le range des dates à nettoyer: ",
                           style={'width': "60%", 'marginTop': 30, 'marginLeft': 15, 'marginBotttom': 10}),
                html.Div([
                    dcc.DatePickerRange(
                        id='nettoyage-date-picker',
                        min_date_allowed=date(2015, 1, 1),
                        max_date_allowed=date(2030, 1, 1),
                        initial_visible_month=date(2023, 1, 1),
                        end_date=date(2025, 1, 1),
                        style={'width': "60%", 'marginTop': 10, 'marginLeft': 15, 'marginBotttom': 10}
                    ),
                    html.Div(id='nettoyage-output-date-picker')])
            ])]
        )])
    return layout