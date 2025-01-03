from dash import html, dcc
from dash.dependencies import Input, Output

import functions_cleaning
from home import create_page_home
from page_2 import create_page_2
from page_3 import create_page_3
from app import app
from sqlalchemy import create_engine, text
import urllib
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date
import Nettoyage as nettoyage
import functions_cleaning as cleaning


### ETABLISSEMENT DES CONNEXIONS ================================================================
#MSSQLServer.energymgt.ch\MSSQL_EMGT_I02
access_token = {'driver': 'ODBC Driver 18 for SQL Server',
                    'server': 'SRV-NTD-MSQL-01',
                    'user': 'EMGT_Access',
                    'pwd': '12-NRJ-28'}

constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
             f"{access_token['server']}/{"master"}?driver={access_token['driver']}&TrustServerCertificate=yes")

engine_general = create_engine(constring)

server = app.server
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-2':
        return create_page_2()
    if pathname == '/page-3':
        return create_page_3()
    else:
        return create_page_home()

#############################################################################
### GESTION DE L'INTERACTIVITE DE LA PAGE DE VISUALISATION DES DATA EN TREND

# INTERACTIVITE : LISTE DE TABLES EN FONCTION DE LA BASE CHOISIE
@app.callback(
    Output(component_id='table-dropdown', component_property='options'),
    Input(component_id='db-dropdown', component_property='value'))
def update_table_dropdowns(selected_db):
    if selected_db:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            requete = "SELECT * FROM information_schema.tables;"
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tables de la base ' + selected_db)
                # exit(1)
        liste_tables = df_out['TABLE_NAME'].to_list()

        options = [{'label': table, 'value': table} for table in liste_tables]
        return options
    return []

# INTERACTIVITE : LISTE DE TAGS EN FONCTION DE LA BASE CHOISIE ET DE LA TABLE
# TAG 1
@app.callback(
    Output(component_id='tag-dropdown', component_property='options'),
    Input(component_id='table-dropdown', component_property='value'),
    Input(component_id='db-dropdown', component_property='value'))
def update_tag_dropdowns(selected_table, selected_db):
    if selected_table:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            if selected_table == 'light_records':
                requete = "SELECT DISTINCT [key] FROM dbo." + selected_table
            else:
                requete = "SELECT DISTINCT tagName FROM dbo." + selected_table
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tag de la table' + selected_table)
                # exit(1)
        if selected_table == 'light_records':
            liste_tags = df_out['key'].to_list()
        else:
            liste_tags = df_out['tagName'].to_list()

        options = [{'label': tag, 'value': tag} for tag in liste_tags]
        return options
    return []

# INTERACTIVITE : LISTE DE TAGS EN FONCTION DE LA BASE CHOISIE ET DE LA TABLE
# TAG 2
@app.callback(
    Output(component_id='tag-dropdown-2', component_property='options'),
    Input(component_id='table-dropdown', component_property='value'),
    Input(component_id='db-dropdown', component_property='value'))
def update_tag_dropdowns(selected_table, selected_db):
    if selected_table:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            if selected_table == 'light_records':
                requete = "SELECT DISTINCT [key] FROM dbo." + selected_table
            else:
                requete = "SELECT DISTINCT tagName FROM dbo." + selected_table
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tag de la table' + selected_table)
                # exit(1)
        if selected_table == 'light_records':
            liste_tags = df_out['key'].to_list()
        else:
            liste_tags = df_out['tagName'].to_list()

        options = [{'label': tag, 'value': tag} for tag in liste_tags]
        return options
    return []

# INTERACTIVITE : CHARGEMENT DU GRAPHE AVEC LE TAG
@app.callback(
    Output(component_id='trend-graph', component_property='figure'),
    Input(component_id='table-dropdown', component_property='value'),
    Input(component_id='db-dropdown', component_property='value'),
    Input(component_id='tag-dropdown', component_property='value'),
    Input(component_id='tag-dropdown-2', component_property='value'),
    Input(component_id='date-picker-range', component_property= 'start_date'),
    Input(component_id='date-picker-range', component_property= 'end_date')
)
def update_graph(selected_table, selected_db, selected_tag, selected_tag_2, start_date, end_date):
    # On détermine la date du jour pour mettre un max sur l'historique des données
    year_today_1 = str(datetime.now().year - 1)

    # On cree/update le graphe
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)
    if selected_tag and selected_table and selected_db and selected_tag_2:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)

        # # on recupere les dates
        if start_date is not None and end_date is not None:
            start_date_object = date.fromisoformat(start_date)
            start_date_string = start_date_object.strftime('%d-%m-%Y %H:%M:%S')
            end_date_object = date.fromisoformat(end_date)
            end_date_string = end_date_object.strftime('%d-%m-%Y %H:%M:%S')
        #else:
           # start_date_string = '01-12-2024 00:00:00'
            #end_date_string = '12-12-2024 10:00:00'

        df_out = nettoyage.get_new_data_global(engine_general, selected_table,
                                            selected_tag, pd.to_datetime(start_date_string),
                                               pd.to_datetime(end_date_string))
        if not df_out.empty:
            print('valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)
            print(df_out.head())
        else:
            print('Pas de valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)

        df_out_2 = nettoyage.get_new_data_global(engine_general, selected_table,
                                              selected_tag_2, pd.to_datetime(start_date_string),
                                               pd.to_datetime(end_date_string))
        if not df_out_2.empty:
            print('valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)
            print(df_out_2.head())
        else:
            print('Pas de valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)


        # Add the first plot
        fig.add_trace(
            go.Scatter(x=df_out.index, y=df_out['tagValue'], mode='lines+markers', name=df_out['tagName'][0]),
            row=1, col=1)
        # Add the 2nd plot
        fig.add_trace(
            go.Scatter(x=df_out_2.index, y=df_out_2['tagValue'], mode='lines+markers', name=df_out_2['tagName'][0]),
            row=2, col=1)
        # fig = px.line(df_out, x=df_out.index, y=df_out['tagValue'], title=f'{selected_tag} vs Time')
        # Update layout for better appearance
        fig.update_layout(
            height=800,  # Set the height of the figure
            title="Tracés des Tags demandés",
            xaxis2=dict(title="Date et Heure"),  # X-axis title
            yaxis=dict(title=df_out['tagName'][0]),  # Y-axis title for the first plot
            yaxis2=dict(title=df_out_2['tagName'][0])  # Y-axis title for the second plot
        )
        return fig

    return px.line(title="Select Tags to display the trends.")

####
##################################################################################

#############################################################################
### GESTION DE L'INTERACTIVITE DE LA PAGE DE VISUALISATION DES DATA EN CONSO

# INTERACTIVITE : LISTE DE TABLES EN FONCTION DE LA BASE CHOISIE
@app.callback(
    Output(component_id='conso-table-dropdown', component_property='options'),
    Input(component_id='conso-db-dropdown', component_property='value'))
def update_table_dropdowns(selected_db):
    if selected_db:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            requete = "SELECT * FROM information_schema.tables;"
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tables de la base ' + selected_db)
                # exit(1)
        liste_tables = df_out['TABLE_NAME'].to_list()

        options = [{'label': table, 'value': table} for table in liste_tables]
        return options
    return []

# INTERACTIVITE : LISTE DE TAGS EN FONCTION DE LA BASE CHOISIE ET DE LA TABLE
# TAG 1
@app.callback(
    Output(component_id='conso-tag-dropdown', component_property='options'),
    Input(component_id='conso-table-dropdown', component_property='value'),
    Input(component_id='conso-db-dropdown', component_property='value'))
def update_tag_dropdowns(selected_table, selected_db):
    if selected_table:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            if selected_table == 'light_records':
                requete = "SELECT DISTINCT [key] FROM dbo." + selected_table
            else:
                requete = "SELECT DISTINCT tagName FROM dbo." + selected_table
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tag de la table' + selected_table)
                # exit(1)
        if selected_table == 'light_records':
            liste_tags = df_out['key'].to_list()
        else:
            liste_tags = df_out['tagName'].to_list()

        options = [{'label': tag, 'value': tag} for tag in liste_tags]
        return options
    return []

# INTERACTIVITE : CHARGEMENT DU GRAPHE AVEC LE TAG
@app.callback(
    Output(component_id='conso-graph', component_property='figure'),
    Input(component_id='conso-table-dropdown', component_property='value'),
    Input(component_id='conso-db-dropdown', component_property='value'),
    Input(component_id='conso-tag-dropdown', component_property='value'),
    Input(component_id='conso-date-picker-range', component_property= 'start_date'),
    Input(component_id='conso-date-picker-range', component_property= 'end_date')
)
def update_graph_conso(selected_table, selected_db, selected_tag, start_date, end_date):
    # On détermine la date du jour pour mettre un max sur l'historique des données
    year_today_1 = str(datetime.now().year - 1)

    # On cree/update le graphe
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)
    if selected_tag and selected_table and selected_db :
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)

        # # on recupere les dates
        if start_date is not None and end_date is not None:
            start_date_object = date.fromisoformat(start_date)
            start_date_string = start_date_object.strftime('%d-%m-%Y %H:%M:%S')
            end_date_object = date.fromisoformat(end_date)
            end_date_string = end_date_object.strftime('%d-%m-%Y %H:%M:%S')
        #else:
           # start_date_string = '01-12-2024 00:00:00'
            #end_date_string = '12-12-2024 10:00:00'

        df_out = nettoyage.get_new_data_global(engine_general, selected_table,
                                            selected_tag, pd.to_datetime(start_date_string),
                                               pd.to_datetime(end_date_string))
        if not df_out.empty:
            print('valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)
            print(df_out.head())

            # On vérifie que c'est un index
            if cleaning.is_index(df_out) == False:
                return px.line(title="LE TAG SELECTIONNE N'EST PAS UN INDEX DE COMPTEUR !")

        else:
            print('Pas de valeurs du tag ' + selected_tag + ' entre les dates ' +
                  start_date_string + ' et ' + end_date_string)

        # On calcule les consos à partir des données
        consos = functions_cleaning.consumption_from_index(df_out)


        # Add the first plot
        fig.add_trace(
            go.Scatter(x=df_out.index, y=df_out['tagValue'], mode='lines+markers', name=df_out['tagName'][0]),
            row=1, col=1)
        # fig = px.line(df_out, x=df_out.index, y=df_out['tagValue'], title=f'{selected_tag} vs Time')
        # Update layout for better appearance
        fig.update_layout(
            height=800,  # Set the height of the figure
            title="Tracés des Tags demandés",
            xaxis=dict(title="Date et Heure"),  # X-axis title
            yaxis=dict(title=df_out['tagName'][0]) # Y-axis title for the first plot
        )
        return fig

    return px.line(title="Select Tags to display the trends.")









#################################################################################
######## GESTION DES INTERACTIONS DE LA PAGE DE NETTOYAGE

# INTERACTIVITE : LISTE DE TABLES EN FONCTION DE LA BASE CHOISIE
@app.callback(
    Output(component_id='nettoyage-table-dropdown', component_property='options'),
    Input(component_id='nettoyage-db-dropdown', component_property='value'))
def update_table_dropdowns(selected_db):
    if selected_db:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            requete = "SELECT * FROM information_schema.tables;"
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tables de la base ' + selected_db)
                # exit(1)
        liste_tables = df_out['TABLE_NAME'].to_list()

        options = [{'label': table, 'value': table} for table in liste_tables]
        return options
    return []

# INTERACTIVITE : LISTE DE TAGS EN FONCTION DE LA BASE CHOISIE ET DE LA TABLE
# TAG 1
@app.callback(
    Output(component_id='nettoyage-tag-dropdown', component_property='options'),
    Input(component_id='nettoyage-table-dropdown', component_property='value'),
    Input(component_id='nettoyage-db-dropdown', component_property='value'))
def update_tag_dropdowns(selected_table, selected_db):
    if selected_table:
        constring = (f"mssql+pyodbc://{access_token['user']}:{access_token['pwd']}@"
                     f"{access_token['server']}/{selected_db}?driver={access_token['driver']}&TrustServerCertificate=yes")

        engine_general = create_engine(constring)
        with engine_general.connect() as con:
            requete = "SELECT DISTINCT tagName FROM dbo." + selected_table
            try:
                df_out = pd.read_sql_query(requete, con)
            except:
                print('Erreur pour acceder aux tag de la table' + selected_table)
                # exit(1)
        liste_tags = df_out['tagName'].to_list()

        options = [{'label': tag, 'value': tag} for tag in liste_tags]
        return options
    return []




if __name__ == '__main__':
    app.run_server(debug=False)