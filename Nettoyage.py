import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from datetime import datetime
import functions_cleaning as cleaning
import logging
import os

def load_sql(df, engine, nom_table):
    '''Charge un dataframe dans table et base MS SQL Server
    Au format utilisé par EMGT
    -le dataframe doit avoir les col suivantes:
    ts : type datetime
    tagName : type str
    tagValue : type str
    quality : type int

    -engine : moteur de connexion a DB
    quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+
                                 access_token['server']+';DATABASE='+access_token['database']+
                                 ';UID='+access_token['user']+';PWD='+ access_token['pwd'])
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    - nom_table : nom de la table dans laquelle ecrire'''

    # On verifie d'abord que la table existe pour ne pas en creer une nouvelle si non
    requete = "SELECT * FROM information_schema.tables;"
    #requete = "SELECT TOP(100) * FROM dbo.Donnees_Brutes;"
    with engine.connect() as con:
        df_out = pd.read_sql_query(requete,con)
        if (not nom_table in df_out['TABLE_NAME'].to_list()) and (not nom_table.replace('dbo.','') in df_out['TABLE_NAME'].to_list()):
            print('Table ' + nom_table + ' non trouvée dans la base')
            return 'Table ' + nom_table + ' non trouvée dans la base'

    try:
        df.to_sql(nom_table, con=engine, if_exists='append', index=False, chunksize=10000)
        return None
    except:
        print('Erreur en écriture de données dans la table ' + nom_table)
        return 'Erreur en écriture de données dans la table ' + nom_table

def check_new_data(engine,tagName):
    """Check si nouvelles données presente pour un tag bien défini
    La base vérifiée doit contenir une table de données brutes et données propre
    -Si nouvelle donnees brute pas dans propre > renvoie la derniere date propre
    - Si pas de nouvelle données, renvoie None"""

    #Lit la date derniere valeur brute
    #   si pas de valeur > None (pas de nouvelles données)
    #   sinon, lit la derniere valeur brute
    #Lit la date derniere valeur propre
    #   si pas de valeur > date renvoyée 1970
    #   sinon lit la date derniere valeur propre
    #        si derniere date brute > derniere date propre > nouvelle data > date derniere propre renvoyée
    #        sinon >> None >> pas de nouvelle données

    # Nom des tables dans la base
    table_brut = "Donnees_Brutes"
    table_propre = "Donnees_Propres"

    with engine.connect() as con:
        requete = "SELECT TOP (1) ts FROM dbo." + table_brut + " WHERE tagName = " +\
                  "'" + tagName + "'" + " ORDER BY ts DESC"
        try:
            df_out = pd.read_sql_query(requete, con)
        except:
            print('Erreur pour acceder a la derniere valeur brute du tag ' + tagName)
            exit(1)

        if df_out.empty:
            return None
        else:
            last_time_brut = df_out['ts'][0]

        requete = "SELECT TOP (1) ts FROM dbo." + table_propre + " WHERE tagName = " + \
                  "'" + tagName + "'" + " ORDER BY ts DESC"
        try:
            df_out = pd.read_sql_query(requete, con)
        except:
            print('Erreur pour acceder a la derniere valeur propre du tag ' + tagName)
            exit(1)

        if df_out.empty:
            return pd.to_datetime('1970-01-01 00:00:00')
        else:
            last_time_propre = df_out['ts'][0]

            if last_time_brut > last_time_propre:
                return last_time_propre
            else:
                return None

def limit_dates(engine,tagName,table):
    """Renvoie les dates limites des records d'un tag dans une table
    """
    con = engine.connect()
    requete = "SELECT TOP (1) ts FROM dbo." + table + " WHERE tagName = " +\
             "'" + tagName + "'" + " ORDER BY ts DESC"

    try:
        df_out = pd.read_sql_query(requete, con)
    except:
        print('Erreur pour acceder a la derniere date du tag ' + tagName)
        exit(1)

    if df_out.empty:
        return None, None
    else:
        last_date = df_out['ts'][0]

    requete = "SELECT TOP (1) ts FROM dbo." + table + " WHERE tagName = " + \
              "'" + tagName + "'" + " ORDER BY ts ASC"
    try:
        df_out = pd.read_sql_query(requete, con)
    except:
        print('Erreur pour acceder a la premiere date tag ' + tagName)
        exit(1)

    first_date = df_out['ts'][0]
    con.close()
    return first_date, last_date

def get_new_data(engine, tagName, last_date):
    '''Renvoie un dataframe avec les données du tag demandé
    aux dates supererieures a la date en entrée'''

    ## ATTENTION : INSTAL DE MS SQL SERVER EN FRANCAIS, DONC FORMAT DATE = DD-MM-YYYY !!!
    last_date_str = last_date.strftime('%d-%m-%Y %H:%M:%S')
    with engine.connect() as con:
        requete = "SELECT * FROM dbo.Donnees_Brutes WHERE tagName = " +\
                  "'" + tagName + "'" + " AND ts > '" + last_date_str + "' ORDER BY ts ASC"
        try:
            df_out = pd.read_sql_query(requete, con)
        except:
            print('Erreur pour acceder aux nouvelles valeur du tag ' + tagName)
            exit(1)

        if df_out.empty:
            return None
        else:
            # La requete a fonctionne, on met le dataframe dans le bon format pr travailler en python
            df_out = df_out.set_index('ts')
            df_out.index = pd.to_datetime(df_out.index)
            df_out['tagValue'] = df_out['tagValue'].astype(float)

            return df_out

def get_new_data_gen(engine, table, tagName, last_date):
    '''Renvoie un dataframe avec les données du tag demandé
    aux dates supererieures a la date en entrée'''
    if 'dbo.' not in table:
        table = 'dbo.' + table

    ## ATTENTION : INSTAL DE MS SQL SERVER EN FRANCAIS, DONC FORMAT DATE = DD-MM-YYYY !!!
    last_date_str = last_date.strftime('%d-%m-%Y %H:%M:%S')
    with engine.connect() as con:
        requete = "SELECT * FROM " + table + " WHERE tagName = " +\
                  "'" + tagName + "'" + " AND ts > '" + last_date_str + "' ORDER BY ts ASC"
        try:
            df_out = pd.read_sql_query(requete, con)
        except:
            print('Erreur pour acceder aux nouvelles valeur du tag ' + tagName)
            exit(1)

        if df_out.empty:
            return None
        else:
            # La requete a fonctionne, on met le dataframe dans le bon format pr travailler en python
            print(df_out.head())
            df_out = df_out.set_index('ts')
            df_out.index = pd.to_datetime(df_out.index)
            # On vire les valeurs vides ou booleene non numeriques
            df_out = df_out[(df_out['tagValue'] != '') & (df_out['tagValue'] != 'null') &
                            (df_out['tagValue'] != 'false') & (df_out['tagValue'] != 'False') &
                            (df_out['tagValue'] != 'True') & (df_out['tagValue'] != 'true') &
                            (df_out['tagValue'] != '-') & (df_out['tagValue'] != 'undefined')]
            df_out['tagValue'] = df_out['tagValue'].astype(float)

            return df_out

def plot_nettoyage(liste_df, dir_write):

    df_brut = liste_df[0]
    df_0 = liste_df[1]
    df_saut = liste_df[2]
    df_clean = liste_df[3]

    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12,7))
    ax1.scatter(df_brut.index, df_brut['tagValue'], s=50, marker='o',
                color='black', label='Données Brutes')
    if not df_0.empty:
        ax1.scatter(df_0.index, df_0['tagValue'], s=50, marker='o',
                color='red', label='Données Ecartées - 0')
    if not df_saut.empty:
        ax1.scatter(df_saut.index, df_saut['tagValue'], s=50, marker='o',
                color='orange', label='Données Ecartées - Sauts Index')
    ax2.scatter(df_clean.index, df_clean['tagValue'], s=50, marker='o',
                color='green', label='Données Propres Chargées')

    plt.gca().yaxis.set_major_locator(MaxNLocator(nbins=8))
    plt.gca().xaxis.set_major_locator(MaxNLocator(nbins=12))
    ax1.legend(fontsize=9)
    ax2.legend(fontsize=9)
    # plt.show()
    plt.suptitle("Tag : " + df_brut['tagName'][0] + ' \n\nDates du ' +
                 str(df_brut.index[0]) + ' au ' + str(df_brut.index[-1]))
    plt.savefig(dir_write + '\\' + df_brut['tagName'][0] + '.png')
    plt.close()

    return dir_write + '\\' + df_brut['tagName'][0] + '.png'
#
def send_email(df_email):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

    # On envoie un email par base de données (=projet)
    sortie = []
    for base in list(df_email['db_name'].unique()):

        # Email configuration for Gmail
        smtp_username = 'emgt.energy'
        smtp_password = 'afye cest qcwu iyrr'
        sender = smtp_username
        recipient = df_email.loc[df_email['db_name']== base ]['email'].values[0]

        # Create the email
        msg = MIMEMultipart('related')
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = 'Nettoyage Automatique ' + base

        # On boucle sur les tags/images a mettre dans l'email
        image_paths = []
        image_cids = []
        liste_balise = []
        # On prepare d'abord les balises des images a inserer
        j = 1
        for i, row in df_email.loc[df_email['db_name'] == base].iterrows():
            image_paths.append(row['img_path'])
            liste_balise.append(('<img src="cid:image' + str(j) + '"' + ' alt="Image ' + str(j) + '">'))
            image_cids.append(('image'+str(j)))
            j += 1

        #code html
        html_content = '''
            <html>
            <body>
                <h1>Rapport de Nettoyage pour la base ''' + base + '''</h1>
                <p>Visualisation des data: </p>
                '''
        for bal in liste_balise:
            html_content += bal

        html_content += '''</body></html>'''

        # Ajout du contenu HTML au message
        msg.attach(MIMEText(html_content, 'html'))

        for i, image_path in enumerate(image_paths):
            with open(image_path, 'rb') as img_file:
                img_data = img_file.read()
                img = MIMEImage(img_data, name=image_path)
                img.add_header('Content-ID', f'<{image_cids[i]}>')
                msg.attach(img)

        # Send the email
        try:
            with smtplib.SMTP_SSL('smtp.gmail.com',465) as server:
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, msg.as_string())
                sortie.append(None)
        except Exception as e:
            sortie.append(e)

    return sortie

def PREPARE_TESTS():


    LOAD_TEST_DATA = False
    if LOAD_TEST_DATA:

        dir_data = 'C:\\EXTRACTIONS\\JTI_DATA\\'

        access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                        'server': "SERV-OVH04-LDB.energymgt.ch\EMGT",
                        'user': "EMGT_Access",
                        'pwd': "12-NRJ-28",
                        'database': "TEST_AAU",
                        'table': "dbo.Donnees_Brutes"}

        quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' +
                                         access_token['server'] + ';DATABASE=' + access_token['database'] +
                                         ';UID=' + access_token['user'] + ';PWD=' + access_token['pwd'])
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

        df_brut = pd.read_csv(dir_data + 'Test_JTI.csv')
        df_brut = df_brut.drop(['quality'], axis=1, errors='ignore')
        df_brut['tagName'] = 'test_jti_2'
        df_brut['quality'] = 1

        #  Forcing types
        df_brut['ts'] = pd.to_datetime(df_brut['ts'])
        df_brut['tagValue'] = df_brut['tagValue'].astype(str)
        df_brut['tagName'] = df_brut['tagName'].astype(str)
        df_brut['quality'] = df_brut['quality'].astype(int)

        # Ordering columns
        df_brut = df_brut[['ts', 'tagName', 'tagValue', 'quality']]

        df1 = df_brut.loc[df_brut['ts'] < '2022-08-15']

        # On charge des données dans la base brute
        error_load = load_sql(df1, engine, 'Donnees_Brutes')
        if error_load is not None:
            print(error_load)
            exit(1)

    LOAD_DEF_TABLE = True
    if LOAD_DEF_TABLE:
        access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                        'server': "SERV-OVH04-LDB.energymgt.ch\EMGT",
                        'user': "EMGT_Access",
                        'pwd': "12-NRJ-28",
                        'database': "TEST_AAU_PROG",
                        'table': "dbo.Nettoyage"}

        quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' +
                                         access_token['server'] + ';DATABASE=' + access_token['database'] +
                                         ';UID=' + access_token['user'] + ';PWD=' + access_token['pwd'])
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        # Creation d'une table relationnel pour les taches de nettoyage
        df_def = pd.read_csv(dir_data + 'data.txt')

        df_def.to_sql('Nettoyage', con=engine, if_exists='append', index=False)

        print('toto')


def to_utc(df):

    df.index = df.index.tz_localize('Europe/Paris', ambiguous='NaT')
    df.index = df.index.tz_convert('UTC')
    print('toto')



def main():

    logging.basicConfig(filename='OUTPUT_NETTOYAGE\\NETTOYAGE.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')



    dir_data = 'C:\\EXTRACTIONS\\JTI_DATA\\'

    ##==== Definition Connexion Base de reference
    access_token_clean = {'driver': 'ODBC Driver 13 for SQL Server',
                    'server': "SERV-OVH04-LDB.energymgt.ch\EMGT",
                    'user': "EMGT_Access",
                    'pwd': "12-NRJ-28",
                    'database': "TEST_AAU_PROG",
                    'table': "dbo.Nettoyage"}

    quoted_clean = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+
                                     access_token_clean['server']+';DATABASE='+access_token_clean['database']+
                                     ';UID='+access_token_clean['user']+';PWD='+ access_token_clean['pwd'])

    engine_clean = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_clean))

    logging.info('=== LANCEMENT NETTOYAGE SUR SERVEUR ' + access_token_clean['server'] + '====')

    # On récupère les infos sur les bases à nettoyer
    logging.info('RECUPERATION DES TAGS A NETTOYER')
    with engine_clean.connect() as con:
        requete = "SELECT * FROM dbo.Nettoyage"
        try:
            df_infos = pd.read_sql_query(requete, con)
        except:
            print('Erreur pour récupérer les taches de nettoyage')
            logging.error('ERREUR RECUPERATION DES TAGS A NETTOYER')
            exit(1)

    ##==== Definition Connexion Serveur
    access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                          'server': "SERV-OVH04-LDB.energymgt.ch\EMGT",
                          'user': "EMGT_Access",
                          'pwd': "12-NRJ-28"}

    # On cree un dossier de sortie a date du jour
    # On cree un dossier d'extraction a date du jour
    new_dir_jour = 'OUTPUT_NETTOYAGE\\' + str(datetime.now().year) + '_' + \
              str(datetime.now().month) + '_' + str(datetime.now().day) + '_' + str(datetime.now().hour) + \
              '_' + str(datetime.now().minute) + '_' + str(datetime.now().second)
    os.mkdir(new_dir_jour)

    # On cree une liste vide pour stocker les infos
    df_email = pd.DataFrame(columns=['db_name','tagName','email','img_path'])
    # BOUCLAGE SUR LES DIFFERENTES TAGS A NETTOYER DANS LEUR BASE RESPECTIVE
    for i,row in df_infos.iterrows():

        database = row['db_name']
        tag_name = row['tagName']
        adresse_email = row['email']

        # Creation sous-dossier pour cette database
        if not os.path.isdir(new_dir_jour + '\\' + database):
            os.mkdir(new_dir_jour + '\\' + database)

        logging.info('**** Traitement Base ' + database + ' Tag ' + tag_name + ' User ' + adresse_email + '****')

        # Etablissement de la connexion
        quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' +
                                         access_token['server'] + ';DATABASE=' + database +
                                         ';UID=' + access_token['user'] + ';PWD=' + access_token['pwd'])
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

        # On verifie si de nouvelles valeurs sont arrivées en base brute
        logging.info('Check si nouvelles dates')
        derniere_date_propre = check_new_data(engine,tag_name)

        # Si nouvelles valeurs en base brut, on lance un nettoyage et le transfert

        if derniere_date_propre is not None:
            logging.info('Nouvelles dates Brutes après ' + str(derniere_date_propre))
            # dans ce cas on doit prendre les valeur brutes depuis cette derniere date propre
            df = get_new_data(engine, tag_name, derniere_date_propre)

            #===============================
            # DEBUT DES FONCTIONS DE NETTOYAGE
            #=================================

            #>>>> Nettoyage des 0
            try:
                df_0, df_0_clean = cleaning.clean_zeroes(df,'tagValue')
            except:
                logging.critical('Erreur nettoyage 0')
                exit(1)

            # >>>> Nettoyage des Saut d'Index
            try:
                df_saut, df_saut_clean = cleaning.clean_sauts(df_0_clean, 'tagValue')
            except:
                logging.critical('Erreur nettoyage Sauts Index')
                exit(1)

            df_propre = df_saut_clean

            # >>>> Nettoyage des Index Bloqués
            try:
                df_ind, df_ind_clean = cleaning.clean_stuck_index(df_saut_clean, 'tagValue')
            except:
                logging.critical('Erreur nettoyage Sauts Index')
                exit(1)

            # ===============================
            # FIN DES FONCTIONS DE NETTOYAGE
            # =================================

            # ===============================
            # TRANSFERT VERS BASE PROPRE
            # =================================

            # On met le dataframe au bon format
            df_propre['ts'] = pd.to_datetime(df_propre.index)
            df_propre['tagValue'] = df_propre['tagValue'].astype(str)
            df_propre['tagName'] = df_propre['tagName'].astype(str)
            df_propre['quality'] = df_propre['quality'].astype(int)

            logging.info('Chargement nouvelles données Base Propre')
            error_load = load_sql(df_propre, engine, 'Donnees_Propres')
            if error_load is not None:
                print(error_load)
                logging.critical(error_load)
                exit(1)
            else:
                logging.info('Fin Chargement nouvelles données Base Propre Tag ' + tag_name)

            ## TRACE DES NETTOYAGE
            img = plot_nettoyage([df, df_0, df_saut, df_saut_clean], new_dir_jour + '\\' + database)

            df_email = pd.concat([df_email, pd.DataFrame([{'db_name':database, 'tagName':tag_name,
                                        'email':adresse_email, 'img_path':img}])])

        else:
            logging.info('Pas de nouvelles dates pour Tag ' + tag_name)

    error = send_email(df_email)
    logging.info('=== BASE ' + ' FIN PROGRAMME NETTOYAGE ====')




##>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#>>>>>> Exécution Principale >>
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#main()
#PREPARE_TESTS()
#test_email()