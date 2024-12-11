import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import *
import pandas as pd
from sqlalchemy import create_engine
import sys
import os
import pyodbc
import urllib

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

# Create the main application window
root = tk.Tk()
root.title('THE MONSIEUR PROPRE OF ENERGY MANAGEMENT')

img_dir = resource_path("img")

img = PhotoImage(file=img_dir + "\\EM_Logo.png")
canvas1 = tk.Canvas(root, width=900, height=500, relief='raised')
canvas1.create_image(192, 150, image = img)
canvas1.pack()

col_pos = 550

# Global variable to store file path
selected_file = None


# Function to select and load the file
def load_file():
    global selected_file
    selected_file = filedialog.askopenfilename(
        title="Select a file",
        filetypes=(("All files", "*.*"),("Excel files", "*.xlsx"), ("CSV files", "*.csv"),("txt files", "*.txt"))
    )
    if selected_file:
        file_label.config(text=f"Loaded File: {selected_file}")
    else:
        messagebox.showinfo("File Load", "No file selected")

# Function to load the selected file's data into the SQL Server
def load_data_to_sql():

    def check_tag_exist(access_token,db_name,tag_name):

        table_brut = "Donnees_Brutes"
        table_brut_2 = "Donnees_Propres"


        connection_string = 'DRIVER={' + access_token['driver'] + '};SERVER=' + access_token['server'] + \
                            ';DATABASE=' + db_name + ';UID=' + access_token['user'] + \
                            ';PWD=' + access_token['pwd'] + ";"
        try:
            conn = pyodbc.connect(connection_string)
        except:
            messagebox.showinfo("ERREUR", "Connexion à la base impossible, vérifiez login et mot de passe")
        cursor = conn.cursor()

        requete = "SELECT DISTINCT tagName FROM dbo." + table_brut + ";"
        requete_bis = "SELECT DISTINCT tagName FROM dbo." + table_brut_2 + ";"

        try:
            cursor.execute(requete)
        except:
            try:
                cursor.execute(requete_bis)
            except:
                messagebox.showerror("Error", "Impossible de vérifier le Tag " + tag_name + " en base " + db_name)

        df_tags = pd.DataFrame.from_records(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        liste_tags = list(df_tags['tagName'])
        conn.close()

        if tag_name in liste_tags:
            return True
        else:
            return False

    def send_conf_email(df):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.image import MIMEImage
        # Email configuration for Gmail
        smtp_username = 'emgt.energy'
        smtp_password = 'afye cest qcwu iyrr'
        sender = smtp_username
        recipient = df['email'][0]

        body = 'Bases et Tags ajoutés au système de nettoyage :\n\n'
        for i, row in df.iterrows():
            body += 'Base ' + row['db_name'] + ' et Tag ' + row['tagName'] + '\n\n'

        # Create the email
        msg = MIMEText(body, "plain")
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = 'Confirmation Abonnement Nettoyage Automatique '

        # Send the email
        try:
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, msg.as_string())

        except Exception as e:
            messagebox.showerror("Error", "Problem with confirmation email")

    global selected_file
    if not selected_file:
        messagebox.showerror("Error", "No file loaded! Please load a file first.")
        return

    # Reading the file into a DataFrame
    if selected_file.endswith('.xlsx'):
        df = pd.read_excel(selected_file)
    elif selected_file.endswith('.csv'):
        df = pd.read_csv(selected_file)
    elif selected_file.endswith('.txt'):
        df = pd.read_csv(selected_file)
    else:
        messagebox.showerror("Error", "Unsupported file format. Please load an Excel or CSV file.")
        return

    ##==== Definition Connexion Base de reference
    access_token = {'driver': 'ODBC Driver 13 for SQL Server',
                    'server': server_menu.get(),
                    'user': entry_user.get(),
                    'pwd': entry_pwd.get()}

    connection_string = 'DRIVER={' + access_token['driver'] + '};SERVER=' + access_token['server'] + \
                        ';UID=' + access_token['user'] + \
                        ';PWD=' + access_token['pwd'] + ";"
    try:
        conn = pyodbc.connect(connection_string)
    except:
        messagebox.showinfo("ERREUR", "Connexion au serveur impossible, vérifiez login et mot de passe")

    # On liste les bases dispos sur ce serveur
    cursor = conn.cursor()
    requete = "SELECT name FROM sys.databases;"
    cursor.execute(requete)
    df_db = pd.DataFrame.from_records(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    liste_db = df_db['name'].to_list()

    # On liste les emails dispos sur le serveur
    cursor = conn.cursor()

    requete = "SELECT DISTINCT email FROM TEST_AAU_PROG.dbo.Users;"
    cursor.execute(requete)
    df_email = pd.DataFrame.from_records(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    liste_email = df_email['email'].to_list()

    # On traite les données
    # On verifie que on a bien les bons noms de colonne
    if df.columns.to_list() != ['db_name', 'tagName', 'conso_perm', 'email']:
        messagebox.showinfo("ERREUR", "Vérifiez les noms de colonne (voir manuel) ")

    for i, row in df.iterrows():
        if row['db_name'] not in liste_db:
            messagebox.showinfo("ERREUR", "Base " + row['db_name'] + ' non existante sur ce serveur')
        if row['email'] not in liste_email:
            messagebox.showinfo("ERREUR", "Email " + row['email'] + ' non référencé')
        if row['conso_perm'] != 0 and row['conso_perm'] != 1:
            messagebox.showinfo("ERREUR", "Tag " + row['tagName'] + ' manque définition conso')

        # On check si le tag existe
        tag_exist = check_tag_exist(access_token,row['db_name'],row['tagName'])

        if not tag_exist:
            messagebox.showinfo("ERREUR", "Tag " + row['tagName'] + ' non référencé')

        # On insert la ligne dans la table si elle n'existe pas deja
        requete = "BEGIN IF NOT EXISTS (SELECT * FROM TEST_AAU_PROG.dbo.Nettoyage "
        requete += "WHERE db_name = " + "'" + row['db_name'] + "'" + " AND tagName = " + "'" + row['tagName'] + "'"
        requete += " AND conso_perm = " + "'" + str(row['conso_perm']) + "'"" AND email = " + "'" + row['email'] + "'" + ")"
        requete += " BEGIN INSERT INTO TEST_AAU_PROG.dbo.Nettoyage (db_name,tagName,conso_perm,email)"
        requete += " VALUES (" + "'" + row['db_name'] + "'" + "," + "'" +\
                   row['tagName'] + "'" + "," + "'" + str(row['conso_perm']) + "'" + "," + "'" + row['email'] + "'" + ")"
        requete += " END END"

        try:
            cursor.execute(requete)
            conn.commit()
            print('toto')
        except:
            messagebox.showinfo("ERREUR", "Chargement impossible de la ligne " + str(i))
            print('toto')

    cursor.close()
    send_conf_email(df)
    messagebox.showinfo("Success", "Data loaded successfully into SQL Server!\n Confirmation email will be sent")

options_server = [
    "MSSQLServer.energymgt.ch\MSSQL_EMGT_I02",
    "MSSQLServer.energymgt.ch\MSSQL_EMGT_E02",
    "SERV-OVH04-LDB.energymgt.ch\EMGT",
    "SRV-NTD-MSQL-01"]

label_server = tk.Label(root, text='Serveur de données de votre projet ?')
label_server.config(font=('helvetica', 12), fg='medium blue')
canvas1.create_window(col_pos, 70, window=label_server)

server_menu = ttk.Combobox(root, width=50, value= (options_server))
canvas1.create_window(col_pos, 90, window=server_menu)

# INPUT DES LOGIN ET MOT DE PASSE
label_user = tk.Label(root, text='Login:')
label_user.config(font=('helvetica', 12), fg='medium blue')
canvas1.create_window(col_pos, 140, window=label_user)
entry_user = tk.Entry(root)
entry_user.insert(0,'EMGT_Access')
canvas1.create_window(col_pos, 160, window=entry_user)

label_pwd = tk.Label(root, text='Mot de passe:')
label_pwd.config(font=('helvetica', 12), fg='medium blue')
canvas1.create_window(col_pos, 190, window=label_pwd)
entry_pwd = tk.Entry(root)
entry_pwd.insert(0,'')
canvas1.create_window(col_pos, 210, window=entry_pwd)

# Create a button to load the file
load_button = tk.Button(root, text="Charge le fichier pour Monsieur Propre", command=load_file,
                        bg='medium blue', fg='white', font=('helvetica', 11, 'bold'))
canvas1.create_window(col_pos, 280, window=load_button)

# Create a label to show the loaded file path
file_label = tk.Label(root, text="No file loaded", wraplength=450)
canvas1.create_window(col_pos, 320, window=file_label)

# Create a button to load the data into SQL Server
load_data_button = tk.Button(root, text="Inscrire les tags", command=load_data_to_sql,
                             bg='medium blue', fg='white', font=('helvetica', 11, 'bold'))
canvas1.create_window(col_pos, 400, window=load_data_button)

# Start the tkinter event loop
root.mainloop()