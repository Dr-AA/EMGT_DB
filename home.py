from dash import html
from navbar import create_navbar

nav = create_navbar()

header = html.H4("Bienvenue sur l'interface Data de Energy Management")

contenu = html.H5("Choisissez une application dans le menu")

def create_page_home():
    layout = html.Div([
        nav,
        header,
        contenu,
    ],style={'marginTop': 10, 'marginBottom': 10, 'marginLeft': 10 , 'marginRight': 10})
    return layout

### >> TODO 1 : Gérer les formats de stockage différents avec les base S+T
### >> TODO 2 : Gérer la non prise en compte de la date de fin des data
### >> TODO 3 : Gérer la prise en compte des styles de conso