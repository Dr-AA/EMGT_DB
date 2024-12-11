from dash import html
from navbar import create_navbar

nav = create_navbar()

header = html.H4("Bienvenue sur l'interface Data de Energy Management")


def create_page_home():
    layout = html.Div([
        nav,
        header,
    ],style={'marginTop': 10})
    return layout