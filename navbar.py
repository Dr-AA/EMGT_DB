import dash_bootstrap_components as dbc
from dash import dcc, html

def create_navbar():
    navbar = html.Div([
        dbc.Row([
            dbc.Col(
                html.Img(src=r'assets/Logo-Noir-V2.png', alt='image',width='200px'),width=2
            ),
            dbc.Col(
                dbc.NavbarSimple(
                children=[
                    dbc.DropdownMenu(
                        nav=True,
                        in_navbar=True,
                        label="Menu",
                        children=[
                            dbc.DropdownMenuItem("Home", href='/'),
                            dbc.DropdownMenuItem(divider=True),
                            dbc.DropdownMenuItem("Visualiser des données", href='/page-2'),
                            dbc.DropdownMenuItem("Nettoyer des données", href='/page-3'),
                        ],
                    ),
                ],
                brand="Home",
                brand_href="/",
                sticky="top",
                color="dark",  # Change this to change color of the navbar e.g. "primary", "secondary" etc.
                dark=True,  # Change this to change color of text within the navbar (False for dark text)
                )
            )
        ])

    ])
    return navbar