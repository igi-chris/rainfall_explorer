# layout.py

from datetime import date, timedelta

from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import dash_leaflet as dl

from dash_table.Format import Format, Scheme

# Initial position and radius
initial_lat = 52.45
initial_lon = -2.15
initial_radius = 20  # in km

# Define the layout
app_layout = dbc.Container([
    html.H1("Rainfall Data Explorer", className="text-center"),

    # Row containing button and summary
    dbc.Row([
        dbc.Col([
            dbc.Button(
                "Show/Hide Selection Controls",
                id="toggle-button",
                color="dark",
                n_clicks=0,
                className="me-2"
            ),
            html.Div(
                id="selection-summary",
                className="d-inline-block align-middle",
                style={
                    "padding": "6px 12px",
                    "border": "1px solid #ccc",
                    "border-radius": "4px",
                    "background-color": "#f8f9fa"
                }
            )
        ], width=12)
    ], className="mb-3"),

    dbc.Collapse(
        id="collapse",
        is_open=True,
        children=[
            dbc.Row([
                dbc.Col([
                    html.H4("Select Location"),
                    dbc.Label("Latitude:"),
                    dbc.Input(id="latitude-input", type="number", value=initial_lat, step=0.0001),
                    html.Br(),
                    dbc.Label("Longitude:"),
                    dbc.Input(id="longitude-input", type="number", value=initial_lon, step=0.0001),
                    html.Br(),
                    dl.Map(center=[initial_lat, initial_lon], zoom=8, children=[
                        dl.TileLayer(),
                        dl.Circle(
                            center=[initial_lat, initial_lon],
                            radius=initial_radius * 1000,  # Convert km to meters
                            id='location-circle',
                            color='blue',
                            fillColor='blue',
                            fillOpacity=0.2,
                        ),
                    ], style={'width': '100%', 'height': '50vh'}, id="map"),
                ], width=6),

                dbc.Col([
                    html.H4("Parameters"),
                    dbc.Label("Radius (km):"),
                    dbc.Input(id="radius-input", type="number", value=initial_radius, min=1, max=200, step=1),
                    html.Br(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Start Date:"),
                            dcc.DatePickerSingle(
                                id="start-date-picker",
                                date=(date.today() - timedelta(days=0)),
                                display_format='DD/MM/YYYY'
                            ),
                        ], width=6),
                        dbc.Col([
                            dbc.Label("End Date:"),
                            dcc.DatePickerSingle(
                                id="end-date-picker",
                                date=date.today(),
                                display_format='DD/MM/YYYY'
                            ),
                        ], width=6),
                    ]),
                    dbc.Button(
                        "Fetch Data",
                        id="fetch-data-button",
                        color="success",
                        className="mt-3"
                    ),
                    html.Div(id="message", style={"marginTop": "10px", "color": "slategray"})
                ], width=6)
            ])
        ]
    ),

    dbc.Row([
        dbc.Col([
            html.H4("Aggregated Data Table"),
            dcc.Loading(
                id="loading-table",
                type="default",
                children=dash_table.DataTable(
                    id="data-table",
                    page_size=10,
                    sort_action='native',
                    sort_by=[{"column_id": "total_rainfall", "direction": "desc"}]
                )
            )
        ], width=12)
    ]),

    dbc.Row([
        dbc.Col([
            html.H4("Rainfall Data Map"),
            dcc.Loading(
                id="loading-map",
                type="default",
                children=dcc.Graph(id="rainfall-map")
            )
        ], width=12)
    ])
])
