from dash import Output, Input, State, callback_context, no_update
from dash.exceptions import PreventUpdate
import plotly.express as px
import pandas as pd

def register_callbacks(app):
    # Callback to update circle center when latitude and longitude inputs change
    @app.callback(
        Output('location-circle', 'center'),
        [Input('latitude-input', 'value'),
         Input('longitude-input', 'value')]
    )
    def update_circle_center(lat, lon):
        if lat is None or lon is None:
            return no_update
        else:
            return [lat, lon]

    # Callback to update circle radius when radius input changes
    @app.callback(
        Output('location-circle', 'radius'),
        Input('radius-input', 'value')
    )
    def update_radius(radius):
        if radius is None:
            return no_update
        else:
            return radius * 1000  # Convert km to meters

    # Callback to control the collapse and update summary
    @app.callback(
        [Output("collapse", "is_open"),
         Output("selection-summary", "children")],
        [Input("toggle-button", "n_clicks"),
         Input("fetch-data-button", "n_clicks"),
         Input("latitude-input", "value"),
         Input("longitude-input", "value"),
         Input("radius-input", "value"),
         Input("start-date-picker", "date"),
         Input("end-date-picker", "date")],
        [State("collapse", "is_open")]
    )
    def toggle_collapse(toggle_n_clicks, fetch_n_clicks, lat, lon, radius, start_date, end_date, is_open):
        ctx = callback_context

        # Create summary text
        if lat is not None and lon is not None:
            summary = f"({lat:.2f}, {lon:.2f}) +{radius}km | {start_date} to {end_date}"
        else:
            summary = "No location selected"

        if not ctx.triggered:
            return is_open, summary

        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if button_id == "toggle-button":
            return not is_open, summary
        elif button_id == "fetch-data-button":
            return False, summary
        else:
            return is_open, summary

    # Callback to fetch data and update data-table, rainfall-map, and timeseries-plot
    @app.callback(
        [Output("data-table", "data"),
         Output("data-table", "columns"),
         Output("rainfall-map", "figure"),
         Output("timeseries-plot", "figure"),
         Output("message", "children"),
         Output("data-collapse", "is_open")],
        [Input("fetch-data-button", "n_clicks")],
        [State("latitude-input", "value"),
         State("longitude-input", "value"),
         State("radius-input", "value"),
         State("start-date-picker", "date"),
         State("end-date-picker", "date")],
        prevent_initial_call=True
    )
    def fetch_data(n_clicks, lat, lon, radius, start_date, end_date):
        if lat is None or lon is None:
            return no_update, no_update, no_update, no_update, "Please enter valid latitude and longitude.", False
        else:
            try:
                print(f"Fetching data for position: lat={lat}, lon={lon}, radius={radius}, start_date={start_date}, end_date={end_date}")
                from data import fetch_and_process_data
                table_data, table_columns, fig, raw_data, message = fetch_and_process_data(lat, lon, radius, start_date, end_date)
                
                if table_data is None:
                    return no_update, no_update, no_update, no_update, message, False
                else:
                    # Create timeseries plot
                    timeseries_fig = px.line(
                        raw_data,
                        x='dateTime',  # Ensure this column exists in raw_data
                        y='value',
                        color='label',  # Use the station label for the legend
                        labels={'value': 'Rainfall (mm)', 'dateTime': 'Time', 'label': 'Station'},
                        title='Rainfall Timeseries'
                    )
                    return table_data, table_columns, fig, timeseries_fig, message, True
            except Exception as e:
                print(f"An error occurred: {e}")
                return no_update, no_update, no_update, no_update, f"An error occurred: {str(e)}", False
