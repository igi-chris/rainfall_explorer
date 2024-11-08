import requests
import pandas as pd
from pyproj import Transformer
import plotly.express as px
from dash_table.Format import Format, Scheme

# Function to convert latitude and longitude to easting and northing (British National Grid)
def latlon_to_bng(lat, lon):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:27700", always_xy=True)
    easting, northing = transformer.transform(lon, lat)
    return easting, northing

# Function to convert easting and northing to latitude and longitude
def bng_to_latlon(easting, northing):
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(easting, northing)
    return lon, lat

# Function to fetch station data based on location and radius
def fetch_station_data(lat, lon, radius):
    easting, northing = latlon_to_bng(lat, lon)

    st_l = "https://environment.data.gov.uk/hydrology/id/stations"
    params = {
        "observedProperty": "rainfall",
        "easting": str(easting),
        "northing": str(northing),
        "dist": str(radius),
        "_limit": "100000"
    }

    st_response = requests.get(st_l, params=params)
    st_response.raise_for_status()
    st_r = st_response.json()
    st_items = st_r.get("items", [])
    st_df = pd.DataFrame(st_items)

    return st_df

# Function to fetch rainfall data for a list of station references
def fetch_rainfall_data(station_references, start_date, end_date):
    val_df = pd.DataFrame()
    for st_rf in station_references:
        link = (
            f"https://environment.data.gov.uk/flood-monitoring/data/readings?"
            f"parameter=rainfall&_view=full&startdate={start_date}&enddate={end_date}"
            f"&_limit=10000&stationReference={st_rf}"
        )
        data_response = requests.get(link)
        data_response.raise_for_status()
        data = data_response.json()
        data_items = data.get("items", [])
        if data_items:
            data_df = pd.json_normalize(data_items)
            val_df = pd.concat([val_df, data_df], ignore_index=True)
    return val_df

# Function to process and aggregate rainfall data
def process_rainfall_data(val_df):
    val_df["value"] = pd.to_numeric(val_df["value"], errors='coerce')
    val_df = val_df.dropna(subset=["value"])
    val_df = val_df[(val_df["value"] <= 100) & (val_df["value"] >= 0)]
    val_df_grouped = val_df.groupby("measure.stationReference")["value"].sum().reset_index()
    val_df_grouped.rename(columns={"measure.stationReference": "stationReference", "value": "total_rainfall"}, inplace=True)
    return val_df_grouped

# Function to prepare table data
def prepare_table_data(merged_df):
    table_columns = [
        {"name": "Label", "id": "label"},
        {"name": "Station Reference", "id": "stationReference"},
        {"name": "Easting", "id": "easting"},
        {"name": "Northing", "id": "northing"},
        {"name": "Total Rainfall (mm)", "id": "total_rainfall",
         "type": "numeric",
         "format": Format(precision=1, scheme=Scheme.fixed)}
    ]

    table_data = merged_df[["label", "stationReference", "easting", "northing", "total_rainfall"]].to_dict('records')
    return table_data, table_columns

# Function to create the map figure
def create_map_figure(merged_df):
    fig = px.scatter_mapbox(
        merged_df,
        lat="lat",
        lon="lon",
        hover_name="label",
        hover_data={"total_rainfall": True, "lat": False, "lon": False},
        color="total_rainfall",
        size="total_rainfall",
        color_continuous_scale=px.colors.sequential.Blues,
        size_max=15,
        zoom=8
    )
    fig.update_layout(mapbox_style="carto-positron")  # alt "open-street-map"
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig

# Main function to fetch and process all data
def fetch_and_process_data(lat, lon, radius, start_date, end_date):
    try:
        # Fetch station data
        st_df = fetch_station_data(lat, lon, radius)
        if st_df.empty:
            message = "No stations found in the specified area."
            return None, None, None, message

        # Fetch rainfall data
        val_df = fetch_rainfall_data(st_df["stationReference"], start_date, end_date)
        if val_df.empty:
            message = "No rainfall data found for the specified dates and area."
            return None, None, None, message

        # Process rainfall data
        val_df_grouped = process_rainfall_data(val_df)

        # Merge station data with rainfall data
        merged_df = pd.merge(st_df, val_df_grouped, on="stationReference", how="left")
        merged_df["total_rainfall"] = merged_df["total_rainfall"].fillna(0)

        # Round total_rainfall to 1 decimal place
        merged_df["total_rainfall"] = merged_df["total_rainfall"].round(1)

        # Convert easting and northing to latitude and longitude
        easting = merged_df["easting"].astype(float)
        northing = merged_df["northing"].astype(float)
        lon_arr, lat_arr = bng_to_latlon(easting.values, northing.values)
        merged_df["lon"] = lon_arr
        merged_df["lat"] = lat_arr

        # Prepare table data
        table_data, table_columns = prepare_table_data(merged_df)

        # Sort table data by total_rainfall descending
        table_data.sort(key=lambda x: x["total_rainfall"], reverse=True)

        # Create map figure
        fig = create_map_figure(merged_df)

        message = f"Found {len(merged_df)} stations and {len(val_df)} rainfall readings."

        return table_data, table_columns, fig, message

    except Exception as e:
        message = f"An error occurred during data fetching and processing: {str(e)}"
        return None, None, None, message
