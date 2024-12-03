from dateutil.parser import parse
import pickle
from datetime import datetime, timedelta, date

import asyncio
import aiohttp
import requests
import pandas as pd
from pyproj import Transformer
import plotly.express as px
from dash_table.Format import Format, Scheme

# Import get_redis_client from cache.py
from cache import get_redis_client

# Initialize Redis client using get_redis_client
redis_client = get_redis_client()

# Maximum number of concurrent requests
CONCURRENT_REQUESTS_LIMIT = 8
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)

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

# Function to split date range into list of dates
def split_date_range(start_date_str, end_date_str):
    start_date = parse(start_date_str).date()
    end_date = parse(end_date_str).date()
    if start_date > end_date:
        return []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    return date_list

# Asynchronous function to fetch rainfall data for a single station reference and date
async def fetch_rainfall_data_for_station_and_date(session, station_reference, date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    cache_key = f"rainfall_data:{station_reference}:{date_str}"
    cached_bytes = redis_client.get(cache_key)

    today = date.today()

    if cached_bytes:
        print(f"Fetching rainfall data for station {station_reference} on {date_str} from cache...")
        data_items = pickle.loads(cached_bytes) if isinstance(cached_bytes, bytes) else b''
    else:
        async with semaphore:
            print(f"Fetching rainfall data for station {station_reference} on {date_str} from API...")
            link = (
                f"https://environment.data.gov.uk/flood-monitoring/data/readings?"
                f"parameter=rainfall&_view=full&startdate={date_str}&enddate={date_str}"
                f"&_limit=10000&stationReference={station_reference}"
            )
            try:
                async with session.get(link) as response:
                    if response.status == 400:
                        error_text = await response.text()
                        print(f"Skipping date {date_str} for station {station_reference} due to Bad Request.")
                        print(f"Error message: {error_text}")
                        data_items = []
                    else:
                        response.raise_for_status()
                        data = await response.json()
                        data_items = data.get("items", [])
                        expiration = timedelta(days=7) if date_obj < today else timedelta(minutes=15)
                        redis_client.setex(cache_key, expiration, pickle.dumps(data_items))
            except asyncio.TimeoutError:
                print(f"Timeout for station {station_reference} on {date_str}")
                data_items = []
            except Exception as e:
                print(f"Error fetching data for station {station_reference} on {date_str}: {str(e)}")
                data_items = []

    return data_items

# Function to fetch rainfall data for a list of station references asynchronously
async def fetch_rainfall_data_async(station_references, start_date_str, end_date_str):
    val_df = pd.DataFrame()
    date_list = split_date_range(start_date_str, end_date_str)

    if not date_list:
        print("No valid dates to fetch data for.")
        return val_df

    # Configure client timeout
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for st_rf in station_references:
            for date_obj in date_list:
                task = fetch_rainfall_data_for_station_and_date(session, st_rf, date_obj)
                tasks.append(task)
        
        # Process tasks in chunks to avoid memory issues
        chunk_size = 50
        for i in range(0, len(tasks), chunk_size):
            chunk_tasks = tasks[i:i + chunk_size]
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            
            # Process results
            for data_items in results:
                if isinstance(data_items, list) and data_items:  # Skip exceptions and empty results
                    data_df = pd.json_normalize(data_items)
                    val_df = pd.concat([val_df, data_df], ignore_index=True)
    
    return val_df

# Wrapper function to run the asynchronous function
def fetch_rainfall_data(station_references, start_date, end_date):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        val_df = loop.run_until_complete(
            fetch_rainfall_data_async(station_references, start_date, end_date)
        )
    finally:
        loop.close()
    return val_df

# Function to fetch station data based on location and radius with caching
def fetch_station_data(lat, lon, radius):
    cache_key = f"station_data:{lat}:{lon}:{radius}"
    cached_bytes = redis_client.get(cache_key)

    if cached_bytes:
        print("Fetching station data from cache...")
        st_df = pickle.loads(cached_bytes) if isinstance(cached_bytes, bytes) else b''
    else:
        print("Fetching station data from API...")
        easting, northing = latlon_to_bng(lat, lon)
        st_l = "https://environment.data.gov.uk/hydrology/id/stations"
        params = {
            "observedProperty": "rainfall",
            "easting": str(easting),
            "northing": str(northing),
            "dist": str(radius),
            "_limit": "100000"
        }
        st_response = requests.get(st_l, params=params, timeout=60)
        st_response.raise_for_status()
        st_r = st_response.json()
        st_items = st_r.get("items", [])
        st_df = pd.DataFrame(st_items)
        redis_client.setex(cache_key, timedelta(hours=24), pickle.dumps(st_df))

    return st_df

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
        #size="total_rainfall",
        color_continuous_scale=px.colors.sequential.Viridis,
        size_max=15,
        zoom=9
    )
    fig.update_layout(mapbox_style="carto-positron")  # alt "open-street-map"
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig

# Main function to fetch and process all data
def fetch_and_process_data(lat, lon, radius, start_date, end_date):
    try:
        st_df = fetch_station_data(lat, lon, radius)
        if st_df.empty:
            return None, None, None, "No stations found in the specified area."

        if len(st_df) > CONCURRENT_REQUESTS_LIMIT:
            print(f"Found {len(st_df)} stations. Using semaphore to limit concurrent requests.")

        val_df = fetch_rainfall_data(st_df["stationReference"], start_date, end_date)
        if val_df.empty:
            return None, None, None, "No rainfall data found for the specified dates and area."

        val_df_grouped = process_rainfall_data(val_df)
        merged_df = pd.merge(st_df, val_df_grouped, on="stationReference", how="left")
        merged_df["total_rainfall"] = merged_df["total_rainfall"].fillna(0).round(1)

        easting = merged_df["easting"].astype(float)
        northing = merged_df["northing"].astype(float)
        lon_arr, lat_arr = bng_to_latlon(easting.values, northing.values)
        merged_df["lon"] = lon_arr
        merged_df["lat"] = lat_arr

        table_data, table_columns = prepare_table_data(merged_df)
        table_data.sort(key=lambda x: x["total_rainfall"], reverse=True)

        fig = create_map_figure(merged_df)
        message = f"Found {len(merged_df)} stations and {len(val_df)} rainfall readings."

        return table_data, table_columns, fig, message

    except Exception as e:
        return None, None, None, f"An error occurred: {str(e)}"
