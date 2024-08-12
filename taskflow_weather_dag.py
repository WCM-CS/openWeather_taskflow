import json
import aiohttp
import asyncio
import pandas as pd
import pymysql
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Set JSON globally
cities_json = """
{
    "cities": [
        {"city": "Boston", "state_code": "MA", "country_code": "US"},
        {"city": "Los Angeles", "state_code": "CA", "country_code": "US"},
        {"city": "Houston", "state_code": "TX", "country_code": "US"},
        {"city": "Denver", "state_code": "CO", "country_code": "US"},
        {"city": "Phoenix", "state_code": "AZ", "country_code": "US"},
        {"city": "Chicago", "state_code": "IL", "country_code": "US"},
        {"city": "Miami", "state_code": "FL", "country_code": "US"},
        {"city": "Seattle", "state_code": "WA", "country_code": "US"},
        {"city": "Atlanta", "state_code": "GA", "country_code": "US"},
        {"city": "Minneapolis", "state_code": "MN", "country_code": "US"}
    ]
}
"""
cities_dict = json.loads(cities_json)
cities = cities_dict["cities"]

# Fetch the coordinates
async def fetch_coordinates(session, city_data, API_key):
    city = city_data["city"]
    state = city_data["state_code"]
    country = city_data["country_code"]
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{state},{country}&limit=1&appid={API_key}"
    
    async with session.get(url) as response:
        return await response.json()

# Fetch the weather data
async def fetch_weather(session, coordinates, API_key):
    lat = coordinates[0]["lat"]
    lon = coordinates[0]["lon"]
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}&units=imperial"
    
    async with session.get(url, ssl=False) as response:
        return await response.json()

# Fetch the pollutant data
async def fetch_pollutant(session, coordinates, API_key):
    lat = coordinates[0]["lat"]
    lon = coordinates[0]["lon"]
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}"
    
    async with session.get(url) as response:
        return await response.json()

async def extract_data(cities, API_key):
    async with aiohttp.ClientSession() as session:
        coord_tasks = [fetch_coordinates(session, city_data, API_key) for city_data in cities]
        coord_results = await asyncio.gather(*coord_tasks)
        
        weather_tasks = [fetch_weather(session, coordinates, API_key) for coordinates in coord_results]
        weather_data = await asyncio.gather(*weather_tasks)
        
        pollutant_tasks = [fetch_pollutant(session, coordinates, API_key) for coordinates in coord_results]
        pollutant_data = await asyncio.gather(*pollutant_tasks)
        
    return weather_data, pollutant_data

def transform_data(weather_data, pollutant_data, cities):
    station_data_list = []
    weather_data_list = []
    pollutants_data_list = []
    
    for i, data in enumerate(weather_data):
        city_info = cities[i]
        station_data_list.append({
            "Longitude": data["coord"]["lon"],
            "Latitude": data["coord"]["lat"],
            "City": data["name"],
            "State": city_info["state_code"],
            "Country": city_info["country_code"]
        })
        
        datetime_obj = pd.to_datetime(data["dt"], unit='s').tz_localize('UTC').tz_convert('America/New_York')
        time_only = datetime_obj.time()
        date_only = datetime_obj.date()    
        weather_data_list.append({
            "Temperature_F": data["main"]["temp"],
            "Weather": data["weather"][0]["main"],
            "Humidity": data["main"]["humidity"],
            "Time_EST": time_only,
            "Date": date_only
        })
     
    for item in pollutant_data:
        pollutants_data_list.append({
            "Carbon_Monoxide": item["list"][0]["components"]["co"],
            "Nitrogen_Dioxide": item["list"][0]["components"]["no2"],
            "Ozone": item["list"][0]["components"]["o3"],
            "Sulfur_Dioxide": item["list"][0]["components"]["so2"],
            "Particulate_Matter": item["list"][0]["components"]["pm10"],
            "Ammonia": item["list"][0]["components"]["nh3"]
        })
        
    df_station = pd.DataFrame(station_data_list)
    df_weather = pd.DataFrame(weather_data_list)
    df_pollutants = pd.DataFrame(pollutants_data_list)
        
    return df_station, df_weather, df_pollutants

def fetch_dim_pks():
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="password",
        database="WeatherData",
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    weather_primaryKeys = []
    pollutants_primaryKeys = []
    
    try:
        with connection.cursor() as cursor:
            sql_A = "SELECT * FROM Weather ORDER BY Weather_ID DESC LIMIT 10"
            cursor.execute(sql_A)
            weather_primaryKeys = cursor.fetchall()
            
            sql_B = "SELECT * FROM Pollutants ORDER BY Pollutant_ID DESC LIMIT 10"
            cursor.execute(sql_B)
            pollutants_primaryKeys = cursor.fetchall()
            
    finally:
        connection.close()

    return weather_primaryKeys[::-1], pollutants_primaryKeys[::-1]

def load_dim_tables(df_weather, df_pollutants):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="password",
        database="WeatherData",
        port=3306, 
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            for row in df_weather.itertuples(index=False):
                sql_weather = """
                INSERT INTO Weather (Temperature_F, Weather, Humidity, Time_EST, Date)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql_weather, (
                    row.Temperature_F,
                    row.Weather,
                    row.Humidity,
                    row.Time_EST,
                    row.Date
                ))
                
            for data in df_pollutants.itertuples(index=False):
                sql_pollutants = """
                INSERT INTO Pollutants (Carbon_Monoxide, Nitrogen_Dioxide, Ozone, Sulfur_Dioxide, Particulate_Matter, Ammonia)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_pollutants, (
                    data.Carbon_Monoxide,
                    data.Nitrogen_Dioxide,
                    data.Ozone, 
                    data.Sulfur_Dioxide,
                    data.Particulate_Matter,
                    data.Ammonia
                ))
                
            connection.commit()
            
    finally:
        connection.close()

def load_fact_table(df_station):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="password",
        database="WeatherData",
        port=3306, 
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            weather_pk, pollutants_pk = fetch_dim_pks()
            
            for i, row in enumerate(df_station.itertuples(index=False)):
                weather_fk = weather_pk[i]['Weather_ID']
                pollutants_fk = pollutants_pk[i]['Pollutant_ID']
                
                sql_stations = """
                INSERT INTO Station (Longitude, Latitude, City, State, Country, Weather_ID, Pollutant_ID)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_stations, (
                    row.Longitude,
                    row.Latitude,
                    row.City,
                    row.State,
                    row.Country,
                    weather_fk,
                    pollutants_fk
                ))
            connection.commit()
            
    finally:
        connection.close()

# Define the DAG using TaskFlow API
@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    description='A simple weather ETL DAG using TaskFlow API',
    catchup=False
)
def weather_etl_taskflow():

    @task()
    def extract():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        weather_data, pollutant_data = loop.run_until_complete(extract_data(cities, "502c479f872645156c5a5f328ad101ff"))
        return weather_data, pollutant_data

    @task()
    def transform(weather_data, pollutant_data):
        df_station, df_weather, df_pollutants = transform_data(weather_data, pollutant_data, cities)
        return df_station, df_weather, df_pollutants

    @task()
    def load_dim(df_weather, df_pollutants):
        load_dim_tables(df_weather, df_pollutants)

    @task()
    def load_fact(df_station):
        load_fact_table(df_station)

    # Define task dependencies
    extracted_data = extract()
    transformed_data = transform(*extracted_data)
    load_dim(transformed_data[1], transformed_data[2])
    load_fact(transformed_data[0])

dag = weather_etl_taskflow()