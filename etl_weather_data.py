import json 
import aiohttp # Replaces the need to import requests, by using async requests
import asyncio # Allows for asyncronous programming, aka running concurrent tasks
import pandas as pd
from tabulate import tabulate
import pymysql # Allows connection to mysql db
import pymysql.cursors

# Set json globally
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
# Using the cities json go through and gather cooordinates based on the citys data
async def fetch_coordinates(session, city_data, API_key):
    # Set variables
    city = city_data["city"]
    state = city_data["state_code"]
    country = city_data["country_code"]
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{state},{country}&limit=1&appid={API_key}"
    
    # Gather async requests
    async with session.get(url) as response:
        return await response.json()
        
# Fetch the weather data
async def fetch_weather(session, coordinates, API_key):
    # Use coordinates
    lat = coordinates[0]["lat"]
    lon = coordinates[0]["lon"]
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}&units=imperial"
    
    # Gather async requests
    async with session.get(url, ssl=False) as response:
        return await response.json()
    
# Fetch the pollutant data
async def fetch_pollutant(session, coordinates, API_key):
    # Use coordinattes
    lat = coordinates[0]["lat"]
    lon = coordinates[0]["lon"]
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}"
    
    # Gather async requests
    async with session.get(url) as response:
        return await response.json()

async def extract(cities):
    # Personal API Key //*encapsulate sensitive data for prod*//
    API_key = "502c479f872645156c5a5f328ad101ff"
    
    # Async requests - gatherng coordinates
    async with aiohttp.ClientSession() as session:
        # Coordinate task, fetch coordiantes for cities based on city daat
        coord_tasks = [fetch_coordinates(session, city_data, API_key) for city_data in cities]
        coord_results = await asyncio.gather(*coord_tasks)
        
        # Weather task, fetch weather data based on coordinates
        weather_tasks = [fetch_weather(session, coordinates, API_key) for coordinates in coord_results]
        weather_data = await asyncio.gather(*weather_tasks)
        
        # Pollutant task, fetch pollutant data based on coordinates
        pollutant_tasks = [fetch_pollutant(session, coordinates, API_key) for coordinates in coord_results]
        pollutant_data = await asyncio.gather(*pollutant_tasks)
        
    # Returns weatehr and pollutant data for the given cities in independant json 
    return weather_data, pollutant_data

def transform_data(weather_data, pollutant_data, cities):
    # Initialize seperate lists to store multiple dictionaries
    station_data_list = []
    weather_data_list = []
    pollutants_data_list = []
    
    # Load data into the lists for both station and weather in one loop 
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
        time_only = datetime_obj.time()  # Extract time part
        date_only = datetime_obj.date()  # Extract date part    
        weather_data_list.append({
            "Temperature_F": data["main"]["temp"],
            "Weather": data["weather"][0]["main"],
            "Humidity": data["main"]["humidity"],
            "Time_EST": time_only,
            "Date": date_only
        })
     
    # Iterate through pollutant json result
    for item in pollutant_data:
        # load pollutants data to list
        pollutants_data_list.append({
            "Carbon_Monoxide": item["list"][0]["components"]["co"],
            "Nitrogen_Dioxide": item["list"][0]["components"]["no2"],
            "Ozone": item["list"][0]["components"]["o3"],
            "Sulfur_Dioxide": item["list"][0]["components"]["so2"],
            "Particulate_Matter": item["list"][0]["components"]["pm10"],
            "Ammonia": item["list"][0]["components"]["nh3"]
        })
        
    # convert the lists storing dictionaries into dataframes
    df_station = pd.DataFrame(station_data_list)
    df_weather = pd.DataFrame(weather_data_list)
    df_pollutants = pd.DataFrame(pollutants_data_list)
        
    # Return the three dataframes
    return df_station, df_weather, df_pollutants

# Query for the dimesnion tables primary keys from the most recent load
def fetch_dim_pks():
    connection = pymysql.connect(
        host = "localhost",
        user = "root",
        password = "password",
        database = "WeatherData",
        port=3306,
        cursorclass = pymysql.cursors.DictCursor
    )
    
    # Lists to store dimension tables primary keys
    weather_primaryKeys = []
    pollutants_primaryKeys = []
    
    try:
        with connection.cursor() as cursor:
            # Fetch weather primary keys
            sql_A = "SELECT * FROM Weather ORDER BY Weather_ID DESC LIMIT 10"
            cursor.execute(sql_A)
            weather_primaryKeys = cursor.fetchall()
            
            # Fetch polllutants primary keys
            sql_B = "SELECT * FROM Pollutants ORDER BY Pollutant_ID DESC LIMIT 10"
            cursor.execute(sql_B)
            pollutants_primaryKeys = cursor.fetchall()
            
    finally:
        connection.close()

    # Returns the dim tables primary keys reversed since the query was listed in desc order
    return weather_primaryKeys[::-1], pollutants_primaryKeys[::-1]

# Loads the data from dataframes into the dimesnion tables Weather & Pollutants
def load_dim_tables(df_weather, df_pollutants):
    connection = pymysql.connect(
        host = "localhost",
        user = "root",
        password = "password",
        database = "WeatherData",
        port=3306, 
        cursorclass = pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            # Load weather data
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
                
            # Load pollutants data
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
                
            # Commit modifications to db
            connection.commit()
            
    finally:
        connection.close()
    
    
def load_fact_table(df_station):
    connection = pymysql.connect(
        host = "localhost",
        user = "root",
        password = "password",
        database = "WeatherData",
        port=3306, 
        cursorclass = pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            weather_pk, pollutants_pk = fetch_dim_pks()
            
            for i, row in enumerate(df_station.itertuples(index=False)):
                weather_fk = weather_pk[i]['Weather_ID']
                pollutants_fk = pollutants_pk[i]['Pollutant_ID']
                
                # Load stations data
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
            # Commit data to ensure persistence
            connection.commit()
            
    finally:
        connection.close()

#main
async def main():
    # Extracts data into two json objects
    weather_data, pollutant_data = await extract(cities)
    print("Extracted Data")
    
    # Transform Data from json into three seperate dataframes
    station, weather, pollutants = transform_data(weather_data, pollutant_data, cities)
    print("Transformed Data")
    
    # Load the dimension tables data
    load_dim_tables(weather, pollutants)
    print("Loaded Dimensions Tables")
    
    # Load the fact tables data
    load_fact_table(station)
    print("Loaded Fact Tables")
    
# runs the main function, uses async due to the extract functions requirements
if __name__ == "__main__":
    asyncio.run(main())
