import json 
import aiohttp #replaces the need to import requests, by using async requests
import asyncio #allows for asyncronous programming, aka running concurrent tasks
import pandas as pd
from tabulate import tabulate

 # Set json globally
cities_json = """
    {
        "cities": [
            {
            "city": "Boston",
            "state_code": "MA",
            "country_code": "US",
            "station_id": 1001
            },
            {
            "city": "Los Angeles",
            "state_code": "CA",
            "country_code": "US",
            "station_id": 1002
            },
            {
            "city": "Houston",
            "state_code": "TX",
            "country_code": "US",
            "station_id": 1003,
            },
            {
            "city": "Denver",
            "state_code": "CO",
            "country_code": "US",
            "station_id": 1004
            },
            {
            "city": "Phoenix",
            "state_code": "AZ",
            "country_code": "US",
            "station_id": 1005
            },
            {
            "city": "Chicago",
            "state_code": "IL",
            "country_code": "US",
            "station_id": 1006
            },
            {
            "city": "Miami",
            "state_code": "FL",
            "country_code": "US",
            "station_id": 1007
            },
            {
            "city": "Seattle",
            "state_code": "WA",
            "country_code": "US",
            "station_id": 1008
            },
            {
            "city": "Atlanta",
            "state_code": "GA",
            "country_code": "US",
            "station_id": 1009
            },
            {
            "city": "Minneapolis",
            "state_code": "MN",
            "country_code": "US",
            "station_id": 1010
            }
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
    # Initialize seperate dataframes
    df_station = pd.DataFrame(columns=["Station_ID", "Longitude", "Latitude", "City", "State", "Country"])
    df_weather = pd.DataFrame(columns=[ "Temperature_F", "Weather", "Humidity", "Time", "Date"])
    df_pollutants = pd.DataFrame(columns=["Carbon Monoxide", "Nitrogen Dioxide", "Ozone", "Sulfur Dioxide", "Particulate Matter", "Ammonia"])
   
    # Load data into the dataframes, both station and weather in one loop 
    for i, data in enumerate(weather_data):
        city_info = cities[i]
        df_station = pd.concat([df_station, pd.DataFrame({
            "Station_ID": [data["sys"]["id"]],
            "Longitude": [data["coord"]["lon"]],
            "Latitude": [data["coord"]["lat"]],
            "City": [data["name"]],
            "State": [city_info["state_code"]],
            "Country": [city_info["country_code"]]
        })], ignore_index = True)
       
        datetime_obj = pd.to_datetime(data["dt"], unit='s')
        time_only = datetime_obj.time()  # Extract time part
        date_only = datetime_obj.date()  # Extract date part    
        df_weather = pd.concat([df_weather, pd.DataFrame({
            "Temperature_F": [data["main"]["temp"]],
            "Weather": [data["weather"][0]["main"]],
            "Humidity": [data["main"]["humidity"]],
            "Time": [time_only],
            "Date": [date_only]
        })], ignore_index = True)
            
    # Iterate through pollutant json result
    for i, item in enumerate(pollutant_data):
        city_name = cities[i]["city"]
        # Concat pollutant data to dataframe
        df_pollutants = pd.concat([df_pollutants, pd.DataFrame({
            "City": [city_name],
            "Carbon Monoxide": [item["list"][0]["components"]["co"]],
            "Nitrogen Dioxide": [item["list"][0]["components"]["no2"]],
            "Ozone": [item["list"][0]["components"]["o3"]],
            "Sulfur Dioxide": [item["list"][0]["components"]["so2"]],
            "Particulate Matter": [item["list"][0]["components"]["so2"]],
            "Ammonia": [item["list"][0]["components"]["nh3"]]
        })], ignore_index = True)
        
    # Return the three dataframes
    return df_station, df_weather, df_pollutants

#main
async def main():
    # Extracts
    weather_data, pollutant_data = await extract(cities)
    
    # Transform Data
    station, weather, pollutants = transform_data(weather_data, pollutant_data, cities)
    
    # Print the dataframes
    print(tabulate(station, headers = 'keys', tablefmt = 'psql'))
    print(tabulate(weather, headers = 'keys', tablefmt = 'psql'))
    print(tabulate(pollutants, headers = 'keys', tablefmt = 'psql'))

if __name__ == "__main__":
    asyncio.run(main())
