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
            "country_code": "US"
            },
            {
            "city": "Los Angeles",
            "state_code": "CA",
            "country_code": "US"
            },
            {
            "city": "Houston",
            "state_code": "TX",
            "country_code": "US"
            },
            {
            "city": "Denver",
            "state_code": "CO",
            "country_code": "US"
            },
            {
            "city": "Phoenix",
            "state_code": "AZ",
            "country_code": "US"
            },
            {
            "city": "Chicago",
            "state_code": "IL",
            "country_code": "US"
            },
            {
            "city": "Miami",
            "state_code": "FL",
            "country_code": "US"
            },
            {
            "city": "Seattle",
            "state_code": "WA",
            "country_code": "US"
            },
            {
            "city": "Atlanta",
            "state_code": "GA",
            "country_code": "US"
            },
            {
            "city": "Minneapolis",
            "state_code": "MN",
            "country_code": "US"
            }
        ]
    }
"""
cities_dict = json.loads(cities_json)
cities = cities_dict["cities"]

# Fetch the coordinates
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
    # Personal API Key
    API_key = "502c479f872645156c5a5f328ad101ff"
    
    # Async requests - gatherng coordinates
    async with aiohttp.ClientSession() as session:
        # Coordinate task
        coord_tasks = [fetch_coordinates(session, city_data, API_key) for city_data in cities]
        coord_results = await asyncio.gather(*coord_tasks)
        
        # Weather task
        weather_tasks = [fetch_weather(session, coordinates, API_key) for coordinates in coord_results]
        weather_data = await asyncio.gather(*weather_tasks)
        
        # Pollutant task
        pollutant_tasks = [fetch_pollutant(session, coordinates, API_key) for coordinates in coord_results]
        pollutant_data = await asyncio.gather(*pollutant_tasks)
        
    return weather_data, pollutant_data

def transform_data(weather_data, pollutant_data, cities):
    # Initialize dataframes
    df_weather = pd.DataFrame(columns=["City", "Time (UTC)", "Temperature (F)", "Weather", "Humidity"])
    df_pollutants = pd.DataFrame(columns=["City", "Carbon Monoxide", "Nitrogen Dioxide", "Ozone", "Sulfur Dioxide", "Particulate Matter", "Ammonia"])
   
    # Iterate through weather json result
    for data in weather_data:
        # Concat weather data to dataframe
        df_weather = pd.concat([df_weather, pd.DataFrame({
            "City": [data["name"]], 
            "Time (UTC)": [data["dt"]],
            "Temperature (F)": [data["main"]["temp"]],
            "Weather": [data["weather"][0]["main"]],
            "Humidity": [data["main"]["humidity"]]
        })], ignore_index = True)
            
    # Iterate through pollutant json result
    for i, item in enumerate(pollutant_data):
        city_name = cities[i]["city"]
        # Concat weather data to dataframe
        df_pollutants = pd.concat([df_pollutants, pd.DataFrame({
            "City": [city_name],
            "Carbon Monoxide": [item["list"][0]["components"]["co"]],
            "Nitrogen Dioxide": [item["list"][0]["components"]["no2"]],
            "Ozone": [item["list"][0]["components"]["o3"]],
            "Sulfur Dioxide": [item["list"][0]["components"]["so2"]],
            "Particulate Matter": [item["list"][0]["components"]["so2"]],
            "Ammonia": [item["list"][0]["components"]["nh3"]]
        })], ignore_index = True)
        
    # Join the dataframes
    combined_df = pd.merge(df_weather, df_pollutants, on="City")
    return combined_df

#main
async def main():
    # Extracts
    weather_data, pollutant_data = await extract(cities)
    
    # Transform Data
    combined_df = transform_data(weather_data, pollutant_data, cities)
    
    # Print the dataframe
    print(tabulate(combined_df, headers = 'keys', tablefmt = 'psql'))

if __name__ == "__main__":
    asyncio.run(main())
    