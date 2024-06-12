import json 
import aiohttp #replaces the need to import requests, by using async requests
import asyncio #allows for asyncronous programming, aka running concurrent tasks

import requests

# Set json global 
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
async def main():
    API_key = "502c479f872645156c5a5f328ad101ff"
    cities_dict = json.loads(cities_json)
    cities = cities_dict["cities"]
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_coordinates(session, city_data, API_key) for city_data in cities]
        results = await asyncio.gather(*tasks)
        
        coordinates = results[0]
        lat = coordinates[0]["lat"]
        lon = coordinates[0]["lon"]
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}"
        weather_data = requests.get(url).json()
        
        dt = weather_data["dt"]
        temp = weather_data["main"]["temp"]
        weather = weather_data["weather"][0]["main"]
        humidity = weather_data["main"]["temp"]
        name = weather_data["name"]
        
        print("Time of data calculation (UTC):", dt)
        print("Temperature (Kelvin):", temp)
        print("Main weather condition:", weather)
        print("Humidity (%):", humidity)
        print("City name:", name)
        
        # from weather data i need to pull dt, temp, weather main, humidity
        
    
if __name__ == "__main__":
    asyncio.run(main())