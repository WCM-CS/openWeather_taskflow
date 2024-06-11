import requests
import json 
import aiohttp
import asyncio

#Variables
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
    },
    {
    "city": "Manchester",
    "state_code": "NH",
    "country_code": "US"
    }
]}
"""

    
if __name__ == "__main__":
    
    API_key = "b73f0fb012694f161d60c11ce2174ab9"
    
    #Extracting data: Geocoding API
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},{state_code},{country_code}&limit=1&appid={API_key}"
    GeoResponse = requests.get(url)
    data = GeoResponse.json()
    
    if data:
        print(data[0]["lat"], data[0]["lon"])
    else:
        print("No Values Found")
    
    
    