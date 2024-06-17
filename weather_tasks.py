import json 
import aiohttp #replaces the need to import requests, by using async requests
import asyncio #allows for asyncronous programming, aka running concurrent tasks
import pandas as pd
from tabulate import tabulate
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago




