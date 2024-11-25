import streamlit as st
from pymongo.mongo_client import MongoClient
import pandas as pd
import requests
from datetime import datetime
from threading import Timer
import altair as alt
from dateutil import parser



# Function to fetch API data
def fetch_sn30_data():
    url = 'https://api.taostats.io/api/metagraph/latest/v1'
    headers = {
        'Authorization': st.secrets["API_TAO"],
        'accept': 'application/json'
    }
    params = {
        'netuid': 30,
        'order': 'emission_desc'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data: {e}")
        return None