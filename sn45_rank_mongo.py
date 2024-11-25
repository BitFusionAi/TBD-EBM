import streamlit as st
import pandas as pd
import pymongo
import requests
import os
from datetime import datetime
from threading import Timer
import altair as alt
from dateutil import parser

# MongoDB Configuration
MONGO_URI = st.secrets["DB_URI"]
DB_NAME = "sn45_database"
RAW_COLLECTION = "raw_sn45"
RANK_COLLECTION = "rank_sn45"

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
raw_collection = db[RAW_COLLECTION]
rank_collection = db[RANK_COLLECTION]

# Function to fetch API data
def fetch_sn45_data():
    url = 'https://api.taostats.io/api/metagraph/latest/v1'
    headers = {
        'Authorization': st.secrets["API_TAO"],
        'accept': 'application/json'
    }
    params = {
        'netuid': 45,
        'order': 'emission_desc'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data: {e}")
        return None

# Function to store data in the RAW_SN_45 collection
def store_raw_sn45_data(data):
    for record in data:
        # Ensure the record has all the necessary fields
        if not all(key in record for key in ['netuid', 'uid', 'block_number', 'timestamp', 'daily_reward']):
            st.warning(f"Skipping malformed record: {record}")
            continue

        # Parse the timestamp
        try:
            record["timestamp"] = parser.isoparse(record["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            st.warning(f"Failed to parse timestamp: {record['timestamp']} - {e}")
            continue

        # Check for duplicates based on block_number, uid, and timestamp
        existing_record = raw_collection.find_one({
            "block_number": record["block_number"],
            "uid": record["uid"],
            "timestamp": record["timestamp"]
        })

        if existing_record:
            continue

        # Insert the record into the collection
        raw_collection.insert_one(record)

# Function to update the RANK_SN_45 collection
def update_rank_sn45():
    raw_data = list(raw_collection.find())
    if not raw_data:
        return

    df = pd.DataFrame(raw_data)
    if df.empty:
        return

    df["is_immunity_period"] = df.get("is_immunity_period", False).astype(bool)
    unique_blocks = df.groupby(["block_number", "timestamp"])
    processed_data = []

    for (block, timestamp), group in unique_blocks:
        try:
            processed_data.append({
                'MAX_netuid': group["netuid"].max(),
                'MAX_block_number': group["block_number"].max(),
                'MAX_timestamp': timestamp,
                'MIN_daily_reward': group["daily_reward"].min(),
                'MIN_NON_IMMUNE_daily_reward': group[~group["is_immunity_period"]]["daily_reward"].min(),
                'UID_152_daily_reward': group[group["uid"] == 152]["daily_reward"].values[0] if not group[group["uid"] == 152].empty else None,
                'UID_155_daily_reward': group[group["uid"] == 155]["daily_reward"].values[0] if not group[group["uid"] == 155].empty else None,
                'MAX_NON_VALI_daily_reward': group[group["validator_trust"] == 0]["daily_reward"].max(),
                'COUNT_NON_IMMUNE_daily_reward_less_UID_152': len(group[~group["is_immunity_period"] & (group["daily_reward"] < group[group["uid"] == 152]["daily_reward"].values[0])]) if not group[group["uid"] == 152].empty else 0,
                'COUNT_NON_IMMUNE_daily_reward_less_UID_155': len(group[~group["is_immunity_period"] & (group["daily_reward"] < group[group["uid"] == 155]["daily_reward"].values[0])]) if not group[group["uid"] == 155].empty else 0,
                'COUNT_NON_VALI_daily_reward_greater_UID_152': len(group[(group["validator_trust"] == 0) & (group["daily_reward"] > group[group["uid"] == 152]["daily_reward"].values[0])]) if not group[group["uid"] == 152].empty else 0,
                'COUNT_NON_VALI_daily_reward_greater_UID_155': len(group[(group["validator_trust"] == 0) & (group["daily_reward"] > group[group["uid"] == 155]["daily_reward"].values[0])]) if not group[group["uid"] == 155].empty else 0,
            })
        except KeyError as e:
            st.warning(f"Missing data for block {block}, timestamp {timestamp}: {e}")
            continue

    # Replace the RANK_SN_45 collection with updated data
    rank_collection.delete_many({})
    if processed_data:
        rank_collection.insert_many(processed_data)

# Function to plot rank chart
def plot_rank_chart():
    rank_data = list(rank_collection.find())
    if not rank_data:
        return

    df = pd.DataFrame(rank_data)
    df['MAX_timestamp'] = pd.to_datetime(df['MAX_timestamp'])

    melted_data = df.melt(
        id_vars=['MAX_timestamp'],
        value_vars=['MIN_daily_reward', 'MIN_NON_IMMUNE_daily_reward', 'UID_152_daily_reward', 'UID_155_daily_reward', 'MAX_NON_VALI_daily_reward'],
        var_name='Metric',
        value_name='Value'
    )

    base_chart = alt.Chart(melted_data).mark_line(point=True).encode(
        x=alt.X('MAX_timestamp:T', title='Timestamp'),
        y=alt.Y('Value:Q', title='Rewards'),
        color='Metric:N',
        tooltip=['MAX_timestamp:T', 'Metric:N', 'Value:Q']
    )

    st.altair_chart(base_chart.properties(width=800, height=400).interactive(), use_container_width=True)

# Background updater every 18 minutes
def background_updater():
    data = fetch_sn45_data()
    if data:
        store_raw_sn45_data(data)
        update_rank_sn45()
    Timer(180, background_updater).start()

# Display in Streamlit
def display_sn45_rank_mongo():
    background_updater()
    plot_rank_chart()
