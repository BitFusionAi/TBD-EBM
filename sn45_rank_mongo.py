import streamlit as st
import pymongo
import requests
from datetime import datetime
import pandas as pd
import altair as alt
from dateutil import parser
from threading import Timer

# MongoDB Configuration
MONGO_URI = st.secrets["DB_URI"]
DB_NAME = "sn45_database"
RANK_COLLECTION = "rank_sn45_MongoNEW"

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
rank_collection = db[RANK_COLLECTION]

# Function to fetch API data
def fetch_sn45_data():
    url = 'https://api.taostats.io/api/metagraph/latest/v1'
    headers = {
        'Authorization': st.secrets["API_TAO_45"],
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

# Function to process data and save to rank_sn45 collection
# def process_and_save_rank_sn45(data):
#     # Ensure the data is valid
#     if not data:
#         return

#     # Convert API data into a DataFrame
#     df = pd.DataFrame(data)

#     # Parse and reformat the timestamp column
#     try:
#         df["timestamp"] = df["timestamp"].apply(lambda ts: parser.isoparse(ts))
#     except Exception as e:
#         st.warning(f"Failed to parse timestamps: {e}")
#         return

#     # Group by block_number and timestamp for calculations
#     unique_blocks = df.groupby(["block_number", "timestamp"])
#     processed_data = []

#     for (block, timestamp), group in unique_blocks:
#         try:
#             processed_data.append({
#                 "MAX_netuid": int(group["netuid"].max()),
#                 "MAX_block_number": int(group["block_number"].max()),
#                 "MAX_timestamp": timestamp,
#                 "MIN_daily_reward": float(group["daily_reward"].astype(float).min()),
#                 "MIN_NON_IMMUNE_daily_reward": float(group[~group["is_immunity_period"]]["daily_reward"].astype(float).min()) if not group[~group["is_immunity_period"]].empty else None,
#                 "UID_152_daily_reward": float(group[group["uid"] == 152]["daily_reward"].values[0]) if not group[group["uid"] == 152].empty else None,
#                 "UID_155_daily_reward": float(group[group["uid"] == 155]["daily_reward"].values[0]) if not group[group["uid"] == 155].empty else None,
#                 "MAX_NON_VALI_daily_reward": float(group[group["validator_trust"] == "0"]["daily_reward"].astype(float).max()) if not group[group["validator_trust"] == "0"].empty else None,
#                 "COUNT_NON_IMMUNE_daily_reward_less_UID_152": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
#                 "COUNT_NON_IMMUNE_daily_reward_less_UID_155": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
#                 "COUNT_NON_VALI_daily_reward_greater_UID_152": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
#                 "COUNT_NON_VALI_daily_reward_greater_UID_155": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
#             })

#         except KeyError as e:
#             st.warning(f"Missing data for block {block}, timestamp {timestamp}: {e}")
#             continue

#     # Replace the RANK_SN_45 collection with the new data
#     rank_collection.delete_many({})
#     rank_collection.insert_many(processed_data)

def process_and_save_rank_sn45(data):
    # Ensure the data is valid
    if not data:
        return

    # Convert API data into a DataFrame
    df = pd.DataFrame(data)

    # Parse and reformat the timestamp column
    try:
        df["timestamp"] = df["timestamp"].apply(lambda ts: parser.isoparse(ts))
    except Exception as e:
        st.warning(f"Failed to parse timestamps: {e}")
        return

    # Group by block_number and timestamp for calculations
    unique_blocks = df.groupby(["block_number", "timestamp"])
    processed_data = []

    for (block, timestamp), group in unique_blocks:
        try:
            processed_data.append({
                "MAX_netuid": int(group["netuid"].max()),
                "MAX_block_number": int(group["block_number"].max()),
                "MAX_timestamp": timestamp,
                "MIN_daily_reward": float(group["daily_reward"].astype(float).min()),
                "MIN_NON_IMMUNE_daily_reward": float(group[~group["is_immunity_period"]]["daily_reward"].astype(float).min()) if not group[~group["is_immunity_period"]].empty else None,
                "UID_152_daily_reward": float(group[group["uid"] == 152]["daily_reward"].values[0]) if not group[group["uid"] == 152].empty else None,
                "UID_155_daily_reward": float(group[group["uid"] == 155]["daily_reward"].values[0]) if not group[group["uid"] == 155].empty else None,
                "MAX_NON_VALI_daily_reward": float(group[group["validator_trust"] == "0"]["daily_reward"].astype(float).max()) if not group[group["validator_trust"] == "0"].empty else None,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_152": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_155": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_152": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_155": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
            })
        except KeyError as e:
            st.warning(f"Missing data for block {block}, timestamp {timestamp}: {e}")
            continue

    # Append new data to the RANK_SN_45 collection
    for record in processed_data:
        if not rank_collection.find_one({
            "MAX_block_number": record["MAX_block_number"],
            "MAX_timestamp": record["MAX_timestamp"]
        }):
            rank_collection.insert_one(record)



# Function to plot rank chart
def plot_rank_chart():
    data = list(rank_collection.find())
    if data:
        df = pd.DataFrame(data)

        if not df.empty:
            # MAX_timestamp is already a datetime object, no need for conversion
            # Melt the data for Altair (long format for multiple lines)
            melted_data = df.melt(
                id_vars=[
                    "MAX_timestamp",
                    "COUNT_NON_IMMUNE_daily_reward_less_UID_152",
                    "COUNT_NON_VALI_daily_reward_greater_UID_152",
                    "COUNT_NON_IMMUNE_daily_reward_less_UID_155",
                    "COUNT_NON_VALI_daily_reward_greater_UID_155"
                ],
                value_vars=[
                    "MIN_daily_reward",
                    "MIN_NON_IMMUNE_daily_reward",
                    "UID_152_daily_reward",
                    "UID_155_daily_reward",
                    "MAX_NON_VALI_daily_reward"
                ],
                var_name="Metric",
                value_name="Value"
            )

            # Base line chart for all metrics
            base_chart = alt.Chart(melted_data).mark_line(point=True).encode(
                x=alt.X("MAX_timestamp:T", title="Timestamp"),
                y=alt.Y("Value:Q", title="Rewards"),
                color="Metric:N",
                tooltip=[
                    alt.Tooltip("MAX_timestamp:T", title="Timestamp (HH:MM)"),
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("COUNT_NON_IMMUNE_daily_reward_less_UID_152:Q", title="COUNT NON IMMUNE < UID_152"),
                    alt.Tooltip("COUNT_NON_VALI_daily_reward_greater_UID_152:Q", title="COUNT NON VALI > UID_152"),
                    alt.Tooltip("COUNT_NON_IMMUNE_daily_reward_less_UID_155:Q", title="COUNT NON IMMUNE < UID_155"),
                    alt.Tooltip("COUNT_NON_VALI_daily_reward_greater_UID_155:Q", title="COUNT NON VALI > UID_155")
                ]
            )

            # Render the chart in Streamlit
            st.altair_chart(base_chart.properties(width=800, height=400).interactive(), use_container_width=True)

# Background updater every 3 minutes
def background_updater():
    data = fetch_sn45_data()
    process_and_save_rank_sn45(data)
    Timer(1080, background_updater).start()

# Display in Streamlit
def display_sn45_rank_mongo():
    background_updater()
    plot_rank_chart()
