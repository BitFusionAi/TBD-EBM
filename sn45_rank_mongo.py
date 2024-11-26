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
                "UID_236_daily_reward": float(group[group["uid"] == 236]["daily_reward"].values[0]) if not group[group["uid"] == 236].empty else None,
                "MAX_NON_VALI_daily_reward": float(group[group["validator_trust"] == "0"]["daily_reward"].astype(float).max()) if not group[group["validator_trust"] == "0"].empty else None,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_152": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_155": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_236": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 236]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 236].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_152": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 152]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 152].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_155": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 155]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 155].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_236": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 236]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 236].empty else 0,
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
            # Convert MAX_timestamp to datetime if not already
            df["MAX_timestamp"] = pd.to_datetime(df["MAX_timestamp"])

            # Divide the daily amounts by 1,000,000,000 for scaling
            df["MIN_daily_reward"] /= 1_000_000_000
            df["MIN_NON_IMMUNE_daily_reward"] /= 1_000_000_000
            df["UID_152_daily_reward"] /= 1_000_000_000
            df["UID_155_daily_reward"] /= 1_000_000_000
            df["UID_236_daily_reward"] /= 1_000_000_000
            df["MAX_NON_VALI_daily_reward"] /= 1_000_000_000

            # Rename columns for better display
            rename_mapping = {
                "MIN_daily_reward": "Min UID",
                "MIN_NON_IMMUNE_daily_reward": "Min Non-Immune",
                "UID_152_daily_reward": "UID 152",
                "UID_155_daily_reward": "UID 155",
                "UID_236_daily_reward": "UID 236",
                "MAX_NON_VALI_daily_reward": "Max UID",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_152": "Safe 152",
                "COUNT_NON_VALI_daily_reward_greater_UID_152": "Rank 152",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_155": "Safe 155",
                "COUNT_NON_VALI_daily_reward_greater_UID_155": "Rank 155",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_236": "Safe 236",
                "COUNT_NON_VALI_daily_reward_greater_UID_236": "Rank 236",
            }

            df.rename(columns=rename_mapping, inplace=True)

            # Melt the data for Altair (long format for multiple lines)
            melted_data = df.melt(
                id_vars=[
                    "MAX_timestamp",
                    "Safe 152",
                    "Rank 152",
                    "Safe 155",
                    "Rank 155",
                    "Safe 236",
                    "Rank 236"
                ],
                value_vars=[
                    "Min UID",
                    "Min Non-Immune",
                    "UID 152",
                    "UID 155",
                    "UID 236",
                    "Max UID"
                ],
                var_name="Metric",
                value_name="Value"
            )

            # Define custom colors for each metric
            custom_colors = {
                "Min UID": "#ff7f0e",  # Orange
                "Min Non-Immune": "#d62728",  # Red
                "UID 152": "#228B22",  # Green
                "UID 155": "#228B22",  # Green
                "UID 236": "#228B22",  # Green
                "Max UID": "#1f77b4",  # Blue
            }

            # Base line chart for all metrics
            base_chart = alt.Chart(melted_data).mark_line(point=True).encode(
                x=alt.X("MAX_timestamp:T", title="Timestamp"),
                y=alt.Y("Value:Q", title="Rewards"),
                color=alt.Color(
                    "Metric:N",
                    title="Metric",
                    scale=alt.Scale(domain=list(custom_colors.keys()), range=list(custom_colors.values()))
                )
            )

            # Add tooltips for UID 152
            uid_152_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 152:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 152:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 152"
            )

            # Add tooltips for UID 155
            uid_155_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 155:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 155:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 155"
            )

            # Add tooltips for UID 236
            uid_236_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 236:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 236:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 236"
            )

            # Combine the charts
            combined_chart = base_chart + uid_152_tooltip + uid_155_tooltip + uid_236_tooltip

            # Render the chart in Streamlit
            st.altair_chart(combined_chart.properties(width=800, height=400).interactive(), use_container_width=True)


# Background updater every 3 minutes
def background_updater():
    data = fetch_sn45_data()
    process_and_save_rank_sn45(data)
    Timer(1080, background_updater).start()

# Display in Streamlit
def display_sn45_rank_mongo():
    background_updater()
    plot_rank_chart()
