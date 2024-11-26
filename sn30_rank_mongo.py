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
RANK_COLLECTION = "rank_sn30_MongoNEW"

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
rank_collection = db[RANK_COLLECTION]

# Function to fetch API data
def fetch_sn30_data():
    url = 'https://api.taostats.io/api/metagraph/latest/v1'
    headers = {
        'Authorization': st.secrets["API_TAO_30"],
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

def process_and_save_rank_sn30(data):
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
                "UID_85_daily_reward": float(group[group["uid"] == 85]["daily_reward"].values[0]) if not group[group["uid"] == 85].empty else None,
                "UID_254_daily_reward": float(group[group["uid"] == 254]["daily_reward"].values[0]) if not group[group["uid"] == 254].empty else None,
                "UID_34_daily_reward": float(group[group["uid"] == 34]["daily_reward"].values[0]) if not group[group["uid"] == 34].empty else None,
                "UID_5_daily_reward": float(group[group["uid"] == 5]["daily_reward"].values[0]) if not group[group["uid"] == 5].empty else None,
                "UID_101_daily_reward": float(group[group["uid"] == 101]["daily_reward"].values[0]) if not group[group["uid"] == 101].empty else None,
                "MAX_NON_VALI_daily_reward": float(group[group["validator_trust"] == "0"]["daily_reward"].astype(float).max()) if not group[group["validator_trust"] == "0"].empty else None,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_85": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 85]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 85].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_254": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 254]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 254].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_34": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 34]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 34].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_5": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 5]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 5].empty else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID_101": int(len(group[~group["is_immunity_period"] & (group["daily_reward"].astype(float) < group[group["uid"] == 101]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 101].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_85": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 85]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 85].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_254": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 254]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 254].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_34": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 34]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 34].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_5": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 5]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 5].empty else 0,
                "COUNT_NON_VALI_daily_reward_greater_UID_101": int(len(group[(group["validator_trust"] == "0") & (group["daily_reward"].astype(float) > group[group["uid"] == 101]["daily_reward"].astype(float).values[0])])) if not group[group["uid"] == 101].empty else 0,
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
            df["UID_85_daily_reward"] /= 1_000_000_000
            df["UID_254_daily_reward"] /= 1_000_000_000
            df["UID_34_daily_reward"] /= 1_000_000_000
            df["UID_5_daily_reward"] /= 1_000_000_000
            df["UID_101_daily_reward"] /= 1_000_000_000
            df["MAX_NON_VALI_daily_reward"] /= 1_000_000_000

            # Rename columns for better display
            rename_mapping = {
                "MIN_daily_reward": "Min UID",
                "MIN_NON_IMMUNE_daily_reward": "Min Non-Immune",
                "UID_85_daily_reward": "UID 85",
                "UID_254_daily_reward": "UID 254",
                "UID_34_daily_reward": "UID 34",
                "UID_5_daily_reward": "UID 5",
                "UID_101_daily_reward": "UID 101",
                "MAX_NON_VALI_daily_reward": "Max UID",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_85": "Safe 85",
                "COUNT_NON_VALI_daily_reward_greater_UID_85": "Rank 85",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_254": "Safe 254",
                "COUNT_NON_VALI_daily_reward_greater_UID_254": "Rank 254",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_34": "Safe 34",
                "COUNT_NON_VALI_daily_reward_greater_UID_34": "Rank 34",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_5": "Safe 5",
                "COUNT_NON_VALI_daily_reward_greater_UID_5": "Rank 5",
                "COUNT_NON_IMMUNE_daily_reward_less_UID_101": "Safe 101",
                "COUNT_NON_VALI_daily_reward_greater_UID_101": "Rank 101",
            }

            df.rename(columns=rename_mapping, inplace=True)

            # Melt the data for Altair (long format for multiple lines)
            melted_data = df.melt(
                id_vars=[
                    "MAX_timestamp",
                    "Safe 85",
                    "Rank 85",
                    "Safe 254",
                    "Rank 254",
                    "Safe 34",
                    "Rank 34",
                    "Safe 5",
                    "Rank 5",
                    "Safe 101",
                    "Rank 101"
                ],
                value_vars=[
                    "Min UID",
                    "Min Non-Immune",
                    "UID 85",
                    "UID 254",
                    "UID 34",
                    "UID 5",
                    "UID 101",
                    "Max UID"
                ],
                var_name="Metric",
                value_name="Value"
            )

            # Define custom colors for each metric
            custom_colors = {
                "Min UID": "#ff7f0e",  # Orange
                "Min Non-Immune": "#d62728",  # Red
                "UID 85": "#228B22",  # Green
                "UID 254": "#228B22",  # Green
                "UID 34": "#228B22",  # Green
                "UID 5": "#228B22",  # Green
                "UID 101": "#228B22",  # Green
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

            # Add tooltips for UID 85
            uid_85_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 85:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 85:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 85"
            )

            # Add tooltips for UID 254
            uid_254_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 254:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 254:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 254"
            )

            # Add tooltips for UID 34
            uid_34_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 34:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 34:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 34"
            )

                        # Add tooltips for UID 5
            uid_5_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 5:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 5:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 5"
            )

            # Add tooltips for UID 101
            uid_101_tooltip = alt.Chart(melted_data).mark_point().encode(
                x=alt.X("MAX_timestamp:T"),
                y=alt.Y("Value:Q"),
                tooltip=[
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Value:Q", title="Reward Value"),
                    alt.Tooltip("Safe 101:Q", title="Deregister Risk"),
                    alt.Tooltip("Rank 101:Q", title="Miner Rank"),
                ]
            ).transform_filter(
                alt.datum.Metric == "UID 101"
            )

            # Combine the charts
            combined_chart = base_chart + uid_85_tooltip + uid_254_tooltip + uid_34_tooltip + uid_5_tooltip + uid_101_tooltip

            # Render the chart in Streamlit
            st.altair_chart(combined_chart.properties(width=800, height=400).interactive(), use_container_width=True)


# Background updater every 3 minutes
def background_updater():
    data = fetch_sn30_data()
    process_and_save_rank_sn30(data)
    Timer(1080, background_updater).start()

# Display in Streamlit
def display_sn30_rank_mongo():
    background_updater()
    plot_rank_chart()
