import streamlit as st
import pymongo
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import altair as alt
from dateutil import parser
from threading import Timer

# MongoDB Configuration
MONGO_URI = st.secrets["DB_URI"]
DB_NAME = "sn45_database"

# Connect to MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

# API Configuration
API_URL = 'https://api.taostats.io/api/metagraph/latest/v1'
API_HEADERS = {
    'Authorization': st.secrets["API_TAO_45"],
    #'Authorization': 'oMsSsdmi9ILQpk3Cokql3C0VPsutpKoy4O2y3RrhNn2qOxJcha7E1RbR2LTnI4E0',
    'accept': 'application/json'
}

# UIDs to track
UIDS = [152, 155, 236, 53, 7]  # Add more UIDs as needed

# Fetch API data
def fetch_sn45_data():
    params = {'netuid': 45, 'order': 'emission_desc'}
    try:
        response = requests.get(API_URL, headers=API_HEADERS, params=params)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data: {e}")
        return None


# Process and save data for a specific UID, including additional metrics
def process_and_save_uid_data(uid, data):
    if not data:
        return

    # Convert data into DataFrame
    df = pd.DataFrame(data)

    # Parse and reformat timestamps
    try:
        df["timestamp"] = df["timestamp"].apply(lambda ts: parser.isoparse(ts))
    except Exception as e:
        st.warning(f"Failed to parse timestamps: {e}")
        return

    # Ensure data types are consistent
    df["validator_trust"] = df["validator_trust"].astype(str)
    df["daily_reward"] = df["daily_reward"].astype(float)

    # Filter data for the given UID
    uid_data = df[df["uid"] == uid]

    if uid_data.empty:
        st.warning(f"No data found for UID {uid}")
        return

    # Group by block and timestamp for calculations
    grouped = df.groupby(["block_number", "timestamp"])
    processed_data = []

    for (block, timestamp), group in grouped:
        try:
            daily_reward = group["daily_reward"]
            is_immunity_period = group["is_immunity_period"]
            validator_trust = group["validator_trust"]

            # Ensure the UID exists in the group for comparison
            uid_reward = uid_data["daily_reward"].max() if not uid_data.empty else None

            # Perform the required calculations
            record = {
                "UID": uid,
                "MAX_block_number": int(block),
                "MAX_timestamp": timestamp,
                "DAILY_REWARD": float(uid_data["daily_reward"].max()),
                "IS_IMMUNE": bool(uid_data["is_immunity_period"].any()),
                "MIN_daily_reward": daily_reward.min(),
                "MIN_NON_IMMUNE_daily_reward": daily_reward[~is_immunity_period].min()
                if not daily_reward[~is_immunity_period].empty else None,
                "MAX_NON_VALI_daily_reward": daily_reward[validator_trust == "0"].max()
                if not daily_reward[validator_trust == "0"].empty else None,
                "COUNT_NON_VALI_daily_reward_greater_UID": int(
                    len(group[(validator_trust == "0") & (daily_reward > uid_reward)])
                ) if uid_reward else 0,
                "COUNT_NON_IMMUNE_daily_reward_less_UID": int(
                    len(group[~is_immunity_period & (daily_reward < uid_reward)])
                ) if uid_reward else 0
            }
            processed_data.append(record)
        except KeyError as e:
            st.warning(f"Missing data for block {block}, timestamp {timestamp}: {e}")
            continue

    # Save data to MongoDB
    collection_name = f"rank_sn45_UID_{uid}"
    uid_collection = db[collection_name]

    for record in processed_data:
        if not uid_collection.find_one({
            "MAX_block_number": record["MAX_block_number"],
            "MAX_timestamp": record["MAX_timestamp"]
        }):
            uid_collection.insert_one(record)


# Plot data for all UIDs

def create_unique_combination_df():
    
    # Calculate the time threshold for filtering (48 hours ago) with timezone-aware datetime
    #time_threshold = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=6)
    
    combined_data = []

    # Collect data from all UID collections
    for uid in UIDS:
        collection_name = f"rank_sn45_UID_{uid}"
        uid_collection = db[collection_name]
        data = list(uid_collection.find({"MAX_timestamp": {"$gte": time_threshold}}))
        

        if data:
            # Convert data to a DataFrame
            df = pd.DataFrame(data)

            # Keep only unique combination columns
            df = df[["MAX_timestamp", "MIN_daily_reward", "MIN_NON_IMMUNE_daily_reward", "MAX_NON_VALI_daily_reward"]]
            df.rename(
                columns={
                    "MIN_daily_reward": f"Min Miner",
                    "MIN_NON_IMMUNE_daily_reward": f"Min Non-Immune",
                    "MAX_NON_VALI_daily_reward": f"Max Miner",
                },
                inplace=True,
            )
            combined_data.append(df)

    if combined_data:
        combined_df = pd.concat(combined_data).drop_duplicates(subset=["MAX_timestamp"])
        combined_df["MAX_timestamp"] = pd.to_datetime(combined_df["MAX_timestamp"])
        return combined_df
    else:
        st.warning("No unique combination data available.")
        return pd.DataFrame()

def create_rewards_df():

    # Calculate the time threshold for filtering (48 hours ago) with timezone-aware datetime
    #time_threshold = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=6)
    
    rewards_data = []

    # Collect data from all UID collections
    for uid in UIDS:
        collection_name = f"rank_sn45_UID_{uid}"
        uid_collection = db[collection_name]
        data = list(uid_collection.find({"MAX_timestamp": {"$gte": time_threshold}}))

        if data:
            # Convert data to a DataFrame
            df = pd.DataFrame(data)

            # Rename the DAILY_REWARD column to be specific for this UID
            df.rename(columns={"DAILY_REWARD": f"UID {uid}"}, inplace=True)

            # Keep only relevant columns
            df = df[["MAX_timestamp", f"UID {uid}"]]
            df["MAX_timestamp"] = pd.to_datetime(df["MAX_timestamp"])
            rewards_data.append(df)

    if rewards_data:
        # Merge all UID-specific DataFrames on MAX_timestamp
        rewards_df = rewards_data[0]
        for df in rewards_data[1:]:
            rewards_df = pd.merge(rewards_df, df, on="MAX_timestamp", how="outer")

        return rewards_df
    else:
        st.warning("No rewards data available.")
        return pd.DataFrame()

def create_rank_risk_df():
    rank_risk_data = []

    # Calculate the time threshold for filtering (48 hours ago) with timezone-aware datetime
    #time_threshold = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=6)

    # Collect data from all UID collections
    for uid in UIDS:
        collection_name = f"rank_sn45_UID_{uid}"
        uid_collection = db[collection_name]
        data = list(uid_collection.find({"MAX_timestamp": {"$gte": time_threshold}}))

        if data:
            # Convert data to a DataFrame
            df = pd.DataFrame(data)

            # Rename columns to include UID
            df.rename(
                columns={
                    "COUNT_NON_VALI_daily_reward_greater_UID": f"Miner_Rank_UID{uid}",
                    "COUNT_NON_IMMUNE_daily_reward_less_UID": f"Deregister_Risk_UID{uid}",
                },
                inplace=True,
            )

            # Keep only relevant columns
            df = df[["MAX_timestamp", f"Miner_Rank_UID{uid}", f"Deregister_Risk_UID{uid}"]]
            df["MAX_timestamp"] = pd.to_datetime(df["MAX_timestamp"])
            rank_risk_data.append(df)

    if rank_risk_data:
        # Merge all UID-specific DataFrames on MAX_timestamp
        rank_risk_df = rank_risk_data[0]
        for df in rank_risk_data[1:]:
            rank_risk_df = pd.merge(rank_risk_df, df, on="MAX_timestamp", how="outer")

        return rank_risk_df
    else:
        st.warning("No rank and risk data available.")
        return pd.DataFrame()


def create_combined_df():
    unique_df = create_unique_combination_df()
    rewards_df = create_rewards_df()
    rank_risk_df = create_rank_risk_df()

    # Ensure all timestamps are in the same format
    if not unique_df.empty:
        unique_df["MAX_timestamp"] = pd.to_datetime(unique_df["MAX_timestamp"])
    if not rewards_df.empty:
        rewards_df["MAX_timestamp"] = pd.to_datetime(rewards_df["MAX_timestamp"])
    if not rank_risk_df.empty:
        rank_risk_df["MAX_timestamp"] = pd.to_datetime(rank_risk_df["MAX_timestamp"])

    # Merge the DataFrames
    if not unique_df.empty and not rewards_df.empty and not rank_risk_df.empty:
        combined_df = pd.merge(unique_df, rewards_df, on="MAX_timestamp", how="outer")
        combined_df = pd.merge(combined_df, rank_risk_df, on="MAX_timestamp", how="outer")
        return combined_df
    else:
        st.warning("No data available to combine.")
        return pd.DataFrame()


def prepare_chart_data(combined_df):
    if combined_df.empty:
        return pd.DataFrame()

    # Scale values for better readability
    for column in combined_df.columns:
        if column not in ["MAX_timestamp"] and not column.startswith("Miner_Rank_UID") and not column.startswith("Deregister_Risk_UID"):
            combined_df[column] /= 1_000_000_000

    # Melt the DataFrame for Altair
    rank_risk_columns = [col for col in combined_df.columns if "Miner_Rank_UID" in col or "Deregister_Risk_UID" in col]
    melted_df = combined_df.melt(
        id_vars=["MAX_timestamp"] + rank_risk_columns,
        var_name="Metric",
        value_name="Value"
    )

    return melted_df


def generate_chart(melted_df, combined_df, dynamic_uids):
    # Ensure the DataFrame is not empty
    if melted_df.empty:
        st.warning("No data available to plot.")
        return
    

    # Define custom colors for key metrics
    custom_colors = {
        "Min Miner": "#ff7f0e",  # Orange
        "Min Non-Immune": "#d62728",  # Red
        "Max Miner": "transparent",  # Blue
    }

    light_colors = [
        "#f0ead2",
        "#656d4a",
        "#adc178",
        "#7f4f24",
        "#582f0e",
        "#5a189a",
        "#9d4edd"
        ]
    
    uid_colors = {
        f"UID {uid}": light_colors[i % len(light_colors)] for i, uid in enumerate(dynamic_uids)
    }

    # Combine the predefined colors and dynamically generated UID colors
    custom_colors.update(uid_colors)

    # Base line chart for all metrics
    base_chart = alt.Chart(melted_df).mark_line(point=False).encode(
        x=alt.X("MAX_timestamp:T", title="Timestamp"),
        y=alt.Y("Value:Q", title="Rewards"),
        #color="Metric:N",
        color=alt.Color(
            "Metric:N",
            title="Metric",
            scale=alt.Scale(domain=list(custom_colors.keys()), range=list(custom_colors.values()))

        ),
        tooltip=[
            alt.Tooltip("Metric:N", title="Metric"),
            alt.Tooltip("Value:Q", title="Reward Value"),
        ]
    )

    # Add tooltips dynamically for each UID
    
    uid_charts = []
    for uid in dynamic_uids:
        uid_chart = alt.Chart(melted_df).mark_point().encode(
            x=alt.X("MAX_timestamp:T"),
            y=alt.Y("Value:Q"),
            color=alt.value("transparent"),  # Set a specific color for the point
            tooltip=[
                alt.Tooltip("Metric:N", title="Metric"),
                alt.Tooltip("Value:Q", title="Reward Value"),
                alt.Tooltip(f"Miner_Rank_UID{uid}:Q", title=f"Miner Rank"),
                alt.Tooltip(f"Deregister_Risk_UID{uid}:Q", title=f"Deregister Risk"),
            ]
        ).transform_filter(
            alt.datum.Metric == f"UID {uid}"
        )
        uid_charts.append(uid_chart)

    # Combine the base chart with UID-specific tooltips
    combined_chart = base_chart
    for chart in uid_charts:
        combined_chart += chart

    # Render the chart in Streamlit
    st.altair_chart(combined_chart.properties(width=1600, height=400).interactive(), use_container_width=True)




# Background updater for all UIDs
def background_updater():
    data = fetch_sn45_data()
    for uid in UIDS:
        process_and_save_uid_data(uid, data)
    Timer(1080, background_updater).start()




# Display data for all UIDs

def display_sn45_rank_mongo():
    # Fetch initial data
    data = fetch_sn45_data()
    for uid in UIDS:
        process_and_save_uid_data(uid, data)
    
    # Start the background updater asynchronously
    Timer(1080, background_updater).start()

    # Create the combined DataFrame (only includes data from the last 48 hours)
    combined_df = create_combined_df()

    # Prepare the data for plotting
    melted_df = prepare_chart_data(combined_df)

    # Generate and display the chart
    generate_chart(melted_df, combined_df, UIDS)

