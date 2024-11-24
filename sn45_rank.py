import streamlit as st
import pandas as pd
import sqlite3
import requests
import os
from datetime import datetime
from threading import Timer
import altair as alt
from dateutil import parser


# Ensure the data directory exists
os.makedirs("data", exist_ok=True)

# Define the database path
DB_PATH = "data/sn45_rank.db"

# Function to fetch API data
def fetch_sn45_data():
    url = 'https://api.taostats.io/api/metagraph/latest/v1'
    headers = {
        'Authorization': 'oMsSsdmi9ILQpk3Cokql3C0VPsutpKoy4O2y3RrhNn2qOxJcha7E1RbR2LTnI4E0',
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

# Function to store data in the RAW_SN_45 table
# def store_raw_sn45_data(data):
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()

#     # Create the RAW_SN_45 table if not exists
#     cursor.execute('''CREATE TABLE IF NOT EXISTS RAW_SN_45 (
#         netuid INTEGER,
#         uid INTEGER,
#         hotkey_ss58 TEXT,
#         coldkey_ss58 TEXT,
#         block_number INTEGER,
#         timestamp TEXT,
#         trust REAL,
#         stake REAL,
#         validator_trust REAL,
#         incentive REAL,
#         dividends REAL,
#         emission REAL,
#         active BOOLEAN,
#         validator_permit BOOLEAN,
#         daily_reward REAL,
#         registered_at_block INTEGER,
#         is_immunity_period BOOLEAN,
#         rank INTEGER
#     )''')

#     # Insert the data
#     for record in data:
#         if not all(key in record for key in ['netuid', 'uid', 'block_number', 'timestamp', 'daily_reward']):
#             st.warning(f"Skipping malformed record: {record}")
#             continue

#         # Parse the timestamp
#         try:
#             timestamp = parser.isoparse(record["timestamp"]).strftime("%Y %m %d %H %M")
#         except ValueError as e:
#             st.warning(f"Failed to parse timestamp: {record['timestamp']} - {e}")
#             continue

#         cursor.execute('''INSERT INTO RAW_SN_45 (
#             netuid, uid, hotkey_ss58, coldkey_ss58, block_number, timestamp, trust, stake, 
#             validator_trust, incentive, dividends, emission, active, validator_permit, 
#             daily_reward, registered_at_block, is_immunity_period, rank
#         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
#             record["netuid"], record["uid"], record["hotkey"]["ss58"], record["coldkey"]["ss58"],
#             record["block_number"], timestamp, record.get("trust", 0), record.get("stake", 0),
#             record.get("validator_trust", 0), record.get("incentive", 0), record.get("dividends", 0),
#             record.get("emission", 0), record.get("active", False), record.get("validator_permit", False),
#             record.get("daily_reward", 0), record.get("registered_at_block", 0),
#             record.get("is_immunity_period", False), record.get("rank", 0)
#         ))

#     conn.commit()
#     conn.close()

# Function to store data in the RAW_SN_45 table
def store_raw_sn45_data(data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create the RAW_SN_45 table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS RAW_SN_45 (
        netuid INTEGER,
        uid INTEGER,
        hotkey_ss58 TEXT,
        coldkey_ss58 TEXT,
        block_number INTEGER,
        timestamp TEXT,
        trust REAL,
        stake REAL,
        validator_trust REAL,
        incentive REAL,
        dividends REAL,
        emission REAL,
        active BOOLEAN,
        validator_permit BOOLEAN,
        daily_reward REAL,
        registered_at_block INTEGER,
        is_immunity_period BOOLEAN,
        rank INTEGER
    )''')

    for record in data:
        # Ensure the record has all the necessary fields
        if not all(key in record for key in ['netuid', 'uid', 'block_number', 'timestamp', 'daily_reward']):
            st.warning(f"Skipping malformed record: {record}")
            continue

        # Parse the timestamp
        try:
            timestamp = parser.isoparse(record["timestamp"]).strftime("%Y %m %d %H %M")
        except ValueError as e:
            st.warning(f"Failed to parse timestamp: {record['timestamp']} - {e}")
            continue

        # Check if the (block_number, uid, timestamp) combination already exists
        cursor.execute('''SELECT COUNT(*) FROM RAW_SN_45 WHERE block_number = ? AND uid = ? AND timestamp = ?''', 
                       (record["block_number"], record["uid"], timestamp))
        exists = cursor.fetchone()[0]

        if exists > 0:
            #st.info(f"Duplicate entry found for block_number {record['block_number']}, uid {record['uid']}, and timestamp {timestamp}. Skipping.")
            continue

        # Insert the data if not a duplicate
        cursor.execute('''INSERT INTO RAW_SN_45 (
            netuid, uid, hotkey_ss58, coldkey_ss58, block_number, timestamp, trust, stake, 
            validator_trust, incentive, dividends, emission, active, validator_permit, 
            daily_reward, registered_at_block, is_immunity_period, rank
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            record["netuid"], record["uid"], record["hotkey"]["ss58"], record["coldkey"]["ss58"],
            record["block_number"], timestamp, record.get("trust", 0), record.get("stake", 0),
            record.get("validator_trust", 0), record.get("incentive", 0), record.get("dividends", 0),
            record.get("emission", 0), record.get("active", False), record.get("validator_permit", False),
            record.get("daily_reward", 0), record.get("registered_at_block", 0),
            record.get("is_immunity_period", False), record.get("rank", 0)
        ))

    conn.commit()
    conn.close()



# Function to update the RANK_SN_45 table
def update_rank_sn45():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create the RANK_SN_45 table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS RANK_SN_45 (
        MAX_netuid INTEGER,
        MAX_block_number INTEGER,
        MAX_timestamp TEXT,
        MIN_daily_reward REAL,
        MIN_NON_IMMUNE_daily_reward REAL,
        UID_152_daily_reward REAL,
        UID_155_daily_reward REAL,
        MAX_NON_VALI_daily_reward REAL,
        COUNT_NON_IMMUNE_daily_reward_less_UID_152 INTEGER,
        COUNT_NON_IMMUNE_daily_reward_less_UID_155 INTEGER,
        COUNT_NON_VALI_daily_reward_greater_UID_152 INTEGER,
        COUNT_NON_VALI_daily_reward_greater_UID_155 INTEGER
    )''')

    # Query RAW_SN_45 to calculate metrics
    df = pd.read_sql_query("SELECT * FROM RAW_SN_45", conn)
    if not df.empty:
        df["is_immunity_period"] = df["is_immunity_period"].astype(bool)
        unique_blocks = df.groupby(["block_number", "timestamp"])
        processed_data = []

        for (block, timestamp), group in unique_blocks:
            try:
                processed_data.append({
                    'MAX_netuid': group["netuid"].max(),
                    'MAX_block_number': group["block_number"].max(),
                    'MAX_timestamp': timestamp,
                    'MIN_daily_reward': group["daily_reward"].min() if "daily_reward" in group.columns else None,
                    'MIN_NON_IMMUNE_daily_reward': group[~group["is_immunity_period"]]["daily_reward"].min() if "is_immunity_period" in group.columns else None,
                    'UID_152_daily_reward': group[group["uid"] == 152]["daily_reward"].values[0] if not group[group["uid"] == 152].empty else None,
                    'UID_155_daily_reward': group[group["uid"] == 155]["daily_reward"].values[0] if not group[group["uid"] == 155].empty else None,
                    'MAX_NON_VALI_daily_reward': group[group["validator_trust"] == 0]["daily_reward"].max() if "validator_trust" in group.columns else None,
                    'COUNT_NON_IMMUNE_daily_reward_less_UID_152': len(group[~group["is_immunity_period"] & (group["daily_reward"] < group[group["uid"] == 152]["daily_reward"].values[0])]) if not group[group["uid"] == 152].empty else 0,
                    'COUNT_NON_IMMUNE_daily_reward_less_UID_155': len(group[~group["is_immunity_period"] & (group["daily_reward"] < group[group["uid"] == 155]["daily_reward"].values[0])]) if not group[group["uid"] == 155].empty else 0,
                    'COUNT_NON_VALI_daily_reward_greater_UID_152': len(group[group["validator_trust"] == 0 & (group["daily_reward"] > group[group["uid"] == 152]["daily_reward"].values[0])]) if not group[group["uid"] == 152].empty else 0,
                    'COUNT_NON_VALI_daily_reward_greater_UID_155': len(group[group["validator_trust"] == 0 & (group["daily_reward"] > group[group["uid"] == 155]["daily_reward"].values[0])]) if not group[group["uid"] == 155].empty else 0,
                })
            except KeyError as e:
                st.warning(f"Missing data for block {block}, timestamp {timestamp}: {e}")
                continue

        # Insert into RANK_SN_45
        df_rank = pd.DataFrame(processed_data)
        df_rank.to_sql("RANK_SN_45", conn, if_exists="replace", index=False)

    conn.close()

# Function to plot rank chart
def plot_rank_chart():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM RANK_SN_45", conn)
    conn.close()

    if not df.empty:
        # Convert the MAX_timestamp column to datetime format, retaining hours and minutes
        df['MAX_timestamp'] = pd.to_datetime(df['MAX_timestamp'], format="%Y %m %d %H %M")

        # Melt the data for Altair (long format for multiple lines)
        melted_data = df.melt(
            id_vars=[
                'MAX_timestamp',
                'COUNT_NON_IMMUNE_daily_reward_less_UID_152',
                'COUNT_NON_VALI_daily_reward_greater_UID_152',
                'COUNT_NON_IMMUNE_daily_reward_less_UID_155',
                'COUNT_NON_VALI_daily_reward_greater_UID_155'
            ],
            value_vars=[
                'MIN_daily_reward',
                'MIN_NON_IMMUNE_daily_reward',
                'UID_152_daily_reward',
                'UID_155_daily_reward',
                'MAX_NON_VALI_daily_reward'
            ],
            var_name='Metric',
            value_name='Value'
        )

        # Base line chart for all metrics
        base_chart = alt.Chart(melted_data).mark_line(point=True).encode(
            x=alt.X('MAX_timestamp:T', title='Timestamp'),
            y=alt.Y('Value:Q', title='Rewards'),
            color='Metric:N',
            tooltip=[
                alt.Tooltip('MAX_timestamp:T', title='Timestamp (HH:MM)'),
                alt.Tooltip('Metric:N', title='Metric'),
                alt.Tooltip('Value:Q', title='Reward Value'),
                alt.Tooltip('COUNT_NON_IMMUNE_daily_reward_less_UID_152:Q', title='COUNT NON IMMUNE < UID_152'),
                alt.Tooltip('COUNT_NON_VALI_daily_reward_greater_UID_152:Q', title='COUNT NON VALI > UID_152'),
                alt.Tooltip('COUNT_NON_IMMUNE_daily_reward_less_UID_155:Q', title='COUNT NON IMMUNE < UID_155'),
                alt.Tooltip('COUNT_NON_VALI_daily_reward_greater_UID_155:Q', title='COUNT NON VALI > UID_155')
            ]
        )

        # Render the chart in Streamlit
        st.altair_chart(base_chart.properties(width=800, height=400).interactive(), use_container_width=True)




# Background updater every 18 minutes
def background_updater():
    data = fetch_sn45_data()
    if data:
        store_raw_sn45_data(data)
        update_rank_sn45()
    Timer(180, background_updater).start()

# Display in Streamlit
def display_sn45_rank():
    #st.header("SN45 Rank Metrics")
    background_updater()
    plot_rank_chart()
