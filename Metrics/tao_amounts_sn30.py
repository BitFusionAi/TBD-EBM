import requests
import streamlit as st
import pandas as pd
from datetime import datetime
from Metrics.tao_price_metrics import fetch_tao_data
from sn30_rank_mongo import fetch_sn30_data  # Assuming fetch_sn30_data exists and is similar to fetch_sn45_data

# Function to fetch account data for a given address
def fetch_account_data(address):
    api_url = f"https://api.taostats.io/api/account/latest/v1?address={address}"
    headers = {
        'Authorization': st.secrets["API_TAO"],  # Replace with your secret
        'accept': 'application/json'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]  # Return the first element in the data list
        else:
            st.error(f"No data found for address {address}.")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data for address {address}: {e}")
        return None


# Addresses for SN30
addresses = [
    "5HES48QipR5xVQyhFDSFPCzWmtvjnE4R4Tvb4S4rBqqS6yvD",
    "5GseRuwpzHoimJW5CYwg2BQDtoxHD3tSKSkPEwM7fxoYrVvF"
]

# Function to display the Account Metrics for SN30
# def display_account_sn30():
#     total_free_balance = 0
#     total_staked_balance = 0
#     total_total_balance = 0

#     # Sum balances across both addresses
#     for address in addresses:
#         account_data = fetch_account_data(address)
#         if account_data:
#             total_free_balance += int(account_data['balance_free'])
#             total_staked_balance += int(account_data['balance_staked'])
#             total_total_balance += int(account_data['balance_total'])

#     # Display balance metrics
#     col1, col2, col3, col4 = st.columns(4)

#     with col1:
#         st.metric(
#             label="Total Free Balance",
#             value=f"{total_free_balance / 1000000000:,.3} 𝜏",
#             delta=f"${total_free_balance * float(tao_data['price']) / 1000000000:,.2f}"
#         )

#     with col2:
#         st.metric(
#             label="Total Staked Balance",
#             value=f"{total_staked_balance / 1000000000:,.3} 𝜏",
#             delta=f"${total_staked_balance * float(tao_data['price']) / 1000000000:,.2f}"
#         )

#     with col3:
#         st.metric(
#             label="Total Balance",
#             value=f"{total_total_balance / 1000000000:,.3} 𝜏",
#             delta=f"${total_total_balance * float(tao_data['price']) / 1000000000:,.2f}"
#         )

#     # Process sn30_incentive data
#     if sn30_incentive:
#         # Convert to DataFrame for easier filtering
#         sn30_df = pd.DataFrame(sn30_incentive)

#         # Filter for UIDs 254, 101, 85, 34, 5
#         selected_uids = [254, 101, 85, 34, 5]
#         filtered_data = sn30_df[sn30_df["uid"].isin(selected_uids)]

#         if not filtered_data.empty:
#             # Sum the daily_reward for the selected UIDs
#             total_daily_reward = filtered_data["daily_reward"].astype(float).sum() / 1000000000
#             staked_value = total_daily_reward * float(tao_data["price"])

#             with col4:
#                 st.metric(
#                     label="Total Daily Reward",
#                     value=f"{total_daily_reward:,.3} 𝜏",
#                     delta=f"${staked_value:,.2f}/day"
#                 )
#         else:
#             with col4:
#                 st.metric(label="Total Daily Reward", value="No Data", delta="N/A")
#     else:
#         with col4:
#             st.metric(label="Total Daily Reward", value="No Data", delta="N/A")

def display_account_sn30(return_data=False):
    # Fetch Tao price and SN30 data
    tao_data = fetch_tao_data()
    sn30_incentive = fetch_sn30_data()

    total_free_balance = 0
    total_staked_balance = 0
    total_total_balance = 0

    # Sum balances across both addresses
    for address in addresses:
        account_data = fetch_account_data(address)
        if account_data:
            total_free_balance += int(account_data['balance_free'])
            total_staked_balance += int(account_data['balance_staked'])
            total_total_balance += int(account_data['balance_total'])

    total_daily_reward = 0
    if sn30_incentive:
        sn30_df = pd.DataFrame(sn30_incentive)
        selected_uids = [254, 101, 85, 34, 5]
        filtered_data = sn30_df[sn30_df["uid"].isin(selected_uids)]

        if not filtered_data.empty:
            total_daily_reward = filtered_data["daily_reward"].astype(float).sum()

    if return_data:
        return {
            "free_balance": total_free_balance,
            "staked_balance": total_staked_balance,
            "total_balance": total_total_balance,
            "daily_reward": total_daily_reward,
        }

    # Display metrics in Streamlit
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Free Balance",
            value=f"{total_free_balance / 1000000000:,.3} 𝜏",
            delta=f"${total_free_balance * float(tao_data['price']) / 1000000000:,.2f}"
        )

    with col2:
        st.metric(
            label="Total Staked Balance",
            value=f"{total_staked_balance / 1000000000:,.3} 𝜏",
            delta=f"${total_staked_balance * float(tao_data['price']) / 1000000000:,.2f}"
        )

    with col3:
        st.metric(
            label="Total Balance",
            value=f"{total_total_balance / 1000000000:,.3} 𝜏",
            delta=f"${total_total_balance * float(tao_data['price']) / 1000000000:,.2f}"
        )

    with col4:
        st.metric(
            label="Total Daily Reward",
            value=f"{total_daily_reward / 1000000000:,.3} 𝜏",
            delta=f"${total_daily_reward * float(tao_data['price']) / 1000000000:,.2f}/day"
        )
