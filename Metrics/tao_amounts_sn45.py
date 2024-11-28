import requests
import streamlit as st
import pandas as pd
from datetime import datetime
from Metrics.tao_price_metrics import fetch_tao_data 
from sn45_rank_mongo import fetch_sn45_data

# Function to fetch account data from the API
def fetch_account_data():
    api_url = "https://api.taostats.io/api/account/latest/v1?address=5GseRuwpzHoimJW5CYwg2BQDtoxHD3tSKSkPEwM7fxoYrVvF"
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
            st.error("No data found in the API response.")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data: {e}")
        return None

tao_data = fetch_tao_data()
sn45_incentive = fetch_sn45_data()

# Function to display the Account Metrics

def display_account_sn45(return_data=False):
    account_data = fetch_account_data()
    if not account_data:
        return None if return_data else st.error("Failed to fetch account data for SN45.")
    
    # Compute metrics
    total_free_balance = int(account_data['balance_free'])
    total_staked_balance = int(account_data['balance_staked'])
    total_total_balance = int(account_data['balance_total'])
    total_daily_reward = 0

    if sn45_incentive:
        sn45_df = pd.DataFrame(sn45_incentive)
        selected_uids = [152, 155, 236, 53]
        filtered_data = sn45_df[sn45_df["uid"].isin(selected_uids)]

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
            label="Free Balance",
            value=f"{total_free_balance / 1000000000:,.3} 𝜏",
            delta=f"${total_free_balance * float(tao_data['price']) / 1000000000:,.2f}"
        )
    
    with col2:
        st.metric(
            label="Staked Balance",
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

