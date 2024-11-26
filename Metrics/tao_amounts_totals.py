import streamlit as st
from Metrics.tao_amounts_sn45 import display_account_sn45
from Metrics.tao_amounts_sn30 import display_account_sn30
from Metrics.tao_price_metrics import fetch_tao_data

def fetch_combined_metrics():
    tao_data = fetch_tao_data()

    # Metrics for SN45
    sn45_metrics = display_account_sn45(return_data=True)

    # Metrics for SN30
    sn30_metrics = display_account_sn30(return_data=True)

    # Combine the metrics
    # combined_metrics = {
    #     "free_balance": sn45_metrics["free_balance"] + sn30_metrics["free_balance"],
    #     "staked_balance": sn45_metrics["staked_balance"] + sn30_metrics["staked_balance"],
    #     "total_balance": sn45_metrics["total_balance"] + sn30_metrics["total_balance"],
    #     "daily_reward": sn45_metrics["daily_reward"] + sn30_metrics["daily_reward"],
    #     "price": tao_data["price"],
    # }
    combined_metrics = {
        "free_balance": float(sn45_metrics["free_balance"]) + float(sn30_metrics["free_balance"]),
        "staked_balance": float(sn45_metrics["staked_balance"]) + float(sn30_metrics["staked_balance"]),
        "total_balance": float(sn45_metrics["total_balance"]) + float(sn30_metrics["total_balance"]),
        "daily_reward": float(sn45_metrics["daily_reward"]) + float(sn30_metrics["daily_reward"]),
        "price": float(tao_data["price"]),
    }

    return combined_metrics

# Function to display the combined metrics for SN30 and SN45
def display_account_total():
    combined_metrics = fetch_combined_metrics()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Free Balance",
            value=f"{combined_metrics['free_balance'] / 1000000000:,.3} 𝜏",
            delta=f"${combined_metrics['free_balance'] * combined_metrics['price'] / 1000000000:,.2f}"
        )

    with col2:
        st.metric(
            label="Total Staked Balance",
            value=f"{combined_metrics['staked_balance'] / 1000000000:,.3} 𝜏",
            delta=f"${combined_metrics['staked_balance'] * combined_metrics['price'] / 1000000000:,.2f}"
        )

    with col3:
        st.metric(
            label="Total Balance",
            value=f"{combined_metrics['total_balance'] / 1000000000:,.3} 𝜏",
            delta=f"${combined_metrics['total_balance'] * combined_metrics['price'] / 1000000000:,.2f}"
        )

    with col4:
        st.metric(
            label="Total Daily Reward",
            value=f"{combined_metrics['daily_reward'] / 1000000000:,.3} 𝜏",
            delta=f"${combined_metrics['daily_reward'] * combined_metrics['price'] / 1000000000:,.2f}/day"
        )
