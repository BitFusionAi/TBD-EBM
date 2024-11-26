import requests
import streamlit as st


# Function to fetch Tao Price data from the API
# def fetch_tao_data():
#     api_url = "https://api.taostats.io/api/price/latest/v1?asset=tao"
#     headers = {
#         'Authorization': st.secrets["API_TAO"],
#         'accept': 'application/json'
#     }

#     try:
#         response = requests.get(api_url, headers=headers)
#         response.raise_for_status()
#         data = response.json()
#         if 'data' in data and len(data['data']) > 0:
#             return data['data'][0]  # Return the first element in the data list
#         else:
#             st.error("No data found in the API response.")
#             return None
#     except requests.exceptions.RequestException as e:
#         st.error(f"Failed to fetch data: {e}")
#         return None

def fetch_tao_data():
    api_url = "https://api.taostats.io/api/price/latest/v1?asset=tao"
    headers = {
        'Authorization': st.secrets["API_TAO"],
        'accept': 'application/json'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            tao_data = data['data'][0]
            tao_data['price'] = float(tao_data['price'])  # Ensure price is a float
            return tao_data
        else:
            st.error("No data found in the API response.")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch Tao data: {e}")
        return None


# Function to display the Tao Price Metrics
def display_tao_metrics():
    tao_data = fetch_tao_data()
    if tao_data:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="Current Price (USD)",
                value=f"${float(tao_data['price']):,.2f}",
                delta=f"{float(tao_data['percent_change_24h']):.2f}% (24h)"
            )

        with col2:
            st.metric(
                label="Circulating Supply",
                value=f"{int(float(tao_data['circulating_supply'])):,} 𝜏"
            )

        with col3:
            st.metric(
                label="Max Supply",
                value=f"{int(float(tao_data['max_supply'])):,} 𝜏"
            )
