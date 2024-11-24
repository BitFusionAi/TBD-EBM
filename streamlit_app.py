import streamlit as st
from Metrics.tao_price_metrics import display_tao_metrics
from sn45_rank import display_sn45_rank

# Title
st.title("TBD ⛏️")

# Section 2 - Metrics 1 (Tao Price Metrics)
st.header("Tao Price Metrics", divider='gray')
display_tao_metrics()

# Future sections can be added here following a similar modular approach

st.header("SN45 Rank Metrics",divider='grey')
display_sn45_rank()
