import streamlit as st
from Metrics.tao_price_metrics import display_tao_metrics
from Metrics.tao_amounts_sn45 import display_account_sn45
from Metrics.tao_amounts_sn30 import display_account_sn30
from Metrics.tao_amounts_totals import display_account_total
from sn45_rank_mongo import display_sn45_rank_mongo
from sn30_rank_mongo import display_sn30_rank_mongo

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='TBD EBM',
    page_icon=':pick:',
)

# Title
st.title("TBD ⛏️")

# Section 2 - Metrics 1 (Tao Price Metrics)
st.header("Tao Price Metrics", divider='gray')
display_tao_metrics()

# Future sections can be added here following a similar modular approach

st.header("SN45 Metrics",divider='grey')
display_account_sn45()
st.subheader("Rank",divider='gray')
display_sn45_rank_mongo()

st.header("SN30 Metrics",divider='grey')
display_account_sn30()
st.subheader("Rank",divider='grey')
display_sn30_rank_mongo()


st.header("Total Metrics",divider='grey')
display_account_total()