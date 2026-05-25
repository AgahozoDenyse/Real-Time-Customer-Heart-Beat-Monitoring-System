"""Simple Streamlit dashboard to visualize recent heartbeats from Postgres.

Run:
  streamlit run optional_streamlit_dashboard.py

Requires: streamlit, pandas, psycopg2
"""
from __future__ import annotations

import os
import time
from typing import Any

import pandas as pd
import streamlit as st
import psycopg2

st.set_page_config(page_title='Heartbeat Dashboard', layout='wide')

PG_CONN = os.getenv('PG_CONN', "host=localhost port=5432 dbname=heartdb user=heartuser password=heartpass")


@st.cache_data(ttl=5)
def load_recent(limit: int = 500) -> pd.DataFrame:
    conn = psycopg2.connect(PG_CONN)
    df = pd.read_sql_query(f"SELECT customer_id, timestamp, heart_rate, is_anomaly FROM customer_heartbeats ORDER BY timestamp DESC LIMIT {limit}", conn)
    conn.close()
    return df


def main() -> None:
    st.title('Customer Heartbeat Dashboard')

    limit = st.sidebar.slider('Rows to fetch', 50, 5000, 1000, step=50)
    df = load_recent(limit)

    st.metric('Rows fetched', len(df))
    st.metric('Anomalies', int(df['is_anomaly'].sum()) if 'is_anomaly' in df.columns else 0)

    if st.checkbox('Show raw data'):
        st.dataframe(df)

    # simple per-customer summary
    if not df.empty:
        top = df.groupby('customer_id').agg({'heart_rate': ['mean', 'max']}).reset_index()
        top.columns = ['customer_id', 'avg_hr', 'max_hr']
        st.subheader('Per-customer summary (recent window)')
        st.dataframe(top.sort_values('avg_hr', ascending=False).head(50))


if __name__ == '__main__':
    main()
