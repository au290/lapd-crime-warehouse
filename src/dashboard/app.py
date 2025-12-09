import streamlit as st
import pandas as pd
from minio import Minio
from io import BytesIO

st.set_page_config(page_title="LAPD Crime Dashboard", layout="wide")
st.title("🚔 LAPD Crime Data Warehouse Monitor")

@st.cache_resource
def get_minio_client():
    return Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

def load_gold_data():
    client = get_minio_client()
    # [FIX] Baca file master
    filename = "master_crime_summary.csv"
    try:
        response = client.get_object("crime-gold", filename)
        df = pd.read_csv(BytesIO(response.read()))
        response.close()
        response.release_conn()
        return df
    except Exception:
        return pd.DataFrame()

df = load_gold_data()

if not df.empty:
    col1, col2 = st.columns(2)
    col1.metric("Total Kejahatan (Sejarah + Harian)", df['total_crimes'].sum())
    col1.metric("Area Terbanyak", df.iloc[0]['area_name'])
    st.bar_chart(df.set_index('area_name')['total_crimes'])
    st.dataframe(df)
else:
    st.warning("Data belum tersedia.")