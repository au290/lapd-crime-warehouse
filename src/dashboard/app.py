import streamlit as st
import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime

# Konfigurasi Halaman
st.set_page_config(page_title="LAPD Crime Dashboard", layout="wide")
st.title("🚔 LAPD Crime Data Warehouse Monitor")

# Koneksi ke MinIO
@st.cache_resource
def get_minio_client():
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def load_gold_data():
    client = get_minio_client()
    bucket = "crime-gold"
    
    # Ambil file hari ini
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"daily_crime_summary_{today_str}.csv"
    
    try:
        response = client.get_object(bucket, filename)
        df = pd.read_csv(BytesIO(response.read()))
        response.close()
        response.release_conn()
        return df
    except Exception as e:
        st.error(f"Data Gold hari ini ({filename}) belum tersedia. Cek Airflow!")
        return pd.DataFrame()

# Load Data
df = load_gold_data()

if not df.empty:
    # KPI Cards
    total_crimes = df['total_crimes'].sum()
    top_area = df.iloc[0]['area_name']
    
    col1, col2 = st.columns(2)
    col1.metric("Total Kejahatan (Hari Ini)", total_crimes)
    col1.metric("Area Paling Berbahaya", top_area)

    # Grafik Bar Chart
    st.subheader("Jumlah Kejahatan per Area")
    st.bar_chart(df.set_index('area_name')['total_crimes'])

    # Tabel Data
    with st.expander("Lihat Detail Data Tabel"):
        st.dataframe(df)
else:
    st.warning("Belum ada data yang bisa ditampilkan.")