import streamlit as st
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Enterprise DW", 
    layout="wide", 
    page_icon="⭐",
    initial_sidebar_state="expanded"
)

# --- FUNGSI KONEKSI MINIO ---
@st.cache_resource
def get_minio_client():
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

# --- FUNGSI LOAD DATA (PARQUET) ---
@st.cache_data(ttl=600)
def load_star_schema():
    client = get_minio_client()
    bucket = "crime-gold"
    
    data = {}
    # [CHANGE] Daftar file sekarang .parquet
    files = [
        "fact_crime", 
        "dim_area", 
        "dim_crime", 
        "dim_status", 
        "dim_weapon", 
        "dim_premis", 
        "dim_calendar"
    ]
    
    for f in files:
        filename = f"{f}.parquet"
        try:
            response = client.get_object(bucket, filename)
            # [CHANGE] Baca parquet langsung
            data[filename] = pd.read_parquet(BytesIO(response.read()))
            response.close()
            response.release_conn()
        except Exception as e:
            # print(f"Error loading {filename}: {e}") # Uncomment untuk debug
            return None
    return data

# --- FUNGSI DENORMALISASI (JOIN TABLES) ---
def denormalize_data(data):
    # Ambil Tabel Fakta
    fact = data["fact_crime.parquet"]
    
    # Helper function untuk Join
    def join_dim(df_fact, df_dim_name, key_col):
        dim_key = f"{df_dim_name}.parquet"
        if dim_key in data:
            df_dim = data[dim_key]
            # Parquet menyimpan tipe data, jadi join lebih aman
            # tapi kita pastikan tipe data key sama (string ke string) jika perlu
            # df_fact[key_col] = df_fact[key_col].astype(str)
            # df_dim[key_col] = df_dim[key_col].astype(str)
            return pd.merge(df_fact, df_dim, on=key_col, how='left')
        return df_fact

    # Lakukan Join ke 5 Dimensi Utama
    fact = join_dim(fact, "dim_area", 'area_id')
    fact = join_dim(fact, "dim_crime", 'crm_cd')
    fact = join_dim(fact, "dim_status", 'status_id')
    fact = join_dim(fact, "dim_weapon", 'weapon_id')
    fact = join_dim(fact, "dim_premis", 'premis_id')
    
    return fact

# --- MAIN APP ---
def main():
    st.title("⭐ LAPD Crime Data Warehouse (Star Schema + Parquet)")
    st.markdown("Dashboard Enterprise dengan Backend Parquet Storage.")
    
    # 1. LOAD DATA
    raw_data = load_star_schema()
    
    if not raw_data:
        st.error("⚠️ Gagal memuat data. Jalankan ETL dulu dan pastikan file .parquet ada di Gold Layer.")
        st.stop()

    # 2. PROSES JOIN (OLAP Operation)
    df = denormalize_data(raw_data)
    
    # 3. SIDEBAR FILTER
    st.sidebar.header("🔍 Filter Analisis")
    
    # Filter Tanggal
    min_date = df['date_occ'].min().date()
    max_date = df['date_occ'].max().date()
    start_date, end_date = st.sidebar.date_input("Rentang Waktu", [min_date, max_date])
    
    # Filter Area
    if 'area_name' in df.columns:
        all_areas = sorted(df['area_name'].dropna().unique().tolist())
        selected_areas = st.sidebar.multiselect("Pilih Wilayah", all_areas)
    else:
        selected_areas = []

    # Terapkan Filter
    mask = (df['date_occ'].dt.date >= start_date) & (df['date_occ'].dt.date <= end_date)
    if selected_areas:
        mask = mask & (df['area_name'].isin(selected_areas))
    
    filtered_df = df[mask]

    # 4. KPI CARDS
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    
    c1.metric("Total Kasus", f"{len(filtered_df):,}")
    
    top_crime = filtered_df['crm_cd_desc'].mode()[0] if 'crm_cd_desc' in filtered_df.columns and not filtered_df.empty else "-"
    c2.metric("Kejahatan Terbanyak", top_crime)
    
    top_weapon = filtered_df['weapon_desc'].mode()[0] if 'weapon_desc' in filtered_df.columns and not filtered_df.empty else "-"
    c3.metric("Senjata Dominan", top_weapon)
    
    avg_age = round(filtered_df['vict_age'].mean(), 1) if 'vict_age' in filtered_df.columns and not filtered_df.empty else 0
    c4.metric("Rata-rata Umur Korban", f"{avg_age} Thn")

    st.markdown("---")

    # 5. VISUALISASI UTAMA
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📈 Tren Kriminalitas")
        if not filtered_df.empty:
            daily_trend = filtered_df.groupby(filtered_df['date_occ'].dt.date).size().reset_index(name='Jumlah')
            fig_trend = px.line(daily_trend, x='date_occ', y='Jumlah', template="plotly_white")
            st.plotly_chart(fig_trend, use_container_width=True)
            
    with col2:
        st.subheader("📊 Status Kasus")
        if 'status_desc' in filtered_df.columns and not filtered_df.empty:
            status_counts = filtered_df['status_desc'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Jumlah']
            fig_pie = px.pie(status_counts, values='Jumlah', names='Status', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    # 6. VISUALISASI DIMENSI
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("📍 Top 10 Area Rawan")
        if 'area_name' in filtered_df.columns and not filtered_df.empty:
            area_counts = filtered_df['area_name'].value_counts().head(10).reset_index()
            area_counts.columns = ['Area', 'Jumlah']
            fig_bar = px.bar(area_counts, x='Jumlah', y='Area', orientation='h', color='Jumlah')
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

    with col4:
        st.subheader("🔫 Jenis Senjata")
        if 'weapon_desc' in filtered_df.columns and not filtered_df.empty:
            weapon_counts = filtered_df['weapon_desc'].value_counts().head(10).reset_index()
            weapon_counts.columns = ['Senjata', 'Jumlah']
            fig_bar2 = px.bar(weapon_counts, x='Senjata', y='Jumlah')
            st.plotly_chart(fig_bar2, use_container_width=True)

    # 7. PETA SEBARAN
    st.subheader("🗺️ Peta Panas Lokasi Kejadian")
    if not filtered_df.empty and 'lat' in filtered_df.columns:
        map_df = filtered_df[(filtered_df['lat'] != 0) & (filtered_df['lon'] != 0)]
        if not map_df.empty:
            fig_map = px.density_mapbox(
                map_df, 
                lat='lat', lon='lon', 
                radius=8,
                center=dict(lat=34.05, lon=-118.24), 
                zoom=9,
                mapbox_style="carto-positron"
            )
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("Data lokasi tidak tersedia.")

    # 8. BUKTI STRUKTUR DATA
    with st.expander("📂 Debug: Lihat Sampel Data Hasil Join"):
        st.dataframe(filtered_df.head(100))

if __name__ == "__main__":
    main()