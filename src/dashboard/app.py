import streamlit as st
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Crime Analytics",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- FUNGSI LOAD DATA ---
@st.cache_data(ttl=300)
def load_data():
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    bucket = "crime-gold"
    filename = "master_crime_fact_table.csv"
    
    try:
        response = client.get_object(bucket, filename)
        df = pd.read_csv(BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        # [FIX 1] Konversi Tanggal
        if 'date_occ' in df.columns:
            df['date_occ'] = pd.to_datetime(df['date_occ'])
            
        # [FIX 2] Handle Error Sorting (Str vs Float)
        # Isi data kosong di area_name dengan 'Unknown' lalu paksa jadi String
        if 'area_name' in df.columns:
            df['area_name'] = df['area_name'].fillna('Unknown').astype(str)
            
        return df
    except Exception as e:
        # Print error ke terminal container untuk debugging
        print(f"Error loading data: {e}")
        return pd.DataFrame()

# --- MAIN APP ---
def main():
    # 1. Load Data
    df = load_data()
    
    if df.empty:
        st.error("⚠️ Data belum tersedia atau terjadi error koneksi MinIO.")
        return

    # 2. SIDEBAR FILTER
    st.sidebar.header("🔍 Filter Dashboard")
    
    # Filter Rentang Waktu
    if 'date_occ' in df.columns and not df['date_occ'].isnull().all():
        min_date = df['date_occ'].min().date()
        max_date = df['date_occ'].max().date()
    else:
        # Fallback jika kolom tanggal rusak/kosong
        min_date = pd.to_datetime('2020-01-01').date()
        max_date = pd.to_datetime('2025-12-31').date()
    
    start_date, end_date = st.sidebar.date_input(
        "Pilih Rentang Tanggal",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    
    # Filter Area (Sekarang aman dari error Sorting)
    all_areas = sorted(df['area_name'].unique().tolist())
    selected_areas = st.sidebar.multiselect("Pilih Area", all_areas, default=all_areas[:3])
    
    # Terapkan Filter
    mask = (
        (df['date_occ'].dt.date >= start_date) & 
        (df['date_occ'].dt.date <= end_date) &
        (df['area_name'].isin(selected_areas if selected_areas else all_areas))
    )
    filtered_df = df[mask]
    
    # 3. HEADER & KPI
    st.title("🚔 LAPD Crime Intelligence Dashboard")
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_crimes = len(filtered_df)
    
    # Handle kolom senjata (cek ketersediaan kolom)
    if 'weapon_desc' in filtered_df.columns:
        top_weapon = filtered_df['weapon_desc'].mode()[0] if not filtered_df.empty else "-"
    else:
        top_weapon = "-"

    # Handle rata-rata umur
    if 'vict_age' in filtered_df.columns:
        avg_age = round(filtered_df['vict_age'].mean(), 1) if not filtered_df.empty else 0
    else:
        avg_age = 0
    
    col1.metric("Total Kasus", f"{total_crimes:,}")
    col2.metric("Senjata Terbanyak", str(top_weapon))
    col3.metric("Rata-rata Umur Korban", f"{avg_age} Thn")
    col4.metric("Jumlah Area Terpilih", len(filtered_df['area_name'].unique()))
    
    st.markdown("---")

    # 4. ROW 1: PETA & TREN WAKTU
    col_left, col_right = st.columns([1, 2])
    
    with col_left:
        st.subheader("📍 Peta Sebaran (Heatmap)")
        if not filtered_df.empty and 'lat' in filtered_df.columns and 'lon' in filtered_df.columns:
            # Hapus koordinat 0 sebelum plotting map
            map_df = filtered_df[(filtered_df['lat'] != 0) & (filtered_df['lon'] != 0)]
            if not map_df.empty:
                fig_map = px.density_mapbox(
                    map_df, 
                    lat='lat', 
                    lon='lon', 
                    z=None, 
                    radius=10,
                    center=dict(lat=34.05, lon=-118.24), 
                    zoom=9,
                    mapbox_style="carto-positron"
                )
                fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Tidak ada data lokasi valid (Lat/Lon bukan 0).")
        else:
            st.warning("Kolom Lat/Lon tidak ditemukan.")

    with col_right:
        st.subheader("📈 Tren Kriminalitas Harian")
        if not filtered_df.empty:
            daily_trend = filtered_df.groupby(filtered_df['date_occ'].dt.date).size().reset_index(name='count')
            fig_trend = px.line(daily_trend, x='date_occ', y='count', markers=True, template="plotly_white")
            st.plotly_chart(fig_trend, use_container_width=True)

    # 5. ROW 2: DETAIL ANALISIS
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("📊 Top 10 Area Rawan")
        if not filtered_df.empty:
            area_counts = filtered_df['area_name'].value_counts().head(10).reset_index()
            area_counts.columns = ['Area', 'Jumlah']
            fig_bar = px.bar(area_counts, x='Jumlah', y='Area', orientation='h', color='Jumlah', template="plotly_white")
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)
        
    with col_b:
        st.subheader("⚠️ Jenis Kejahatan Terbanyak")
        # Cek kolom deskripsi kejahatan (crm_cd_desc atau crm_cd)
        cat_col = 'crm_cd_desc' if 'crm_cd_desc' in df.columns else 'crm_cd'
        
        if not filtered_df.empty and cat_col in filtered_df.columns:
            crime_types = filtered_df[cat_col].value_counts().head(10).reset_index()
            crime_types.columns = ['Jenis', 'Jumlah']
            fig_pie = px.pie(crime_types, values='Jumlah', names='Jenis', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    # 6. RAW DATA TABLE
    with st.expander("📂 Lihat Data Mentah"):
        st.dataframe(filtered_df.sort_values(by='date_occ', ascending=False).head(1000))

if __name__ == "__main__":
    main()