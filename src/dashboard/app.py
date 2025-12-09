import streamlit as st
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO
import gc

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Enterprise DW (Optimized)", 
    layout="wide", 
    page_icon="⚡",
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

# --- FUNGSI LOAD DATA (OPTIMIZED) ---
@st.cache_data(ttl=600) # Cache 10 menit
def load_dim_table(filename):
    """Load tabel dimensi kecil dengan optimasi tipe data"""
    client = get_minio_client()
    try:
        response = client.get_object("crime-gold", filename)
        # Baca sebagai string untuk ID agar aman, sisanya biarkan otomatis
        df = pd.read_csv(BytesIO(response.read()), dtype=str)
        response.close()
        response.release_conn()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_fact_table():
    """Load Fact Table (Tabel Terbesar) dengan strategi hemat memori"""
    client = get_minio_client()
    try:
        response = client.get_object("crime-gold", "fact_crime.csv")
        # Optimasi: Hanya load kolom yang perlu ditampilkan/difilter jika file sangat besar
        # Tapi untuk sekarang kita load semua dengan spesifikasi tipe data
        dtypes = {
            'dr_no': 'str',
            'area_id': 'str',
            'crm_cd': 'str', 
            'status_id': 'str',
            'weapon_id': 'str', 
            'premis_id': 'str',
            'vict_age': 'float32', # Hemat memori dibanding float64
            'lat': 'float32',
            'lon': 'float32'
        }
        df = pd.read_csv(BytesIO(response.read()), dtype=dtypes)
        response.close()
        response.release_conn()
        
        # Konversi tanggal (Wajib)
        if 'date_occ' in df.columns:
            df['date_occ'] = pd.to_datetime(df['date_occ'])
            
        return df
    except Exception as e:
        print(f"Error loading fact: {e}")
        return pd.DataFrame()

# --- FUNGSI JOIN DINAMIS (ON-THE-FLY) ---
def join_with_dims(fact_df, dims_dict):
    """Hanya melakukan join pada data yang SUDAH difilter (baris sedikit)"""
    merged = fact_df.copy()
    
    # Mapping nama file dimensi ke kolom key
    join_map = [
        ("dim_area", "area_id"),
        ("dim_crime", "crm_cd"),
        ("dim_status", "status_id"),
        ("dim_weapon", "weapon_id"),
        ("dim_premis", "premis_id")
    ]
    
    for dim_name, key in join_map:
        if dim_name in dims_dict and not dims_dict[dim_name].empty:
            dim_df = dims_dict[dim_name]
            if key in merged.columns and key in dim_df.columns:
                merged = pd.merge(merged, dim_df, on=key, how="left")
    
    return merged

# --- MAIN APP ---
def main():
    st.title("⚡ LAPD Crime Dashboard (High Performance)")
    
    # 1. LOAD DATA TERPISAH (Ringan di awal)
    with st.spinner("Memuat Data Warehouse..."):
        fact_df = load_fact_table()
        
        # Load Dimensi secara paralel (konsepnya)
        dims = {
            "dim_area": load_dim_table("dim_area.csv"),
            "dim_crime": load_dim_table("dim_crime.csv"),
            "dim_status": load_dim_table("dim_status.csv"),
            "dim_weapon": load_dim_table("dim_weapon.csv"),
            "dim_premis": load_dim_table("dim_premis.csv")
        }

    if fact_df.empty:
        st.error("Data Fact kosong. Cek Pipeline Airflow.")
        st.stop()

    # 2. FILTERING (Dilakukan pada Fact Table SAJA sebelum Join)
    st.sidebar.header("🔍 Filter Cepat")
    
    # Filter Tanggal
    min_date = fact_df['date_occ'].min().date()
    max_date = fact_df['date_occ'].max().date()
    start_date, end_date = st.sidebar.date_input("Rentang Waktu", [min_date, max_date])
    
    # Optimasi: Filter Fact Table DULUAN
    # Ini membuang jutaan baris yang tidak perlu ditampilkan
    filtered_fact = fact_df[
        (fact_df['date_occ'].dt.date >= start_date) & 
        (fact_df['date_occ'].dt.date <= end_date)
    ]
    
    # Filter Area (Optional - butuh join dulu kalau mau filter by nama area)
    # Trik: Kita filter by ID kalau user pilih nama, biar hemat memori join
    area_dim = dims["dim_area"]
    if not area_dim.empty:
        area_names = sorted(area_dim['area_name'].dropna().unique())
        selected_areas = st.sidebar.multiselect("Pilih Wilayah", area_names)
        
        if selected_areas:
            # Ambil ID dari area yang dipilih
            selected_ids = area_dim[area_dim['area_name'].isin(selected_areas)]['area_id'].tolist()
            filtered_fact = filtered_fact[filtered_fact['area_id'].isin(selected_ids)]

    # 3. JOINING (Hanya pada data yang sudah disaring)
    # Misal: Dari 1 juta baris, setelah filter tanggal cuma sisa 5000 baris.
    # Join 5000 baris itu SANGAT CEPAT dan MEMORI KECIL.
    final_df = join_with_dims(filtered_fact, dims)
    
    # Garbage Collection manual untuk membuang sampah memori
    gc.collect()

    # 4. KPI CARDS
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    
    c1.metric("Total Kasus", f"{len(final_df):,}")
    
    top_crime = final_df['crm_cd_desc'].mode()[0] if 'crm_cd_desc' in final_df.columns and not final_df.empty else "-"
    c2.metric("Kejahatan Terbanyak", str(top_crime)[:20] + "...") # Truncate biar ga ngerusak UI
    
    top_weapon = final_df['weapon_desc'].mode()[0] if 'weapon_desc' in final_df.columns and not final_df.empty else "-"
    c3.metric("Senjata Dominan", str(top_weapon)[:20] + "...")
    
    avg_age = round(final_df['vict_age'].mean(), 1) if not final_df.empty else 0
    c4.metric("Rata-rata Umur", f"{avg_age} Thn")

    st.markdown("---")

    # 5. VISUALISASI
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📈 Tren Kriminalitas")
        if not final_df.empty:
            # Grouping data kecil (sudah difilter) -> Cepat
            daily = final_df.groupby(final_df['date_occ'].dt.date).size().reset_index(name='Jumlah')
            fig = px.line(daily, x='date_occ', y='Jumlah', template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)
            
    with col2:
        st.subheader("📊 Status Kasus")
        if 'status_desc' in final_df.columns and not final_df.empty:
            status_counts = final_df['status_desc'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Jumlah']
            fig = px.pie(status_counts, values='Jumlah', names='Status', hole=0.4)
            st.plotly_chart(fig, use_container_width=True)

    # 6. PETA (Hanya tampilkan max 1000 titik agar browser tidak crash)
    st.subheader(f"🗺️ Peta Sebaran (Sampel 1000 Titik dari {len(final_df)} data)")
    if not final_df.empty:
        map_data = final_df[(final_df['lat'] != 0) & (final_df['lon'] != 0)]
        if len(map_data) > 1000:
            map_data = map_data.sample(1000)
            
        if not map_data.empty:
            fig_map = px.scatter_mapbox(
                map_data, lat="lat", lon="lon",
                color="area_name" if "area_name" in map_data.columns else None,
                zoom=9, center=dict(lat=34.05, lon=-118.24),
                mapbox_style="carto-positron"
            )
            st.plotly_chart(fig_map, use_container_width=True)

if __name__ == "__main__":
    main()