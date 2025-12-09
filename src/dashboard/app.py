import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import duckdb

# --- 1. KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Crime Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. ENGINE KONEKSI (DuckDB + MinIO) ---
@st.cache_resource
def get_db_connection():
    # In-memory OLAP database
    con = duckdb.connect(database=':memory:')
    
    # Setup koneksi S3 ke MinIO
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='minio:9000';
        SET s3_use_ssl=false;
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_url_style='path';
    """)
    return con

# --- 3. DATA LOADER (Metadata untuk Filter) ---
def load_filter_options(con):
    try:
        # Ambil range tanggal dari Fact Table (scan metadata saja, cepat)
        dates = con.execute("SELECT min(date_occ), max(date_occ) FROM read_parquet('s3://crime-gold/fact_crime.parquet')").fetchone()
        
        # Ambil daftar Area
        areas = con.execute("SELECT DISTINCT area_name FROM read_parquet('s3://crime-gold/dim_area.parquet') ORDER BY 1").df()
        
        # Ambil daftar Kejahatan
        crimes = con.execute("SELECT DISTINCT crm_cd_desc FROM read_parquet('s3://crime-gold/dim_crime.parquet') ORDER BY 1").df()
        
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist()
    except Exception as e:
        return None, [], []

# --- 4. DATA ENGINE (Query Utama) ---
def query_analytical_data(con, start_date, end_date, selected_areas, selected_crimes):
    # Dynamic SQL Building
    where_clauses = [f"f.date_occ BETWEEN '{start_date}' AND '{end_date}'"]
    
    if selected_areas:
        areas_str = "', '".join(selected_areas)
        where_clauses.append(f"da.area_name IN ('{areas_str}')")
        
    if selected_crimes:
        # Escape single quotes untuk keamanan SQL sederhana
        safe_crimes = [c.replace("'", "''") for c in selected_crimes]
        crimes_str = "', '".join(safe_crimes)
        where_clauses.append(f"dc.crm_cd_desc IN ('{crimes_str}')")

    where_stmt = " AND ".join(where_clauses)

    # Query Utama: Join Fact + Dimensions
    # Limit 50k untuk visualisasi peta agar browser tidak crash/berat
    sql = f"""
        SELECT 
            f.date_occ,
            da.area_name,
            dc.crm_cd_desc,
            dw.weapon_desc,
            ds.status_desc,
            f.vict_age,
            f.lat,
            f.lon
        FROM read_parquet('s3://crime-gold/fact_crime.parquet') f
        LEFT JOIN read_parquet('s3://crime-gold/dim_area.parquet') da ON f.area_id = da.area_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_crime.parquet') dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN read_parquet('s3://crime-gold/dim_status.parquet') ds ON f.status_id = ds.status_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_weapon.parquet') dw ON f.weapon_id = dw.weapon_id
        WHERE {where_stmt}
        AND f.lat != 0 AND f.lon != 0 
        LIMIT 50000
    """
    
    return con.execute(sql).df()

def query_kpi_stats(con, start_date, end_date):
    # Query terpisah untuk KPI total (tanpa limit) agar akurat
    sql = f"""
        SELECT count(*) as total_cases, avg(vict_age) as avg_age
        FROM read_parquet('s3://crime-gold/fact_crime.parquet')
        WHERE date_occ BETWEEN '{start_date}' AND '{end_date}'
    """
    return con.execute(sql).df()

# --- 5. UI UTAMA ---
def main():
    con = get_db_connection()
    
    # --- SIDEBAR ---
    st.sidebar.title("🔍 Filter Dashboard")
    
    # Load Metadata
    dates, area_opts, crime_opts = load_filter_options(con)
    
    if not dates or not dates[0]:
        st.error("Gagal terhubung ke Data Warehouse. Pastikan MinIO & ETL berjalan.")
        st.stop()

    min_d, max_d = dates
    
    # Input Filters
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [min_d, max_d])
    sel_areas = st.sidebar.multiselect("Wilayah (Area)", area_opts)
    sel_crimes = st.sidebar.multiselect("Jenis Kejahatan", crime_opts)
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 **Tips:** Heatmap menunjukkan konsentrasi kejadian. Zoom in untuk melihat detail jalan.")

    # --- BODY ---
    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"Data Analisis Periode: {s_date} s/d {e_date}")

    # Load Data
    with st.spinner("Memproses data analitik..."):
        df = query_analytical_data(con, s_date, e_date, sel_areas, sel_crimes)
        kpi_df = query_kpi_stats(con, s_date, e_date)

    if df.empty:
        st.warning("Tidak ada data yang cocok dengan filter.")
        return

    # --- KPI ROW ---
    col1, col2, col3, col4 = st.columns(4)
    
    total_cases = kpi_df['total_cases'][0]
    col1.metric("Total Kasus", f"{total_cases:,}", delta_color="off")
    
    top_crime = df['crm_cd_desc'].mode()[0] if not df.empty else "-"
    col2.metric("Kejahatan #1", top_crime[:15] + "..." if len(top_crime)>15 else top_crime)
    
    top_area = df['area_name'].mode()[0] if not df.empty else "-"
    col3.metric("Area Rawan", top_area)
    
    avg_age = round(kpi_df['avg_age'][0] or 0)
    col4.metric("Avg Umur Korban", f"{avg_age} Thn")

    st.markdown("---")

    # --- TABS LAYOUT ---
    tab1, tab2, tab3 = st.tabs(["🗺️ Peta Panas (Heatmap)", "📈 Grafik & Tren", "📋 Data Mentah"])

    # TAB 1: PETA PYDECK (HEATMAP)
    with tab1:
        st.subheader("Peta Konsentrasi Kriminalitas")
        
        # Palet Warna Heatmap (Biru -> Kuning -> Merah)
        COLOR_RANGE = [
            [65, 182, 196],
            [127, 205, 187],
            [199, 233, 180],
            [237, 248, 177],
            [254, 224, 139],
            [253, 174, 97],
            [244, 109, 67],
            [215, 48, 39]
        ]

        # Layer 1: Heatmap (Utama)
        layer_heatmap = pdk.Layer(
            "HeatmapLayer",
            data=df,
            get_position=["lon", "lat"],
            opacity=0.8,
            radiusPixels=40,  # Ukuran pendaran (semakin besar semakin menyatu)
            colorRange=COLOR_RANGE,
            threshold=0.05,
            pickable=True,
        )
        
        # Layer 2: Scatter (Titik Detail - Opsional)
        layer_scatter = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["lon", "lat"],
            get_color="[255, 255, 255, 150]", # Putih transparan
            get_radius=30,
            pickable=True,
        )

        # View State (Kamera Tegak Lurus / 2D)
        view_state = pdk.ViewState(
            latitude=34.05,
            longitude=-118.24,
            zoom=9.5,
            pitch=0,  # 0 Derajat = Datar (seperti peta biasa)
            bearing=0
        )

        # Tooltip
        tooltip = {
            "html": "<b>Area:</b> {area_name}<br/><b>Kejahatan:</b> {crm_cd_desc}<br/><b>Koordinat:</b> {lat}, {lon}"
        }

        # Render Deck
        # map_style='dark' akan otomatis memuat basemap Carto Dark (Gratis)
        r = pdk.Deck(
            layers=[layer_heatmap], # Default cuma heatmap
            initial_view_state=view_state,
            map_style="light", 
            tooltip=tooltip
        )
        
        # Kontrol Overlay
        c_map_main, c_map_ctrl = st.columns([3, 1])
        
        with c_map_ctrl:
            st.markdown("#### Kontrol Peta")
            show_points = st.checkbox("Tampilkan Titik Kasus", value=False, help="Menampilkan titik putih di pusat kejadian")
            if show_points:
                r.layers.append(layer_scatter)
                
        with c_map_main:
            st.pydeck_chart(r)

    # TAB 2: GRAFIK
    with tab2:
        c_chart1, c_chart2 = st.columns(2)
        
        with c_chart1:
            st.subheader("Tren Harian")
            if not df.empty:
                daily = df.groupby('date_occ').size().reset_index(name='Jumlah')
                fig_line = px.area(daily, x='date_occ', y='Jumlah', template="plotly_dark")
                fig_line.update_layout(height=350)
                st.plotly_chart(fig_line, use_container_width=True)
            
        with c_chart2:
            st.subheader("Proporsi Status Kasus")
            if not df.empty:
                status_cnt = df['status_desc'].value_counts().reset_index()
                status_cnt.columns = ['Status', 'Jumlah']
                fig_pie = px.pie(status_cnt, values='Jumlah', names='Status', hole=0.5, template="plotly_dark")
                fig_pie.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig_pie, use_container_width=True)
            
        st.subheader("Analisis Senjata vs Kejahatan")
        if 'weapon_desc' in df.columns and not df.empty:
            matriks = pd.crosstab(df['crm_cd_desc'], df['weapon_desc'])
            # Top 10 interaction
            top_crime_idx = matriks.sum(axis=1).sort_values(ascending=False).head(10).index
            top_weapon_idx = matriks.sum(axis=0).sort_values(ascending=False).head(10).index
            matriks_filtered = matriks.loc[top_crime_idx, top_weapon_idx]
            
            fig_heat = px.imshow(matriks_filtered, text_auto=True, aspect="auto", color_continuous_scale="Viridis", template="plotly_dark")
            st.plotly_chart(fig_heat, use_container_width=True)

    # TAB 3: DATA MENTAH
    with tab3:
        st.dataframe(df.sort_values(by='date_occ', ascending=False), use_container_width=True)

if __name__ == "__main__":
    main()