import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import pydeck as pdk

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Crime Intel", 
    layout="wide", 
    page_icon="🚔",
    initial_sidebar_state="expanded"
)

# --- INIT DUCKDB & MINIO ---
@st.cache_resource
def get_db_connection():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='minio:9000';
        SET s3_use_ssl=false;
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_url_style='path';
    """)
    return con

# --- LOAD METADATA ---
def get_filter_options(con):
    try:
        # Ambil range tanggal
        dates = con.execute("""
            SELECT min(date_occ), max(date_occ) 
            FROM read_parquet('s3://crime-gold/fact_crime.parquet')
        """).fetchone()
        
        # Ambil list area
        areas = con.execute("""
            SELECT DISTINCT area_name 
            FROM read_parquet('s3://crime-gold/dim_area.parquet') 
            ORDER BY 1
        """).df()
        
        # Ambil list kategori kejahatan
        crimes = con.execute("""
            SELECT DISTINCT crm_cd_desc 
            FROM read_parquet('s3://crime-gold/dim_crime.parquet') 
            ORDER BY 1
        """).df()
        
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist()
    except Exception:
        return None, [], []

# --- QUERY OLAP ---
def query_data(con, start_date, end_date, selected_areas, selected_crimes):
    if not selected_areas:
        area_filter = "1=1"
    else:
        areas_str = "', '".join(selected_areas)
        area_filter = f"da.area_name IN ('{areas_str}')"

    if not selected_crimes:
        crime_filter = "1=1"
    else:
        safe_crimes = [c.replace("'", "''") for c in selected_crimes]
        crimes_str = "', '".join(safe_crimes)
        crime_filter = f"dc.crm_cd_desc IN ('{crimes_str}')"

    # Query dioptimalkan untuk PyDeck (Butuh lat, lon, dan info warna)
    query = f"""
        SELECT 
            f.dr_no, 
            f.date_occ, 
            da.area_name, 
            dc.crm_cd_desc, 
            ds.status_desc, 
            dw.weapon_desc,
            dp.premis_desc,
            f.vict_age, 
            f.lat, 
            f.lon
        FROM read_parquet('s3://crime-gold/fact_crime.parquet') f
        LEFT JOIN read_parquet('s3://crime-gold/dim_area.parquet') da ON f.area_id = da.area_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_crime.parquet') dc ON f.crm_cd = dc.crm_cd
        LEFT JOIN read_parquet('s3://crime-gold/dim_status.parquet') ds ON f.status_id = ds.status_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_weapon.parquet') dw ON f.weapon_id = dw.weapon_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_premis.parquet') dp ON f.premis_id = dp.premis_id
        WHERE f.date_occ BETWEEN '{start_date}' AND '{end_date}'
          AND {area_filter}
          AND {crime_filter}
          AND f.lat != 0 AND f.lon != 0
    """
    return con.execute(query).df()

# --- MAIN APP ---
def main():
    st.title("🚔 LAPD Crime Intelligence (WebGL Accelerated)")
    
    con = get_db_connection()
    
    # --- SIDEBAR ---
    st.sidebar.header("🔍 Filter Dashboard")
    dates, area_opts, crime_opts = get_filter_options(con)
    
    if not dates or not dates[0]:
        st.error("Data Warehouse tidak merespons. Cek MinIO.")
        st.stop()

    min_d, max_d = dates
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [min_d, max_d])
    sel_areas = st.sidebar.multiselect("Pilih Wilayah", area_opts)
    sel_crimes = st.sidebar.multiselect("Jenis Kejahatan", crime_opts)

    with st.spinner("Processing massive dataset..."):
        df = query_data(con, s_date, e_date, sel_areas, sel_crimes)

    # --- KPI ROW ---
    st.markdown("### 📊 Ringkasan Eksekutif")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Kasus", f"{len(df):,}")
    
    top_c = df['crm_cd_desc'].mode()[0] if not df.empty else "-"
    k2.metric("Kejahatan #1", top_c[:15] + "..." if len(top_c)>15 else top_c)
    
    top_w = df['weapon_desc'].mode()[0] if not df.empty else "-"
    k3.metric("Senjata #1", top_w[:15] + "..." if len(top_w)>15 else top_w)
    
    avg_age = round(df['vict_age'].mean()) if not df.empty else 0
    k4.metric("Avg Umur", f"{avg_age} Thn")
    
    st.divider()

    # --- PETA PYDECK (SOLUSI OOM) ---
    st.subheader("🗺️ Peta Geospasial Interaktif")
    
    c_map1, c_map2 = st.columns([1, 4])
    with c_map1:
        map_style = st.radio("Mode Visualisasi:", ["3D Hexagon (Agregat)", "Scatter (Titik)"])
        st.info("💡 **Hexagon** lebih ringan untuk data besar. **Scatter** untuk melihat detail lokasi.")

    with c_map2:
        if df.empty:
            st.warning("Data kosong.")
        else:
            # 1. Tentukan View State (Posisi Awal Kamera)
            view_state = pdk.ViewState(
                latitude=34.05,
                longitude=-118.24,
                zoom=9,
                pitch=45 if map_style == "3D Hexagon (Agregat)" else 0, # Miringkan kamera kalau 3D
            )

            # 2. Tentukan Layer
            layers = []
            
            if map_style == "3D Hexagon (Agregat)":
                # Layer Hexagon (Sangat Efisien & Profesional)
                layer = pdk.Layer(
                    "HexagonLayer",
                    data=df,
                    get_position=["lon", "lat"],
                    radius=200, # Ukuran hexagon (meter)
                    elevation_scale=50,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True, # Biar jadi 3D
                )
                layers.append(layer)
                tooltip = {"html": "<b>Jumlah Kasus:</b> {elevationValue}"}
                
            else:
                # Layer Scatter (Titik Biasa tapi pakai GPU)
                # Jauh lebih kuat daripada Plotly
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=df,
                    get_position=["lon", "lat"],
                    get_color="[200, 30, 0, 160]", # Merah transparan
                    get_radius=100,
                    pickable=True,
                )
                layers.append(layer)
                tooltip = {
                    "html": "<b>Kejahatan:</b> {crm_cd_desc}<br/><b>Area:</b> {area_name}<br/><b>Senjata:</b> {weapon_desc}"
                }

            # 3. Render Peta
            st.pydeck_chart(pdk.Deck(
                map_style="mapbox://styles/mapbox/dark-v10", # Dark mode
                initial_view_state=view_state,
                layers=layers,
                tooltip=tooltip
            ))

    # --- CHARTS ROW ---
    st.divider()
    c_chart1, c_chart2 = st.columns(2)
    
    with c_chart1:
        st.subheader("📈 Tren Harian")
        if not df.empty:
            daily = df.groupby('date_occ').size().reset_index(name='Jumlah')
            fig = px.line(daily, x='date_occ', y='Jumlah', template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)
            
    with c_chart2:
        st.subheader("⚠️ Top 10 Kejahatan")
        if not df.empty:
            top_crimes = df['crm_cd_desc'].value_counts().head(10).reset_index()
            top_crimes.columns = ['Kejahatan', 'Jumlah']
            fig = px.bar(top_crimes, x='Jumlah', y='Kejahatan', orientation='h', color='Jumlah', color_continuous_scale='Reds')
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()