import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import duckdb
import joblib
from prophet import Prophet
from minio import Minio
from io import BytesIO

# --- 1. KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="LAPD Executive Command Center",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. ENGINE KONEKSI (DuckDB + MinIO) ---
@st.cache_resource
def get_minio_client():
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

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

# --- 3. LOAD MODEL AI (FORECASTING) ---
@st.cache_data(ttl=3600)
def load_forecast_model():
    client = get_minio_client()
    try:
        # Cek apakah model ada
        client.stat_object("crime-models", "prophet_crime_v1.joblib")
        
        # Download model
        response = client.get_object("crime-models", "prophet_crime_v1.joblib")
        m = joblib.load(BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        # Lakukan Prediksi (30 Hari ke Depan)
        future = m.make_future_dataframe(periods=30)
        forecast = m.predict(future)
        
        # Ambil 90 hari terakhir (60 hari history + 30 hari prediksi) agar grafik enak dilihat
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(90)
    except Exception:
        return pd.DataFrame()

# --- 4. DATA LOADER (Metadata) ---
def load_filter_options(con):
    try:
        dates = con.execute("SELECT min(date_occ), max(date_occ) FROM read_parquet('s3://crime-gold/fact_crime.parquet')").fetchone()
        areas = con.execute("SELECT DISTINCT area_name FROM read_parquet('s3://crime-gold/dim_area.parquet') ORDER BY 1").df()
        crimes = con.execute("SELECT DISTINCT crm_cd_desc FROM read_parquet('s3://crime-gold/dim_crime.parquet') ORDER BY 1").df()
        return dates, areas['area_name'].tolist(), crimes['crm_cd_desc'].tolist()
    except Exception:
        return None, [], []

# --- 5. DATA ENGINE (Query Utama) ---
# A. Query Detail (Limit 50k untuk Peta)
def query_analytical_data(con, start_date, end_date, selected_areas, selected_crimes):
    where_clauses = [f"f.date_occ BETWEEN '{start_date}' AND '{end_date}'"]
    
    if selected_areas:
        areas_str = "', '".join(selected_areas)
        where_clauses.append(f"da.area_name IN ('{areas_str}')")
        
    if selected_crimes:
        safe_crimes = [c.replace("'", "''") for c in selected_crimes]
        crimes_str = "', '".join(safe_crimes)
        where_clauses.append(f"dc.crm_cd_desc IN ('{crimes_str}')")

    where_stmt = " AND ".join(where_clauses)

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
        LIMIT 5000000
    """
    return con.execute(sql).df()

# B. Query Tren (Full History)
def query_trend_data(con, start_date, end_date, selected_areas, selected_crimes):
    where_clauses = [f"f.date_occ BETWEEN '{start_date}' AND '{end_date}'"]
    if selected_areas:
        areas_str = "', '".join(selected_areas)
        where_clauses.append(f"da.area_name IN ('{areas_str}')")
    if selected_crimes:
        safe_crimes = [c.replace("'", "''") for c in selected_crimes]
        crimes_str = "', '".join(safe_crimes)
        where_clauses.append(f"dc.crm_cd_desc IN ('{crimes_str}')")
    
    where_stmt = " AND ".join(where_clauses)
    
    sql = f"""
        SELECT f.date_occ, count(*) as Jumlah
        FROM read_parquet('s3://crime-gold/fact_crime.parquet') f
        LEFT JOIN read_parquet('s3://crime-gold/dim_area.parquet') da ON f.area_id = da.area_id
        LEFT JOIN read_parquet('s3://crime-gold/dim_crime.parquet') dc ON f.crm_cd = dc.crm_cd
        WHERE {where_stmt}
        GROUP BY f.date_occ
        ORDER BY f.date_occ
    """
    return con.execute(sql).df()

def query_kpi_stats(con, start_date, end_date):
    sql = f"""
        SELECT count(*) as total_cases, avg(vict_age) as avg_age
        FROM read_parquet('s3://crime-gold/fact_crime.parquet')
        WHERE date_occ BETWEEN '{start_date}' AND '{end_date}'
    """
    return con.execute(sql).df()

# --- 6. UI UTAMA ---
def main():
    con = get_db_connection()
    
    st.sidebar.title("🔍 Filter Panel")
    dates, area_opts, crime_opts = load_filter_options(con)
    
    if not dates or not dates[0]:
        st.error("Gagal terhubung ke Data Warehouse.")
        st.stop()

    min_d, max_d = dates
    s_date, e_date = st.sidebar.date_input("Rentang Waktu", [min_d, max_d])
    sel_areas = st.sidebar.multiselect("Wilayah (Area)", area_opts)
    sel_crimes = st.sidebar.multiselect("Jenis Kejahatan", crime_opts)
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 Tips: Gunakan mode 'Light' atau 'Dark' di Settings.")

    st.title("🚔 LAPD Crime Intelligence")
    st.caption(f"Data Analisis Periode: {s_date} s/d {e_date}")

    with st.spinner("Processing data..."):
        df = query_analytical_data(con, s_date, e_date, sel_areas, sel_crimes)
        df_trend = query_trend_data(con, s_date, e_date, sel_areas, sel_crimes)
        kpi_df = query_kpi_stats(con, s_date, e_date)
        forecast_df = load_forecast_model() # Load Prediksi

    if df.empty:
        st.warning("Tidak ada data.")
        return

    # KPI
    col1, col2, col3, col4 = st.columns(4)
    total_cases = kpi_df['total_cases'][0]
    col1.metric("Total Kasus", f"{total_cases:,}")
    
    top_crime = df['crm_cd_desc'].mode()[0] if not df.empty else "-"
    col2.metric("Top Kejahatan", top_crime[:15] + "..." if len(top_crime)>15 else top_crime)
    
    top_area = df['area_name'].mode()[0] if not df.empty else "-"
    col3.metric("Area Hotspot", top_area)
    
    avg_age = round(kpi_df['avg_age'][0] or 0)
    col4.metric("Avg Umur", f"{avg_age} Thn")

    st.markdown("---")

    # --- TABS LAYOUT (Menambahkan Tab AI) ---
    tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Peta", "📈 Grafik", "📋 Data", "🤖 AI Forecast"])

    # TAB 1: PETA
    with tab1:
        st.subheader("Peta Konsentrasi Kriminalitas")
        COLOR_RANGE = [
            [65, 182, 196], [127, 205, 187], [199, 233, 180],
            [237, 248, 177], [254, 224, 139], [253, 174, 97],
            [244, 109, 67], [215, 48, 39]
        ]
        layer_heatmap = pdk.Layer(
            "HeatmapLayer",
            data=df,
            get_position=["lon", "lat"],
            opacity=0.8,
            radiusPixels=40,
            colorRange=COLOR_RANGE,
            threshold=0.05,
            pickable=True,
        )
        layer_scatter = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["lon", "lat"],
            get_color="[255, 255, 255, 150]",
            get_radius=30,
            pickable=True,
        )
        view_state = pdk.ViewState(latitude=34.05, longitude=-118.24, zoom=9.5, pitch=0)
        tooltip = {"html": "<b>Area:</b> {area_name}<br/><b>Kejahatan:</b> {crm_cd_desc}"}
        r = pdk.Deck(layers=[layer_heatmap], initial_view_state=view_state, map_style="light", tooltip=tooltip)
        
        c_map_main, c_map_ctrl = st.columns([3, 1])
        with c_map_ctrl:
            show_points = st.checkbox("Tampilkan Titik Kasus", value=False)
            if show_points: r.layers.append(layer_scatter)
        with c_map_main:
            st.pydeck_chart(r)

    # TAB 2: GRAFIK
    with tab2:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("Tren Harian (Full History)")
            if not df_trend.empty:
                fig_line = px.area(df_trend, x='date_occ', y='Jumlah', template="plotly_dark")
                fig_line.update_layout(height=350)
                st.plotly_chart(fig_line, use_container_width=True)
        with c2:
            st.subheader("Proporsi Status")
            if not df.empty:
                status_cnt = df['status_desc'].value_counts().reset_index()
                status_cnt.columns = ['Status', 'Jumlah']
                fig_pie = px.pie(status_cnt, values='Jumlah', names='Status', hole=0.5, template="plotly_dark")
                fig_pie.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        st.subheader("Analisis Senjata")
        if 'weapon_desc' in df.columns:
            matriks = pd.crosstab(df['crm_cd_desc'], df['weapon_desc'])
            top_c = matriks.sum(axis=1).sort_values(ascending=False).head(10).index
            top_w = matriks.sum(axis=0).sort_values(ascending=False).head(10).index
            fig_heat = px.imshow(matriks.loc[top_c, top_w], text_auto=True, aspect="auto", color_continuous_scale="Viridis", template="plotly_dark")
            st.plotly_chart(fig_heat, use_container_width=True)

    # TAB 3: DATA
    with tab3:
        st.dataframe(df.sort_values(by='date_occ', ascending=False), use_container_width=True)

    # [BARU] TAB 4: AI FORECAST
    with tab4:
        st.subheader("🤖 Prediksi Kriminalitas (30 Hari Kedepan)")
        
        if not forecast_df.empty:
            # Gabungkan data historis terakhir (real) dengan prediksi
            # agar grafiknya nyambung
            
            # Plotting dengan Plotly
            fig_forecast = px.line(forecast_df, x='ds', y='yhat', labels={'ds': 'Tanggal', 'yhat': 'Prediksi Jumlah Kasus'}, template="plotly_dark")
            
            # Tambahkan Confidence Interval (Area Ketidakpastian)
            fig_forecast.add_scatter(
                x=forecast_df['ds'], y=forecast_df['yhat_upper'], mode='lines', 
                line=dict(width=0), showlegend=False, name='Upper Bound'
            )
            fig_forecast.add_scatter(
                x=forecast_df['ds'], y=forecast_df['yhat_lower'], mode='lines', 
                line=dict(width=0), fill='tonexty', fillcolor='rgba(0, 173, 181, 0.2)', 
                showlegend=False, name='Lower Bound'
            )
            
            fig_forecast.update_layout(title="Forecast Model Prophet (Meta AI)")
            st.plotly_chart(fig_forecast, use_container_width=True)
            
            st.info("ℹ️ **Info:** Grafik ini dihasilkan oleh model Machine Learning (Prophet) yang dilatih secara otomatis setiap hari. Area arsiran menunjukkan tingkat kepercayaan model.")
        else:
            st.warning("Model prediksi belum tersedia. Pastikan DAG '3_train_forecasting_model' sudah dijalankan di Airflow.")

if __name__ == "__main__":
    main()