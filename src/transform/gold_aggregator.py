import pandas as pd
from minio import Minio
from io import BytesIO
import os
from datetime import datetime

def aggregate_crime_by_area(**kwargs):
    # 1. Konfigurasi
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    SOURCE_BUCKET = "crime-silver"
    DEST_BUCKET = "crime-gold"
    
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    print("🔄 Memulai Transformasi EXTENDED STAR SCHEMA (6 Dimensi)...")
    
    # 2. LOAD DATA
    objects = client.list_objects(SOURCE_BUCKET, recursive=True)
    all_dfs = []
    for obj in objects:
        if obj.object_name.endswith('.parquet'):
            try:
                response = client.get_object(SOURCE_BUCKET, obj.object_name)
                all_dfs.append(pd.read_parquet(BytesIO(response.read())))
                response.close()
                response.release_conn()
            except: pass

    if not all_dfs: 
        print("❌ Tidak ada data.")
        return

    full_df = pd.concat(all_dfs, ignore_index=True)
    if 'dr_no' in full_df.columns:
        full_df = full_df.drop_duplicates(subset=['dr_no'], keep='last')

    print(f"📦 Data Bersih: {len(full_df)} baris")

    # ==========================================
    # PEMBUATAN 6 TABEL DIMENSI
    # ==========================================

    def create_dim(df, id_col, name_col):
        if id_col in df.columns and name_col in df.columns:
            dim = df[[id_col, name_col]].drop_duplicates().dropna()
            # Hapus data dimana ID-nya 'Unknown' atau error
            dim = dim[dim[id_col] != 'Unknown']
            return dim.sort_values(id_col)
        return pd.DataFrame()

    # 1. DIM_AREA
    dim_area = create_dim(full_df, 'area_id', 'area_name')
    
    # 2. DIM_CRIME
    dim_crime = create_dim(full_df, 'crm_cd', 'crm_cd_desc')
    
    # 3. DIM_STATUS
    dim_status = create_dim(full_df, 'status_id', 'status_desc')
    
    # 4. DIM_WEAPON
    dim_weapon = create_dim(full_df, 'weapon_id', 'weapon_desc')
    
    # 5. DIM_PREMIS (Lokasi)
    dim_premis = create_dim(full_df, 'premis_id', 'premis_desc')

    # 6. DIM_CALENDAR (Generated)
    if 'date_occ' in full_df.columns:
        min_date = full_df['date_occ'].min()
        max_date = full_df['date_occ'].max()
        # Handle jika date range valid
        if pd.notnull(min_date) and pd.notnull(max_date):
            date_range = pd.date_range(start=min_date, end=max_date)
            dim_calendar = pd.DataFrame({
                'date_id': date_range, 
                'year': date_range.year,
                'month': date_range.month,
                'day': date_range.day,
                'day_name': date_range.day_name(),
                'is_weekend': date_range.weekday >= 5
            })
        else:
            dim_calendar = pd.DataFrame()
    else:
        dim_calendar = pd.DataFrame()

    # ==========================================
    # PEMBUATAN FACT TABLE
    # ==========================================
    
    # Fact Table hanya menyimpan ID (Foreign Keys) dan Measures
    fact_cols = ['dr_no', 'date_occ', 'area_id', 'crm_cd', 'status_id', 'weapon_id', 'premis_id', 'vict_age', 'lat', 'lon']
    existing_cols = [c for c in fact_cols if c in full_df.columns]
    
    fact_crime = full_df[existing_cols]
    
    if 'date_occ' in fact_crime.columns:
        fact_crime['date_occ'] = pd.to_datetime(fact_crime['date_occ'])

    # ==========================================
    # UPLOAD SEMUA TABEL
    # ==========================================
    
    if not client.bucket_exists(DEST_BUCKET): client.make_bucket(DEST_BUCKET)

    def upload(df, name):
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        client.put_object(DEST_BUCKET, name, buf, length=buf.getbuffer().nbytes, content_type="text/csv")
        print(f"🚀 {name}: {len(df)} rows")

    upload(dim_area, "dim_area.csv")
    upload(dim_crime, "dim_crime.csv")
    upload(dim_status, "dim_status.csv")
    upload(dim_weapon, "dim_weapon.csv")
    upload(dim_premis, "dim_premis.csv")
    upload(dim_calendar, "dim_calendar.csv")
    upload(fact_crime, "fact_crime.csv")
    
    print("🏁 6-Dimension Star Schema Completed!")

if __name__ == "__main__":
    aggregate_crime_by_area()