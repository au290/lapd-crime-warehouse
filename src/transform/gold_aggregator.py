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
    
    print("🔄 Memulai Proses Gold Layer (Fact Table Generation)...")
    
    # 2. SCAN SEMUA FILE SILVER
    objects = client.list_objects(SOURCE_BUCKET, recursive=True)
    all_dfs = []
    
    for obj in objects:
        if obj.object_name.endswith('.parquet'):
            try:
                response = client.get_object(SOURCE_BUCKET, obj.object_name)
                df_chunk = pd.read_parquet(BytesIO(response.read()))
                all_dfs.append(df_chunk)
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"⚠️ Gagal baca {obj.object_name}")

    if not all_dfs:
        print("❌ Tidak ada data.")
        return

    # 3. GABUNG DATA
    full_df = pd.concat(all_dfs, ignore_index=True)
    print(f"📦 Total Raw Data: {len(full_df)}")

    # 4. DEDUPLIKASI (PENTING!)
    if 'dr_no' in full_df.columns:
        full_df = full_df.drop_duplicates(subset=['dr_no'], keep='last')
        print(f"✨ Total Unik: {len(full_df)}")

    # 5. DATA CLEANUP UNTUK DASHBOARD
    # Filter data yang koordinatnya 0 (Null Island) agar peta tidak rusak
    if 'lat' in full_df.columns and 'lon' in full_df.columns:
        full_df = full_df[(full_df['lat'] != 0) & (full_df['lon'] != 0)]
    
    # Pastikan tanggal formatnya benar
    if 'date_occ' in full_df.columns:
        full_df['date_occ'] = pd.to_datetime(full_df['date_occ'])

    # 6. SIMPAN MASTER FACT TABLE (CSV)
    # Kita simpan CSV lengkap agar Dashboard bisa melakukan filtering bebas
    dest_filename = "master_crime_fact_table.csv"
    
    csv_buffer = BytesIO()
    full_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    print(f"🚀 Menyimpan Fact Table ke {dest_filename}...")
    client.put_object(
        DEST_BUCKET,
        dest_filename,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )
    print("✅ Gold Layer Selesai!")

if __name__ == "__main__":
    aggregate_crime_by_area()