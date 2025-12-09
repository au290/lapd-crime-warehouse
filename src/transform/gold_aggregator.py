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
    
    print("🔄 Memulai Aggregasi Full Refresh...")
    
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

    # 3. GABUNG & DEDUPLIKASI
    full_df = pd.concat(all_dfs, ignore_index=True)
    print(f"📦 Total Raw: {len(full_df)}")

    # [FIX] Hapus duplikat berdasarkan ID Kasus (dr_no)
    if 'dr_no' in full_df.columns:
        full_df = full_df.drop_duplicates(subset=['dr_no'], keep='last')
        print(f"✨ Total Bersih (Unik): {len(full_df)}")

    # 4. AGREGASI
    if 'area_name' in full_df.columns:
        agg_df = full_df.groupby('area_name').size().reset_index(name='total_crimes')
        agg_df = agg_df.sort_values(by='total_crimes', ascending=False)
    else:
        print("❌ Kolom area_name hilang.")
        return

    # 5. SIMPAN MASTER FILE
    # Gunakan satu nama file master
    dest_filename = "master_crime_summary.csv"
    
    csv_buffer = BytesIO()
    agg_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    client.put_object(DEST_BUCKET, dest_filename, csv_buffer, length=csv_buffer.getbuffer().nbytes, content_type="text/csv")
    print("✅ Gold Layer Updated!")

if __name__ == "__main__":
    aggregate_crime_by_area()