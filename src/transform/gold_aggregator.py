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
    
    # 2. Ambil File Parquet Hari Ini dari Silver
    today_str = datetime.now().strftime("%Y-%m-%d")
    source_file = f"clean_crime_{today_str}.parquet"
    
    print(f"Mengambil data Silver: {source_file}...")
    try:
        response = client.get_object(SOURCE_BUCKET, source_file)
        # Baca Parquet dari stream
        df = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"Gagal membaca Silver layer: {e}")
        return

    # 3. PROSES AGREGASI (LOGIKA WAREHOUSE)
    # Kita ingin tahu: "Ada berapa kejahatan di setiap area?"
    # Group By: AREA NAME
    print("Melakukan agregasi data...")
    
    # Pastikan kolom area ada
    if 'area_name' in df.columns:
        agg_df = df.groupby('area_name').size().reset_index(name='total_crimes')
        agg_df['date_processed'] = today_str # Tambahkan tanggal proses
        
        # Sortir dari yang kriminalitas tertinggi
        agg_df = agg_df.sort_values(by='total_crimes', ascending=False)
        
        print(agg_df.head()) # Preview di log
    else:
        print("Kolom area_name tidak ditemukan, skip agregasi.")
        return

    # 4. Simpan ke Gold (CSV agar mudah dibaca manusia/Excel)
    csv_buffer = BytesIO()
    agg_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    dest_filename = f"daily_crime_summary_{today_str}.csv"
    
    # Cek bucket gold
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    print(f"Menyimpan summary ke {DEST_BUCKET}...")
    client.put_object(
        DEST_BUCKET,
        dest_filename,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )
    print("Gold Layer Selesai!")

if __name__ == "__main__":
    aggregate_crime_by_area()