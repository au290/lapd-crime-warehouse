import pandas as pd
from minio import Minio
from io import BytesIO
import json
from datetime import datetime

def clean_and_load_to_silver(**kwargs):
    # 1. Konfigurasi
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    SOURCE_BUCKET = "crime-bronze"
    DEST_BUCKET = "crime-silver"
    
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    # 2. Cari File Terbaru di Bronze
    today_str = datetime.now().strftime("%Y-%m-%d")
    target_file = f"raw_crime_{today_str}.json"
    
    print(f"Mencari file {target_file} di {SOURCE_BUCKET}...")
    try:
        response = client.get_object(SOURCE_BUCKET, target_file)
        data = json.loads(response.read())
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"Gagal membaca file atau file tidak ditemukan: {e}")
        return

    # 3. Transformasi dengan Pandas
    df = pd.DataFrame(data)
    print(f"Data mentah dimuat: {len(df)} baris.")

    # --- CLEANING RULES (VERSI ROBUST) ---
    
    # A. Tanggal: Pastikan format datetime
    date_cols = ['date_occ', 'date_rptd']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # B. Numerik: Paksa jadi angka, yang error jadi NaN
    numeric_cols = ['lat', 'lon', 'vict_age', 'dr_no']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # C. [PENTING - FIX ERROR PARQUET] 
    # Paksa semua kolom sisa (Object) menjadi STRING murni.
    # Ini mencegah error "ArrowInvalid" karena tipe data campuran.
    for col in df.columns:
        if col not in date_cols and col not in numeric_cols:
            df[col] = df[col].astype(str)

    print("Data berhasil dibersihkan dan distandarisasi.")

    # 4. Simpan ke Silver (Format Parquet)
    parquet_buffer = BytesIO()
    try:
        # Engine pyarrow wajib terinstall
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    except Exception as e:
        print(f"CRITICAL ERROR saat convert Parquet: {e}")
        # Print tipe data untuk debugging jika masih gagal
        print(df.dtypes)
        raise e

    parquet_buffer.seek(0)
    
    dest_filename = f"clean_crime_{today_str}.parquet"
    
    # Cek bucket silver, buat jika belum ada
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    print(f"Menyimpan {dest_filename} ke {DEST_BUCKET}...")
    client.put_object(
        DEST_BUCKET,
        dest_filename,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print("Transformasi ke Silver Selesai!")

if __name__ == "__main__":
    clean_and_load_to_silver()