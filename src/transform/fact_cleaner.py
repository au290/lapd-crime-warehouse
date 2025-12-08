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
    
    # 2. Cari File Terbaru di Bronze (Logika sederhana: ambil file hari ini)
    today_str = datetime.now().strftime("%Y-%m-%d")
    target_file = f"raw_crime_{today_str}.json"
    
    print(f"Mencari file {target_file} di {SOURCE_BUCKET}...")
    try:
        response = client.get_object(SOURCE_BUCKET, target_file)
        data = json.loads(response.read())
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    # 3. Transformasi dengan Pandas
    df = pd.DataFrame(data)
    print(f"Data mentah dimuat: {len(df)} baris.")

    # --- CLEANING RULES ---
    # Convert format tanggal yang aneh dari API ("2023-01-01T00:00:00.000") menjadi datetime murni
    if 'date_occ' in df.columns:
        df['date_occ'] = pd.to_datetime(df['date_occ'])
    
    if 'date_rptd' in df.columns:
        df['date_rptd'] = pd.to_datetime(df['date_rptd'])

    # Pastikan Kolom Numerik benar-benar angka (bukan string)
    numeric_cols = ['lat', 'lon', 'vict_age']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') # ubah error jadi NaN

    # Filter Data Sampah (Data Governance Sederhana)
    # Contoh: Hapus data jika tidak ada nomor kasus (dr_no)
    if 'dr_no' in df.columns:
        df = df.dropna(subset=['dr_no'])

    print("Data berhasil dibersihkan.")

    # 4. Simpan ke Silver (Format Parquet)
    # Kita gunakan buffer memori, tidak perlu save file ke hardisk lokal
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    dest_filename = f"clean_crime_{today_str}.parquet"
    
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