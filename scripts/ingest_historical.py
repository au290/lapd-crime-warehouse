import pandas as pd
from minio import Minio
from io import BytesIO
import json
import os

# --- KONFIGURASI ---
LOCAL_CSV_PATH = "data_historis.csv"  # Taruh file CSV Anda di root project folder
MINIO_BUCKET = "crime-bronze"

def upload_historical_data():
    # 1. Cek File Lokal
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"Error: File {LOCAL_CSV_PATH} tidak ditemukan. Harap taruh file CSV di folder root.")
        return

    print("Membaca file CSV historis (ini mungkin memakan waktu)...")
    # Pastikan nama kolom di CSV sesuai dengan yang diharapkan kode transformation
    # Misalnya: 'DATE OCC', 'AREA NAME', dll.
    df = pd.read_csv(LOCAL_CSV_PATH)
    
    # 2. Konversi ke Format API (JSON records)
    # Pipeline kita mengharapkan list of dictionaries (JSON)
    print("Mengkonversi ke JSON...")
    
    # Kita pecah per tahun atau bulan agar tidak terlalu besar (Opsional, disini kita upload full dulu)
    records = df.to_dict(orient='records')
    json_bytes = json.dumps(records).encode('utf-8')
    data_stream = BytesIO(json_bytes)

    # 3. Upload ke MinIO Bronze
    client = Minio(
        "localhost:9000", # Gunakan localhost jika dijalankan dari luar Docker (Terminal Windows)
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Beri nama file khusus agar script transform membacanya
    # Trik: Kita namakan sesuai tanggal hari ini agar pipeline yang sudah ada langsung memprosesnya
    from datetime import datetime
    today_str = datetime.now().strftime("%Y-%m-%d")
    object_name = f"raw_crime_{today_str}.json" 
    
    print(f"Mengupload {object_name} ({len(records)} baris) ke {MINIO_BUCKET}...")
    
    client.put_object(
        MINIO_BUCKET,
        object_name,
        data_stream,
        length=len(json_bytes),
        content_type="application/json"
    )
    print("✅ Upload Sukses! Silakan trigger DAG Airflow sekarang.")

if __name__ == "__main__":
    upload_historical_data()