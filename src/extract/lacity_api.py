import pandas as pd
from sodapy import Socrata
from minio import Minio
from io import BytesIO
import json
from datetime import datetime
import os

def fetch_and_upload_crime_data(**kwargs):
    # 1. Konfigurasi Koneksi (Biasanya ditaruh di .env, tapi kita hardcode dulu untuk tes)
    MINIO_ENDPOINT = "minio:9000"  # Hostname internal docker
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    BUCKET_NAME = "crime-bronze"
    
    # 2. Tarik Data dari API LAPD (Socrata)
    print("Mengubungi API LAPD...")
    client = Socrata("data.lacity.org", None) # App Token None (Public access limits apply)
    
    # Mengambil 2000 data terbaru (Limit agar cepat saat testing)
    results = client.get("2nrs-mtv8", limit=2000, order="date_occ DESC")
    
    if not results:
        print("Tidak ada data yang ditemukan.")
        return

    # 3. Konversi ke JSON Bytes
    print(f"Berhasil menarik {len(results)} data.")
    data_bytes = json.dumps(results).encode('utf-8')
    data_stream = BytesIO(data_bytes)
    
    # 4. Upload ke MinIO (Data Lake Layer: Bronze)
    client_minio = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    
    # Nama file unik berdasarkan tanggal hari ini
    today_str = datetime.now().strftime("%Y-%m-%d")
    object_name = f"raw_crime_{today_str}.json"
    
    # Cek apakah bucket ada (Safe guard)
    if not client_minio.bucket_exists(BUCKET_NAME):
        client_minio.make_bucket(BUCKET_NAME)
    
    print(f"Mengupload {object_name} ke bucket {BUCKET_NAME}...")
    client_minio.put_object(
        BUCKET_NAME,
        object_name,
        data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print("Upload Selesai!")

if __name__ == "__main__":
    # Untuk testing manual lewat terminal (python src/extract/lacity_api.py)
    fetch_and_upload_crime_data()