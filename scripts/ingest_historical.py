import pandas as pd
from minio import Minio
from io import BytesIO
import json
import os

# --- KONFIGURASI ---
LOCAL_CSV_PATH = "data_historis.csv"
MINIO_BUCKET = "crime-bronze"

def upload_historical_data():
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"Error: File {LOCAL_CSV_PATH} tidak ditemukan.")
        return

    print("Membaca file CSV historis...")
    df = pd.read_csv(LOCAL_CSV_PATH)
    
    print("Mengkonversi ke JSON...")
    records = df.to_dict(orient='records')
    json_bytes = json.dumps(records).encode('utf-8')
    data_stream = BytesIO(json_bytes)

    client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

    # [FIX] Nama file statis khusus history agar tidak menimpa daily ingest
    object_name = "raw_crime_historical_master.json" 
    
    print(f"Mengupload {object_name} ({len(records)} baris) ke {MINIO_BUCKET}...")
    
    client.put_object(
        MINIO_BUCKET,
        object_name,
        data_stream,
        length=len(json_bytes),
        content_type="application/json"
    )
    print("✅ Upload Sukses! File historis aman.")

if __name__ == "__main__":
    upload_historical_data()