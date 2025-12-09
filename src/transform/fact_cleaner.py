import pandas as pd
from minio import Minio
from io import BytesIO
import json
import os
import sys
from datetime import datetime

# --- IMPORT VALIDATOR ---
# Menambahkan path root agar bisa import module governance
sys.path.append('/opt/airflow')
# Import logic validator yang baru kita buat
try:
    from governance.quality_checks.logic_validation import check_business_logic
except ImportError:
    # Fallback biar tidak crash kalau file logic belum ada
    print("⚠️ Warning: Modul logic_validation tidak ditemukan. Logic check akan dilewati.")
    def check_business_logic(df): return df

# --- FUNGSI BACA KONTRAK GOVERNANCE ---
def load_schema_contract():
    schema_path = "/opt/airflow/governance/contracts/crime_schema.json"
    if not os.path.exists(schema_path):
        print("⚠️ Warning: File contract tidak ditemukan. Menggunakan logic default.")
        return None
    with open(schema_path, 'r') as f:
        return json.load(f)

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
    
    print(f"📥 Mengambil file {target_file}...")
    try:
        response = client.get_object(SOURCE_BUCKET, target_file)
        data = json.loads(response.read())
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"❌ Error: File tidak ditemukan. {e}")
        raise e 

    # 3. Load ke Pandas
    df = pd.DataFrame(data)
    print(f"📊 Data mentah: {len(df)} baris.")

    # --- PENERAPAN SCHEMA GOVERNANCE (LAYER 1) ---
    contract = load_schema_contract()
    
    if contract:
        print("🛡️ Menerapkan Governance Contract (Schema)...")
        
        # A. Rename Kolom (Sesuai JSON)
        if "column_mapping" in contract:
             df.rename(columns=contract["column_mapping"], inplace=True)
             # Support juga untuk header uppercase (CSV Historis)
             upper_mapping = {k.upper(): v for k, v in contract["column_mapping"].items()}
             df.rename(columns=upper_mapping, inplace=True)

        # B. Validasi Kolom Wajib
        missing_cols = [c for c in contract["required_columns"] if c not in df.columns]
        if missing_cols:
            error_msg = f"⛔ DATA DITOLAK! Governance Check Gagal. Kolom hilang: {missing_cols}"
            print(error_msg)
            raise ValueError(error_msg)
            
        print("✅ Struktur data valid sesuai kontrak.")
    else:
        # Fallback Default
        df.columns = df.columns.str.lower().str.replace(' ', '_')

    # --- CLEANING TIPE DATA (LAYER 2) ---
    # 1. Tanggal
    for col in df.columns:
        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 2. Numerik
    numeric_cols = ['lat', 'lon', 'vict_age', 'dr_no']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3. String (Sisa)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    # --- LOGIC VALIDATION (LAYER 3 - BARU) ---
    # Memeriksa kebenaran logika data
    df = check_business_logic(df)
    
    # 4. Simpan ke Silver (Parquet)
    parquet_buffer = BytesIO()
    # Gunakan pyarrow engine
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    dest_filename = f"clean_crime_{today_str}.parquet"
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    print(f"🚀 Menyimpan {dest_filename} ke {DEST_BUCKET}...")
    client.put_object(DEST_BUCKET, dest_filename, parquet_buffer, length=parquet_buffer.getbuffer().nbytes)
    print("✅ Transformasi Selesai!")

if __name__ == "__main__":
    clean_and_load_to_silver()