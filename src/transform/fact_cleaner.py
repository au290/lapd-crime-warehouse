import pandas as pd
from minio import Minio
from io import BytesIO
import json
import os
import sys
from datetime import datetime

# --- IMPORT VALIDATOR ---
sys.path.append('/opt/airflow')
try:
    from governance.quality_checks.logic_validation import check_business_logic
except ImportError:
    def check_business_logic(df): return df

# --- FUNGSI BACA KONTRAK GOVERNANCE ---
def load_schema_contract():
    schema_path = "/opt/airflow/governance/contracts/crime_schema.json"
    if not os.path.exists(schema_path):
        return None
    with open(schema_path, 'r') as f:
        return json.load(f)

def clean_and_load_to_silver(**kwargs):
    target_filename = kwargs.get('target_file')

    # 1. Konfigurasi
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    SOURCE_BUCKET = "crime-bronze"
    DEST_BUCKET = "crime-silver"
    
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    # 2. Tentukan File
    if target_filename:
        source_file = target_filename
        output_file = target_filename.replace('raw_', 'clean_').replace('.json', '.parquet')
    else:
        today_str = datetime.now().strftime("%Y-%m-%d")
        source_file = f"raw_crime_{today_str}.json"
        output_file = f"clean_crime_{today_str}.parquet"
    
    print(f"📥 Mengambil file {source_file}...")
    try:
        response = client.get_object(SOURCE_BUCKET, source_file)
        data = json.loads(response.read())
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"❌ Error: File {source_file} tidak ditemukan.")
        raise e 

    # 3. Transformasi
    df = pd.DataFrame(data)
    
    # --- GOVERNANCE SCHEMA ---
    contract = load_schema_contract()
    if contract:
        # Rename Kolom Wajib (Sesuai Kontrak)
        if "column_mapping" in contract:
             df.rename(columns=contract["column_mapping"], inplace=True)
             upper_mapping = {k.upper(): v for k, v in contract["column_mapping"].items()}
             df.rename(columns=upper_mapping, inplace=True)
        
        # Validasi Kolom Wajib
        missing_cols = [c for c in contract["required_columns"] if c not in df.columns]
        if missing_cols:
            raise ValueError(f"⛔ DATA DITOLAK! Kolom hilang: {missing_cols}")
    
    # Normalisasi SEMUA kolom sisa menjadi snake_case
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # --- CLEANING TIPE DATA ---
    
    # A. Tanggal
    for col in df.columns:
        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # B. Numerik Murni (Hanya Lat, Lon, Umur)
    # Jangan masukkan ID di sini karena ID bisa alphanumeric
    numeric_cols = ['lat', 'lon', 'vict_age']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # C. [FIX] ID & Codes (Dimensi Keys) - Handle Alphanumeric
    # Strategi Baru: 
    # 1. Isi NaN dengan 'Unknown'
    # 2. Ubah semua ke String
    # 3. Hapus akhiran ".0" manual dengan regex (utk kasus ID angka seperti 100.0)
    # Ini aman untuk 'IC', 'AA', maupun '100.0'
    id_cols = ['dr_no', 'area_id', 'crm_cd', 'status_id', 'weapon_id', 'premis_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown').astype(str).str.replace(r'\.0$', '', regex=True)
            # Pastikan 'nan' string juga dianggap Unknown
            df[col] = df[col].replace('nan', 'Unknown')

    # D. String Deskripsi (Sisa)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    # --- LOGIC CHECK ---
    df = check_business_logic(df)
    
    # 4. Simpan ke Silver
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)

    print(f"🚀 Menyimpan {output_file} ke {DEST_BUCKET}...")
    client.put_object(DEST_BUCKET, output_file, parquet_buffer, length=parquet_buffer.getbuffer().nbytes)
    print("✅ Transformasi Selesai!")

if __name__ == "__main__":
    clean_and_load_to_silver()