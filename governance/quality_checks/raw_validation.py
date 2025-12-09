import json
from minio import Minio
from datetime import datetime

def validate_raw_json_structure(**kwargs):
    # Konfigurasi
    client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    bucket_name = "crime-bronze"
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"raw_crime_{today_str}.json"
    
    print(f"🔍 [RAW CHECK] Memeriksa integritas file: {file_name}...")
    
    # 1. Cek Keberadaan File
    try:
        response = client.get_object(bucket_name, file_name)
        data = json.loads(response.read())
        response.close()
        response.release_conn()
    except Exception as e:
        raise ValueError(f"⛔ CRITICAL: File raw tidak ditemukan! Pipeline STOP. ({e})")

    # 2. Cek Apakah Kosong?
    if not data:
        raise ValueError("⛔ CRITICAL: Data kosong (0 records). Pipeline STOP.")
    
    # 3. Cek Kualitas Dasar (Misal: Minimal ada 1 kolom kunci)
    # Kita ambil sampel baris pertama
    first_record = data[0]
    
    # Cek apakah ada indikasi kolom 'DR_NO' (Case insensitive search)
    # Kita tidak validasi skema lengkap disini (itu tugas Silver), 
    # di sini cuma pastikan ini bukan file JSON sampah.
    keys_lower = [k.lower().replace(' ', '_') for k in first_record.keys()]
    if 'dr_no' not in keys_lower and 'dr_no' not in str(keys_lower):
        print(f"Sampel keys: {list(first_record.keys())}")
        raise ValueError("⛔ CRITICAL: Data tidak terlihat seperti Data Kriminal (Kolom DR_NO hilang).")

    print(f"✅ [RAW CHECK] Lolos. Data berisi {len(data)} baris.")
    return True