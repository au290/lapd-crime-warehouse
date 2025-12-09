from minio import Minio
from minio.commonconfig import CopySource
from datetime import datetime
import os

def perform_backup(**kwargs):
    # --- KONFIGURASI ---
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    
    # Bucket Tujuan (Archive/DR)
    BACKUP_BUCKET = "crime-archive"
    # Bucket Sumber yang mau diamankan
    SOURCE_BUCKETS = ["crime-bronze", "crime-silver", "crime-gold"]

    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

    # Folder backup hari ini: misal "2025-12-09/"
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"🛡️ MEMULAI DISASTER RECOVERY BACKUP: {today_str}")

    # Pastikan bucket archive ada
    if not client.bucket_exists(BACKUP_BUCKET):
        client.make_bucket(BACKUP_BUCKET)
        print(f"Created bucket: {BACKUP_BUCKET}")

    total_files = 0
    
    for bucket in SOURCE_BUCKETS:
        if not client.bucket_exists(bucket):
            print(f"⚠️ Bucket sumber {bucket} tidak ditemukan. Skip.")
            continue

        print(f"📦 Memproses bucket: {bucket}...")
        
        # List semua file di bucket sumber
        objects = client.list_objects(bucket, recursive=True)
        
        for obj in objects:
            # Format Tujuan: crime-archive/2025-12-09/crime-gold/master_summary.csv
            dest_path = f"{today_str}/{bucket}/{obj.object_name}"
            
            # Lakukan Copy (Server-side copy, cepat & efisien)
            client.copy_object(
                BACKUP_BUCKET,
                dest_path,
                CopySource(bucket, obj.object_name)
            )
            total_files += 1
            print(f"   -> Backed up: {obj.object_name}")

    print(f"✅ BACKUP SUKSES! Total {total_files} file diamankan di {BACKUP_BUCKET}/{today_str}/")

if __name__ == "__main__":
    perform_backup()