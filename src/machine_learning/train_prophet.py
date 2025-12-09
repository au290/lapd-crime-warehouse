import pandas as pd
from prophet import Prophet
from minio import Minio
from io import BytesIO
import joblib
import os

def train_and_save_model(**kwargs):
    # 1. Konfigurasi
    MINIO_ENDPOINT = "minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    
    # Bucket Khusus Model
    MODEL_BUCKET = "crime-models"
    DATA_BUCKET = "crime-gold"
    
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    # Buat bucket model jika belum ada
    if not client.bucket_exists(MODEL_BUCKET):
        client.make_bucket(MODEL_BUCKET)

    print("🧠 Memulai Training Model Forecasting...")

    # 2. Load Data dari Gold Layer
    try:
        response = client.get_object(DATA_BUCKET, "fact_crime.parquet")
        df = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
    except Exception as e:
        print(f"❌ Gagal load data: {e}")
        return

    # 3. Preprocessing untuk Prophet
    # Prophet butuh kolom: 'ds' (tanggal) dan 'y' (nilai)
    if 'date_occ' in df.columns:
        df['date_occ'] = pd.to_datetime(df['date_occ'])
        # Agregasi harian (Jumlah kasus per hari)
        daily_counts = df.groupby('date_occ').size().reset_index(name='y')
        daily_counts.rename(columns={'date_occ': 'ds'}, inplace=True)
    else:
        print("❌ Kolom tanggal tidak ditemukan")
        return

    print(f"📊 Data Training: {len(daily_counts)} hari.")

    # 4. Training Prophet
    # Kita tambahkan seasonality mingguan (kejahatan biasanya tinggi di weekend)
    m = Prophet(daily_seasonality=True, weekly_seasonality=True)
    m.fit(daily_counts)
    
    print("✅ Model berhasil dilatih!")

    # 5. Simpan Model ke MinIO
    # Kita serialize model jadi bytes
    model_buffer = BytesIO()
    joblib.dump(m, model_buffer)
    model_buffer.seek(0)
    
    model_name = "prophet_crime_v1.joblib"
    
    client.put_object(
        MODEL_BUCKET,
        model_name,
        model_buffer,
        length=model_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print(f"💾 Model disimpan ke MinIO: {MODEL_BUCKET}/{model_name}")

if __name__ == "__main__":
    train_and_save_model()