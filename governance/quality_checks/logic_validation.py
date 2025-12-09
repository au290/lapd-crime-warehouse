import pandas as pd

def check_business_logic(df):
    print("🧠 [LOGIC CHECK] Memulai pemeriksaan logika bisnis...")
    
    anomalies = []

    # ATURAN 1: Cek Time Travel
    # Tanggal Lapor tidak boleh sebelum Tanggal Kejadian
    if 'date_rptd' in df.columns and 'date_occ' in df.columns:
        invalid_dates = df[df['date_rptd'] < df['date_occ']]
        if not invalid_dates.empty:
            msg = f"⚠️ ANOMALI WAKTU: {len(invalid_dates)} kasus dilaporkan sebelum kejadian."
            anomalies.append(msg)
            # Opsional: Drop data ngaco ini
            # df = df[df['date_rptd'] >= df['date_occ']]

    # ATURAN 2: Cek Umur Tidak Masuk Akal
    # Umur tidak boleh negatif
    if 'vict_age' in df.columns:
        invalid_age = df[df['vict_age'] < 0]
        if not invalid_age.empty:
            msg = f"⚠️ ANOMALI UMUR: {len(invalid_age)} korban berumur negatif."
            anomalies.append(msg)

    # ATURAN 3: Cek Lokasi Null Island (0,0)
    # LA kira-kira di Lat 34, Lon -118. Kalau 0 berarti error alat.
    if 'lat' in df.columns and 'lon' in df.columns:
        invalid_loc = df[(df['lat'] == 0) & (df['lon'] == 0)]
        if not invalid_loc.empty:
            msg = f"⚠️ ANOMALI LOKASI: {len(invalid_loc)} kasus terjadi di koordinat 0,0."
            anomalies.append(msg)

    # Laporan Akhir
    if anomalies:
        print("\n".join(anomalies))
        print(f"Total {len(anomalies)} jenis anomali ditemukan.")
    else:
        print("✅ Semua data masuk akal secara logika.")
    
    return df