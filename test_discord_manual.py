import requests

# --- GANTI DENGAN URL ASLI ANDA ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1447839141063692341/ue-nFshTa5CgxdJ2gEeiU2iwbxiLH0iE9Ievfk5xnnPZ1D82NIVnFwBmDH4dTbhdDMYF" 

def test_connection():
    print(f"Mencoba mengirim pesan ke: {WEBHOOK_URL}...")
    
    data = {
        "username": "Tes Koneksi 🤖",
        "content": "👋 Halo! Jika pesan ini masuk, berarti Webhook Anda berfungsi 100%."
    }

    try:
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code == 204:
            print("✅ SUKSES! Pesan terkirim. Cek channel Discord Anda sekarang.")
        else:
            print(f"❌ GAGAL! Kode Error: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    test_connection()