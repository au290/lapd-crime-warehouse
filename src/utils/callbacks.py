import requests
import logging
from airflow.models import Variable

# --- KONFIGURASI ---
# 1. Masukkan Webhook URL Anda
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1447839141063692341/ue-nFshTa5CgxdJ2gEeiU2iwbxiLH0iE9Ievfk5xnnPZ1D82NIVnFwBmDH4dTbhdDMYF"

# 2. Masukkan Discord User ID untuk di-tag saat error
# Cara dapat ID: Di Discord, aktifkan Developer Mode -> Klik Kanan Profil Anda -> Copy ID
DISCORD_USER_ID = "902058850599993404" 

def send_discord_message(payload):
    """Fungsi helper untuk kirim request ke Discord"""
    if "YOUR_WEBHOOK" in DISCORD_WEBHOOK_URL:
        logging.warning("⚠️ Discord Webhook URL belum di-set!")
        return

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        logging.info("✅ Notifikasi terkirim ke Discord.")
    except Exception as e:
        logging.error(f"❌ Gagal kirim notifikasi: {e}")

def send_failure_alert(context):
    """
    Dipanggil saat Task GAGAL.
    Melakukan TAGGING ke User ID.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url
    exception = context.get('exception')

    payload = {
        # [PENTING] Tagging User dilakukan di sini
        "content": f"🚨 **CRITICAL ALERT** <@{DISCORD_USER_ID}>! Task Failed!",
        "embeds": [{
            "title": "❌ Pipeline Gagal Berjalan",
            "color": 15548997, # Merah
            "fields": [
                {"name": "DAG", "value": f"`{dag_id}`", "inline": True},
                {"name": "Task", "value": f"`{task_id}`", "inline": True},
                {"name": "Error", "value": f"```{str(exception)[:200]}...```", "inline": False}
            ],
            "description": f"[👉 Klik Disini untuk Memperbaiki]({log_url})",
            "footer": {"text": "Segera diperiksa ya!"}
        }]
    }
    send_discord_message(payload)

def send_success_alert(context):
    """
    Dipanggil saat Task SUKSES.
    Hanya info log biasa (Tanpa Tagging).
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')

    payload = {
        # Tidak ada tagging user di sini, hanya pesan biasa
        "content": "✅ **Laporan Status:**",
        "embeds": [{
            "title": "Task Selesai (Success)",
            "color": 5763719, # Hijau
            "description": f"Tugas **{task_id}** pada DAG **{dag_id}** telah berhasil dijalankan.",
            "footer": {"text": f"Waktu Eksekusi: {execution_date}"}
        }]
    }
    send_discord_message(payload)