#!/bin/sh
# Tunggu MinIO menyala
sleep 10;

# Setup alias 'myminio'
/usr/bin/mc config host add myminio http://minio:9000 minioadmin minioadmin;

# Buat bucket jika belum ada
/usr/bin/mc mb --ignore-existing myminio/crime-bronze;
/usr/bin/mc mb --ignore-existing myminio/crime-silver;
/usr/bin/mc mb --ignore-existing myminio/crime-gold;
/usr/bin/mc mb --ignore-existing myminio/crime-archive; # Untuk Disaster Recovery

# Set policy public (opsional, agar mudah diakses saat development)
/usr/bin/mc anonymous set public myminio/crime-bronze;

echo "Bucket initialization complete."