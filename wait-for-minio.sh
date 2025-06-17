#!/bin/sh
# wait-for-minio.sh
# Skrip ini akan mengecek koneksi ke host 'minio' di port 9000.

until nc -z minio 9000; do
  echo "Menunggu MinIO siap di port 9000..."
  sleep 2
done

echo ">>> MINIO SUDAH SIAP! Melanjutkan proses... <<<"
exec "$@"