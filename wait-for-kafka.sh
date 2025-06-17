#!/bin/sh
# wait-for-kafka.sh
# Skrip ini akan terus mencoba terhubung ke host 'kafka' di port 29092
# (port internal yang kita definisikan di docker-compose.yml).

# Loop akan terus berjalan selama perintah 'nc' (netcat) gagal.
until nc -z kafka 29092; do
  echo "Menunggu Kafka siap di port 29092..."
  # Tunggu 2 detik sebelum mencoba lagi
  sleep 2
done

echo ">>> KAFKA SUDAH SIAP! Melanjutkan proses... <<<"
# 'exec "$@"' akan menjalankan perintah selanjutnya yang diberikan.
exec "$@"