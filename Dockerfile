# =================================================================
# Dockerfile Final untuk Proyek Big Data & Data Lakehouse
# =================================================================

# Tahap 1: Pilih Image Dasar yang Ringan dan Stabil
FROM python:3.9-slim

# Tahap 2: Instal Dependensi Level Sistem Operasi
# - netcat-openbsd: Menyediakan perintah 'nc' untuk skrip wait-for-*.sh.
# - procps: Menyediakan perintah 'pkill' jika diperlukan (praktik yang baik).
# Install utilitas sistem dan perkakas untuk kompilasi
RUN apt-get update && apt-get install -y netcat-openbsd procps build-essential && \
    rm -rf /var/lib/apt/lists/*

# Tahap 3: Atur Direktori Kerja di Dalam Container
WORKDIR /app

# Tahap 4: Salin dan Instal Semua Library Python
# Menyalin requirements.txt terlebih dahulu memanfaatkan cache Docker
# agar proses build lebih cepat jika library tidak berubah.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tahap 5: Salin Seluruh Kode Proyek ke Dalam Container
COPY . .

# Tahap 6: Berikan Izin Eksekusi pada Skrip Shell
# Ini adalah langkah WAJIB agar container bisa menjalankan skrip .sh.
RUN chmod +x /app/wait-for-kafka.sh /app/wait-for-minio.sh

# Tahap 7: Perintah Default (hanya sebagai cadangan)
CMD ["python"]