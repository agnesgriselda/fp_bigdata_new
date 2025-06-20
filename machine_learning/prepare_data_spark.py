# machine_learning/prepare_data_spark.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

# --- Konfigurasi ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_DATA_BUCKET = 'raw-data'
PROCESSED_DATA_BUCKET = 'processed-data'
# Simpan sebagai tabel Delta Lake
OUTPUT_DELTA_TABLE = 'cleaned_flight_data_delta'

def create_spark_session():
    """Membuat SparkSession yang dikonfigurasi untuk MinIO (S3)."""
    print("Creating Spark session configured for MinIO...")
    
    # Versi paket ini KRUSIAL. Sesuaikan dengan versi Spark Anda jika perlu.
    # Untuk Spark 3.x, versi ini umumnya kompatibel.
    aws_sdk_version = "1.12.262"
    hadoop_aws_version = "3.3.4"

    spark = SparkSession.builder \
        .appName("FlightDataETL_Spark") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version},io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print("Spark session created successfully.")
    return spark

def main():
    spark = create_spark_session()
    if not spark: return

    raw_path = f"s3a://{RAW_DATA_BUCKET}/*/*.jsonl" # Baca semua file batch
    processed_path = f"s3a://{PROCESSED_DATA_BUCKET}/{OUTPUT_DELTA_TABLE}"

    # 1. EXTRACT: Baca data JSON dari MinIO
    print(f"Reading raw JSON data from {raw_path}...")
    try:
        df = spark.read.json(f"s3a://{RAW_DATA_BUCKET}/*")
        print(f"Successfully loaded {df.count()} records.")
    except Exception as e:
        print(f"Error reading from MinIO: {e}. Pastikan data ada di bucket 'raw-data'."); return

    # 2. TRANSFORM: Bersihkan dan ubah data menggunakan Spark
    print("Starting data transformation with PySpark...")
    
    # Konversi tipe data
    df = df.withColumn("ARRIVAL_DELAY", col("ARRIVAL_DELAY").cast(FloatType())) \
           .withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast(FloatType())) \
           .withColumn("DISTANCE", col("DISTANCE").cast(IntegerType()))

    # Hapus baris dengan target null
    df = df.na.drop(subset=["ARRIVAL_DELAY"])
    # Isi nilai null lainnya
    df = df.na.fill(0, subset=["DEPARTURE_DELAY"])
    
    # Hapus kolom yang tidak diperlukan
    df = df.drop("FLIGHT_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "YEAR")
    
    print("Data transformation complete.")

    # 3. LOAD: Simpan sebagai tabel Delta Lake
    print(f"Saving cleaned data as a Delta table to {processed_path}...")
    try:
        df.write.format("delta").mode("overwrite").save(processed_path)
        print("Successfully saved Delta table.")
    except Exception as e:
        print(f"Error saving Delta table: {e}")

    spark.stop()

if __name__ == "__main__":
    main()