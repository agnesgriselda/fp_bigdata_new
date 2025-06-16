import time
import csv
import json
from kafka import KafkaProducer
import os

# Konfigurasi yang disesuaikan untuk dataset penerbangan
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = 'flight-data'
CSV_FILE_PATH = 'data_source/flights.csv'

def create_producer():
    """Membuat dan mengembalikan instance KafkaProducer yang sudah di-tuning."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        # --- PERBAIKAN: TUNING KONFIGURASI PRODUCER UNTUK HIGH-THROUGHPUT ---
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 2),
            # Ukuran batch (dalam byte). 64KB adalah nilai awal yang baik.
            batch_size=65536,
            # Waktu tunggu (ms) untuk mengumpulkan pesan sebelum mengirim batch.
            linger_ms=5,
            # Jenis kompresi. 'lz4' sangat cepat dan efisien.
            compression_type='lz4',
            # Buffer memori total untuk producer
            buffer_memory=67108864, # 64 MB
        )
        # --------------------------------------------------------------------
        print("Successfully connected to Kafka Broker with tuned configuration.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka Broker: {e}")
        time.sleep(5)
        return create_producer()

def publish_data(producer, topic):
    """Membaca data dari flights.csv dan mengirimkannya ke Kafka secepat mungkin."""
    print(f"Reading data from {CSV_FILE_PATH} and publishing to topic '{topic}'...")
    
    try:
        with open(CSV_FILE_PATH, mode='r') as file:
            csv_reader = csv.DictReader(file)
            
            print("Starting to send flight data in high-throughput mode...")
            start_time = time.time()
            for i, row in enumerate(csv_reader):
                try:
                    message = {
                        "YEAR": int(row.get("YEAR", 0)),
                        "MONTH": int(row.get("MONTH", 0)),
                        "DAY": int(row.get("DAY", 0)),
                        "AIRLINE": row.get("AIRLINE", ""),
                        "FLIGHT_NUMBER": int(row.get("FLIGHT_NUMBER", 0)),
                        "ORIGIN_AIRPORT": row.get("ORIGIN_AIRPORT", ""),
                        "DESTINATION_AIRPORT": row.get("DESTINATION_AIRPORT", ""),
                        "DEPARTURE_DELAY": float(row.get("DEPARTURE_DELAY") or 0),
                        "ARRIVAL_DELAY": float(row.get("ARRIVAL_DELAY") or 0),
                        "DISTANCE": int(row.get("DISTANCE", 0))
                    }
                 
                    producer.send(topic, value=message)
                    
                    # --- PERBAIKAN: KURANGI FREKUENSI PRINT ---
                    # Cetak status setiap 50,000 data agar tidak membanjiri terminal
                    if (i + 1) % 50000 == 0:
                        print(f"Sent {i + 1} records...")
                 
                except (ValueError, KeyError) as e:
                    # Jangan print error untuk setiap baris, cukup hitung saja (opsional)
                    # print(f"Skipping row due to data error: {row}. Error: {e}")
                    continue
    except FileNotFoundError:
        print(f"Error: File not found at {CSV_FILE_PATH}")
        return
 
    print("Flushing final messages... (this might take a moment)")
    producer.flush()
    end_time = time.time()
    print("All data has been sent.")
    print(f"Total time taken: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        publish_data(kafka_producer, KAFKA_TOPIC)
        kafka_producer.close()
        print("Producer has been closed.")