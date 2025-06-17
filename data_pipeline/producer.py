# data_pipeline/producer.py (Versi Final untuk Demo Cepat - 500k Record)

import time
import csv
import json
from kafka import KafkaProducer
import os

# Konfigurasi
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = 'flight-data'
CSV_FILE_PATH = 'data_source/flights.csv'

def create_producer():
    """Membuat instance KafkaProducer yang sudah di-tuning untuk throughput tinggi."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 2),
            batch_size=65536,       # 64KB
            linger_ms=5,            # Tunggu 5ms
            compression_type='lz4'
        )
        print("Successfully connected to Kafka Broker.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka Broker: {e}")
        time.sleep(5)
        return create_producer()

def publish_data(producer, topic):
    """Membaca data dari flights.csv, membatasinya, dan mengirimkannya ke Kafka."""
    print(f"Reading data from {CSV_FILE_PATH} and publishing to topic '{topic}'...")

    # --- REVISI UTAMA: MEMBATASI JUMLAH DATA UNTUK DEMO ---
    RECORD_LIMIT = 500000 
    
    try:
        with open(CSV_FILE_PATH, mode='r') as file:
            csv_reader = csv.DictReader(file)
            
            print(f"Starting to send up to {RECORD_LIMIT} flight data records...")
            start_time = time.time()
            records_sent = 0

            for i, row in enumerate(csv_reader):
                # Hentikan loop jika sudah mencapai limit
                if records_sent >= RECORD_LIMIT:
                    print(f"Reached record limit of {RECORD_LIMIT}. Stopping producer.")
                    break
                
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
                    records_sent += 1
                    
                    # Cetak status setiap 50,000 data
                    if records_sent % 50000 == 0:
                        print(f"Sent {records_sent} records...")
                 
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        print(f"ERROR: File not found at {CSV_FILE_PATH}")
        return
 
    print("Flushing final messages...")
    producer.flush()
    end_time = time.time()
    print(f"All {records_sent} records have been sent.")
    print(f"Total time taken for producing data: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        publish_data(kafka_producer, KAFKA_TOPIC)
        kafka_producer.close()
        print("Producer has been closed.")