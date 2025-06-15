import time
import csv
import json
from kafka import KafkaProducer
import os

# Konfigurasi yang disesuaikan untuk dataset penerbangan
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = 'flight-data'  # <-- DIUBAH
CSV_FILE_PATH = 'data_source/flights.csv'  # <-- DIUBAH
SIMULATION_DELAY_SECONDS = 0.01  # <-- DIUBAH (agar lebih cepat)

def create_producer():
    """Membuat dan mengembalikan instance KafkaProducer."""
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 2)
        )
        print("Successfully connected to Kafka Broker.")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka Broker: {e}")
        time.sleep(5)
        return create_producer()

def publish_data(producer, topic):
    """Membaca data dari flights.csv dan mengirimkannya ke Kafka."""
    print(f"Reading data from {CSV_FILE_PATH} and publishing to topic '{topic}'...")
 
    try:
        with open(CSV_FILE_PATH, mode='r') as file:
            # Menggunakan DictReader untuk kemudahan akses kolom berdasarkan nama
            csv_reader = csv.DictReader(file)
            
            print("Starting to send flight data...")
            for i, row in enumerate(csv_reader):
                try:
                    # <-- LOGIKA UTAMA DIUBAH DI SINI
                    # Pilih kolom yang relevan dan konversi tipe datanya
                    # Gunakan float untuk delay karena bisa bernilai desimal atau negatif
                    # Gunakan try-except untuk setiap konversi numerik untuk menangani nilai kosong
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
                    
                    # Print status setiap 1000 data agar tidak membanjiri terminal
                    if (i + 1) % 1000 == 0:
                        print(f"Sent {i + 1} records...")
                 
                    time.sleep(SIMULATION_DELAY_SECONDS)
                 
                except (ValueError, KeyError) as e:
                    print(f"Skipping row due to data error: {row}. Error: {e}")
                    continue
    except FileNotFoundError:
        print(f"Error: File not found at {CSV_FILE_PATH}")
        return
 
    producer.flush()
    print("All data has been sent.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        publish_data(kafka_producer, KAFKA_TOPIC)
        kafka_producer.close()
        print("Producer has been closed.")
