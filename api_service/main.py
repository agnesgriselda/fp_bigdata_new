# api_service/main.py (Versi Final dengan Endpoint Prediksi & Review)

import os
import io
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio

# --- Konfigurasi Aplikasi FastAPI ---
app = FastAPI(
    title="API Analisis Penerbangan",
    description="Menyediakan estimasi keterlambatan (dari model PySpark) dan sampel ulasan sentimen.",
    version="2.1.0"
)

# Izinkan semua origin untuk kemudahan pengembangan frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Konfigurasi MinIO ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
ARTIFACT_FILE_KEY = "spark_model_feature_importances.csv"
SENTIMENT_FILE_KEY = "sentiment_analysis_results.parquet"

# --- State Aplikasi: Muat Data & Artefak saat Startup ---
app_data = {
    "importances": None,
    "reviews": None
}

@app.on_event("startup")
def load_artifacts_and_data():
    """
    Memuat semua file yang diperlukan dari MinIO saat aplikasi dimulai.
    """
    print("Mencoba memuat semua data dan artefak dari MinIO...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        
        # 1. Muat Artefak Prediksi Delay
        response_pred = client.get_object(BUCKET_NAME, ARTIFACT_FILE_KEY)
        app_data["importances"] = pd.read_csv(io.BytesIO(response_pred.read()))
        print(f"Artefak '{ARTIFACT_FILE_KEY}' berhasil dimuat.")
        
        # 2. Muat Data Hasil Analisis Sentimen
        response_senti = client.get_object(BUCKET_NAME, SENTIMENT_FILE_KEY)
        app_data["reviews"] = pd.read_parquet(io.BytesIO(response_senti.read()))
        print(f"Data '{SENTIMENT_FILE_KEY}' berhasil dimuat.")

    except Exception as e:
        print(f"GAGAL memuat data/artefak dari MinIO: {e}")
        print("API akan berjalan, tetapi endpoint akan mengembalikan error.")

# --- Model Data Input (Request Body) ---
class FlightPredictionRequest(BaseModel):
    month: int
    day: int
    airline: str
    departure_delay: int
    distance: int

# --- Endpoint Prediksi (Simulasi) ---
@app.post("/predict")
def simulate_flight_delay_prediction(data: FlightPredictionRequest):
    if app_data["importances"] is None:
        raise HTTPException(status_code=503, detail="Artefak model prediksi tidak tersedia.")
    
    try:
        importances_df = app_data["importances"]
        delay_weight = importances_df[importances_df.feature == 'DEPARTURE_DELAY']['importance'].values.item(0)
        distance_weight = importances_df[importances_df.feature == 'DISTANCE']['importance'].values.item(0)
        airline_feature_name = f"AIRLINE_{data.airline.upper()}"
        airline_row = importances_df[importances_df.feature == airline_feature_name]
        airline_weight = airline_row['importance'].values.item(0) if not airline_row.empty else 0

        base_prediction = (
            (data.departure_delay * delay_weight) +
            (data.distance * distance_weight / 100) +
            (airline_weight * 50)
        ) * 250
        final_prediction = max(-30, min(base_prediction, 240))
        return {"predicted_arrival_delay": round(final_prediction, 2)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan saat kalkulasi prediksi: {str(e)}")

# --- Endpoint untuk Ulasan ---
@app.get("/reviews")
def get_sample_reviews(count: int = 3):
    if app_data["reviews"] is None:
        raise HTTPException(status_code=503, detail="Data ulasan tidak tersedia.")
    
    sample_reviews = app_data["reviews"].sample(n=count)
    return sample_reviews.to_dict(orient="records")

# --- Endpoint Status ---
@app.get("/")
def read_root():
    pred_status = "OK" if app_data["importances"] is not None else "Error"
    senti_status = "OK" if app_data["reviews"] is not None else "Error"
    return {
        "service": "Flight Analysis API",
        "status": "online",
        "artifacts": {
            "prediction_model_insights": pred_status,
            "sentiment_data": senti_status
        }
    }
