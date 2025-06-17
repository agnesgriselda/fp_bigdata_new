# api_service/main.py (Revisi Final Anti-Seek Error)

import os
import io
import joblib
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio

# --- Konfigurasi ---
app = FastAPI(title="API Prediksi Keterlambatan Penerbangan")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
MODEL_FILE_KEY = "flight_delay_model.pkl"

# --- Load Model dari MinIO saat Startup ---
model = None
model_features = []
try:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    # Download file model ke buffer memori
    response = client.get_object(BUCKET_NAME, MODEL_FILE_KEY)
    model_bytes = response.read()
    model_buffer = io.BytesIO(model_bytes)
    
    # Load model dari buffer memori
    model = joblib.load(model_buffer)
    
    print(f"Model '{MODEL_FILE_KEY}' berhasil di-load dari MinIO.")
    model_features = model.feature_names_in_
except Exception as e:
    print(f"GAGAL me-load model dari MinIO: {e}")
finally:
     if 'response' in locals() and response:
            response.close()
            response.release_conn()

# --- Model Data Input ---
class FlightPredictionRequest(BaseModel):
    year: int; month: int; day: int; day_of_week: int
    airline: str
    departure_delay: int
    distance: int

# --- Endpoint Prediksi ---
@app.post("/predict")
def predict_flight_delay(data: FlightPredictionRequest):
    if model is None: return {"error": "Model tidak tersedia."}
    
    try:
        input_df = pd.DataFrame([data.dict()])
        input_df_encoded = pd.get_dummies(input_df, columns=['airline'], prefix='airline', drop_first=False)
        
        final_df = pd.DataFrame(columns=model_features).fillna(0)
        final_df = pd.concat([final_df, input_df_encoded], ignore_index=True, sort=False).fillna(0)
        final_df = final_df[model_features]

        prediction = model.predict(final_df)
        return {"predicted_arrival_delay": round(prediction[0], 2)}

    except Exception as e:
        return {"error": f"Terjadi kesalahan saat prediksi: {str(e)}"}