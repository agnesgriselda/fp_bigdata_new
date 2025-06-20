# dashboard/app.py (Versi Final untuk Model PySpark & Analisis Sentimen)

import streamlit as st
import pandas as pd
from minio import Minio
import io
import plotly.express as px
import os

# =================================================================================
# BAGIAN 1: KONFIGURASI DAN FUNGSI HELPER
# =================================================================================

# --- Konfigurasi MinIO ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
PROCESSED_BUCKET = "processed-data"

# --- Fungsi Caching untuk Load Artefak & Data dari MinIO ---
@st.cache_resource
def get_minio_client():
    """Membuat koneksi MinIO yang di-cache."""
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

@st.cache_data(ttl=3600) # Cache data selama 1 jam
def load_spark_artifacts():
    """Memuat artefak yang disimpan oleh skrip training PySpark."""
    client = get_minio_client()
    try:
        # Muat Feature Importances
        response = client.get_object(PROCESSED_BUCKET, 'spark_model_feature_importances.csv')
        importances_df = pd.read_csv(io.BytesIO(response.read()))
        return importances_df
    except Exception as e:
        st.error(f"Gagal memuat artefak model PySpark: {e}")
        st.error("Pastikan Anda sudah menjalankan 'prepare_data_spark.py' dan 'train_model_spark.py' terlebih dahulu.")
        return None

@st.cache_data(ttl=3600)
def load_sentiment_results():
    """Memuat hasil analisis sentimen."""
    client = get_minio_client()
    try:
        response = client.get_object(PROCESSED_BUCKET, 'sentiment_analysis_results.parquet')
        df = pd.read_parquet(io.BytesIO(response.read()))
        return df
    except Exception:
        # Jangan tampilkan error jika file belum ada, cukup kembalikan None
        return None

# =================================================================================
# BAGIAN 2: HALAMAN-HALAMAN DASHBOARD
# =================================================================================

def page_delay_analysis_spark():
    """Halaman untuk menampilkan wawasan dari model prediksi PySpark."""
    st.title("✈️ Analisis Keterlambatan Penerbangan (ditenagai PySpark)")
    st.markdown("Dashboard ini menampilkan wawasan yang diekstrak langsung dari model Machine Learning yang dilatih pada **Spark**, bukan hanya visualisasi data mentah.")
    
    importances_df = load_spark_artifacts()
    if importances_df is None:
        st.warning("Artefak model tidak ditemukan. Silakan jalankan pipeline training PySpark terlebih dahulu.")
        return

    st.divider()

    # --- Tampilkan Wawasan Utama dari Model ---
    st.subheader("Faktor Paling Berpengaruh pada Keterlambatan")
    st.write("Grafik ini menunjukkan fitur apa yang dianggap paling penting oleh model PySpark dalam memprediksi keterlambatan. Semakin panjang bar, semakin besar pengaruhnya.")
    
    # Ambil 15 fitur teratas
    top_features = importances_df.head(15)
    
    fig = px.bar(
        top_features,
        x='importance',
        y='feature',
        orientation='h',
        title='Top 15 Feature Importances dari Model RandomForest PySpark',
        labels={'importance': 'Tingkat Kepentingan (Importance Score)', 'feature': 'Faktor / Fitur'},
        color='importance',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}) # Urutkan dari terpenting di atas
    st.plotly_chart(fig, use_container_width=True)
    
    st.info("""
    **Bagaimana Membaca Grafik Ini?**
    - **DEPARTURE_DELAY**: Secara konsisten menjadi faktor paling penting. Ini logis, karena jika pesawat sudah terlambat berangkat, kemungkinan besar ia akan terlambat tiba.
    - **DISTANCE**: Jarak tempuh juga berpengaruh signifikan.
    - **AIRLINE_...**: Nama-nama maskapai yang telah di-encode menunjukkan bahwa beberapa maskapai secara inheren memiliki pola keterlambatan yang berbeda dari yang lain.
    """)

def page_sentiment_analysis():
    """Halaman untuk menampilkan hasil analisis sentimen ulasan."""
    st.title("📊 Analisis Sentimen Ulasan Pelanggan")
    st.markdown("Menganalisis sentimen dari ulasan teks tidak terstruktur yang diberikan oleh penumpang.")

    reviews_df = load_sentiment_results()
    if reviews_df is None:
        st.warning("Data hasil analisis sentimen belum tersedia. Jalankan `analyze_sentiment.py` terlebih dahulu.")
        return

    st.divider()

    # --- Visualisasi Utama ---
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Distribusi Sentimen")
        sentiment_counts = reviews_df['sentiment'].value_counts()
        fig_pie = px.pie(
            sentiment_counts, 
            values=sentiment_counts.values, 
            names=sentiment_counts.index, 
            hole=.3,
            color=sentiment_counts.index,
            color_discrete_map={'Positive':'#2ca02c', 'Negative':'#d62728', 'Neutral':'#7f7f7f'}
        )
        fig_pie.update_traces(textinfo='percent+label', pull=[0.05, 0.05, 0.05])
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Coba ekstrak nama maskapai dari teks ulasan untuk analisis tambahan
        try:
            # Menggunakan regex sederhana untuk mengekstrak nama maskapai dari teks ulasan
            # Ini mungkin tidak sempurna tapi cukup baik untuk demo
            reviews_df['airline_from_text'] = reviews_df['text'].str.extract(r"dengan (\w+\s?\w*)")[0]
            airline_sentiment = reviews_df.groupby(['airline_from_text', 'sentiment']).size().unstack(fill_value=0)
            airline_sentiment = airline_sentiment.sort_values(by=['Positive', 'Negative'], ascending=[False, True]).head(10)
            
            st.subheader("Sentimen per Maskapai (Top 10)")
            fig_bar = px.bar(airline_sentiment, barmode='stack',
                             labels={'value': 'Jumlah Ulasan', 'airline_from_text': 'Maskapai'},
                             color_discrete_map={'Positive':'#2ca02c', 'Negative':'#d62728', 'Neutral':'#7f7f7f'})
            st.plotly_chart(fig_bar, use_container_width=True)
        except Exception:
            st.info("Tidak dapat mengekstrak sentimen per maskapai dari data saat ini.")

    # --- Tabel Data Ulasan ---
    with st.expander("Jelajahi Data Ulasan Individual"):
        st.dataframe(reviews_df[['text', 'sentiment']])

# =================================================================================
# BAGIAN 3: NAVIGASI DAN LAYOUT UTAMA
# =================================================================================

st.set_page_config(page_title="Dashboard Aviasi", layout="wide", initial_sidebar_state="expanded")

# --- Sidebar ---
with st.sidebar:
    st.header("✈️ Dashboard Aviasi")
    st.write("Analisis Big Data & Machine Learning")
    
    page = st.radio(
        "Pilih Halaman Analisis:",
        ("Analisis Keterlambatan (PySpark)", "Analisis Sentimen Ulasan"),
        key="page_selection"
    )
    
    st.divider()
    st.info(
        "Proyek ini mendemonstrasikan pipeline data end-to-end dari Kafka, MinIO, "
        "hingga pemrosesan terdistribusi dengan PySpark."
    )

# --- Routing Halaman ---
if page == "Analisis Keterlambatan (PySpark)":
    page_delay_analysis_spark()
elif page == "Analisis Sentimen Ulasan":
    page_sentiment_analysis()