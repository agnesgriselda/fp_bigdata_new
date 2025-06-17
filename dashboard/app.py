# dashboard/app.py (Versi Final, Anti-KeyError, Tampilan Profesional)

import streamlit as st
import pandas as pd
from minio import Minio
import io
import plotly.express as px
import os

# =================================================================================
# BAGIAN 1: KONFIGURASI DAN FUNGSI HELPER
# =================================================================================

# --- Konfigurasi MinIO (Satu Kali Definisi) ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
DATA_FILE_KEY = "cleaned_flight_data.parquet"

@st.cache_data
def load_data_from_minio():
    """Download file Parquet ke memori dulu, baru dibaca oleh Pandas."""
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        response = client.get_object(BUCKET_NAME, DATA_FILE_KEY)
        parquet_bytes = response.read()
        parquet_buffer = io.BytesIO(parquet_bytes)
        df = pd.read_parquet(parquet_buffer)
        print("Dashboard: Berhasil memuat data dari MinIO.")
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data dari MinIO untuk Dashboard: {e}")
        return None
    finally:
        if 'response' in locals() and response:
            response.close()
            response.release_conn()

def reverse_one_hot_encoding(df, prefix):
    """Mengembalikan kolom yang sudah di-one-hot-encode ke bentuk aslinya."""
    encoded_columns = [col for col in df.columns if col.startswith(prefix)]
    if not encoded_columns: return None
    original_series = df[encoded_columns].idxmax(axis=1).apply(lambda x: x.replace(prefix + '_', ''))
    return original_series

# =================================================================================
# BAGIAN 2: UTAMA APLIKASI STREAMLIT
# =================================================================================

st.set_page_config(page_title="Dashboard Analisis Penerbangan", layout="wide", initial_sidebar_state="expanded")
st.title("✈️ Dashboard Analisis Keterlambatan Penerbangan")

df_raw = load_data_from_minio()

# --- PENANGANAN ERROR UTAMA ---
# Hanya lanjutkan jika DataFrame (df_raw) berhasil dimuat dan tidak kosong
if df_raw is not None and not df_raw.empty:
    df = df_raw.copy()
    
    # Rekonstruksi kolom 'airline_name' agar lebih user-friendly
    airline_series = reverse_one_hot_encoding(df, 'airline')
    if airline_series is not None:
        df['airline_name'] = airline_series
    
    # --- SIDEBAR FILTER ---
    st.sidebar.header("✈️ Filter Data")
    
    airline_list = ['Semua']
    if 'airline_name' in df.columns:
        airline_list.extend(sorted(df['airline_name'].unique()))
    selected_airline = st.sidebar.selectbox("Pilih Maskapai", airline_list)

    month_list = ['Semua'] + sorted(df['month'].unique().tolist())
    selected_month = st.sidebar.selectbox("Pilih Bulan", month_list)

    # Terapkan filter
    filtered_df = df.copy()
    if selected_airline != 'Semua' and 'airline_name' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['airline_name'] == selected_airline]
    if selected_month != 'Semua':
        filtered_df = filtered_df[filtered_df['month'] == selected_month]

    # --- HEADER & KPI ---
    st.markdown("Analisis interaktif untuk memahami pola keterlambatan penerbangan di AS berdasarkan **500,000 data sampel**.")
    
    kpi1, kpi2, kpi3 = st.columns(3)
    if not filtered_df.empty:
        avg_dep_delay = filtered_df['departure_delay'].mean()
        avg_arr_delay = filtered_df['arrival_delay'].mean()
        total_flights = len(filtered_df)
    else:
        avg_dep_delay, avg_arr_delay, total_flights = 0, 0, 0

    kpi1.metric("Rata-rata Delay Keberangkatan", f"{avg_dep_delay:.1f} menit")
    kpi2.metric("Rata-rata Delay Kedatangan", f"{avg_arr_delay:.1f} menit")
    kpi3.metric("Total Penerbangan (Filtered)", f"{total_flights:,}")
    
    st.divider()

    # --- VISUALISASI UTAMA DENGAN TABS ---
    st.subheader("Analisis Mendalam Keterlambatan")
    tab1, tab2, tab3 = st.tabs(["📊 Per Maskapai", "📈 Tren Bulanan", "🔥 Pola Waktu (Heatmap)"])

    with tab1:
        st.markdown("##### Maskapai Mana yang Paling Sering Terlambat?")
        if 'airline_name' in filtered_df.columns and not filtered_df.empty:
            avg_delay_per_airline = filtered_df.groupby('airline_name')['arrival_delay'].mean().sort_values().reset_index()
            fig1 = px.bar(avg_delay_per_airline, x='arrival_delay', y='airline_name', orientation='h',
                          labels={'arrival_delay': 'Rata-rata Delay (menit)', 'airline_name': 'Maskapai'},
                          color='arrival_delay', color_continuous_scale=px.colors.sequential.Plasma,
                          text_auto='.2s')
            fig1.update_traces(textposition='outside')
            st.plotly_chart(fig1, use_container_width=True)
            st.caption("Grafik ini menunjukkan rata-rata keterlambatan kedatangan untuk setiap maskapai.")
    
    with tab2:
        st.markdown("##### Bagaimana Pola Keterlambatan Sepanjang Tahun?")
        if not filtered_df.empty:
            avg_delay_per_month = filtered_df.groupby('month')[['departure_delay', 'arrival_delay']].mean().reset_index()
            fig2 = px.line(avg_delay_per_month, x='month', y=['departure_delay', 'arrival_delay'],
                           labels={'value': 'Rata-rata Delay (menit)', 'month': 'Bulan', 'variable': 'Tipe Delay'}, markers=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.caption("Grafik ini menunjukkan tren keterlambatan bulanan. Puncak keterlambatan biasanya terjadi pada musim liburan atau cuaca buruk.")

    with tab3:
        st.markdown("##### Kapan Waktu Terburuk untuk Terbang?")
        
        # --- PERBAIKAN FINAL DI SINI ---
        # Definisikan kolom yang dibutuhkan untuk heatmap
        heatmap_cols = ['month', 'day_of_week', 'arrival_delay']
        
        # Cek apakah semua kolom yang dibutuhkan ada DAN DataFrame tidak kosong
        if all(col in filtered_df.columns for col in heatmap_cols) and not filtered_df.empty:
            heatmap_data = filtered_df.groupby(['month', 'day_of_week'])['arrival_delay'].mean().reset_index()
            heatmap_pivot = heatmap_data.pivot(index='day_of_week', columns='month', values='arrival_delay')
            
            # Mapping nama hari agar lebih mudah dibaca dan diurutkan
            hari = {1: '1-Senin', 2: '2-Selasa', 3: '3-Rabu', 4: '4-Kamis', 5: '5-Jumat', 6: '6-Sabtu', 7: '7-Minggu'}
            heatmap_pivot = heatmap_pivot.rename(index=hari).sort_index()
            
            fig3 = px.imshow(heatmap_pivot, text_auto='.1f', aspect="auto",
                             labels=dict(x="Bulan", y="Hari", color="Rata-rata Delay"),
                             color_continuous_scale='Reds')
            st.plotly_chart(fig3, use_container_width=True)
            st.caption("Heatmap ini menunjukkan 'hotspot' keterlambatan. Kotak yang lebih merah menandakan rata-rata keterlambatan yang lebih tinggi.")
        else:
            st.warning("Data tidak cukup atau kolom yang dibutuhkan (day_of_week) tidak ditemukan untuk membuat heatmap.")
            st.write("Kolom yang tersedia:", filtered_df.columns.tolist())

    # --- TABEL DATA ---
    with st.expander("Lihat Sampel Data (setelah difilter)"):
        st.info(f"Untuk menjaga performa, tabel di bawah hanya menampilkan 1,000 baris pertama dari total {total_flights:,} baris data yang telah difilter.")
        st.dataframe(filtered_df.head(500000))

else:
    st.error("Gagal memuat data awal dari MinIO. Pastikan proses backend (ETL & Training) telah berjalan dengan sukses.")