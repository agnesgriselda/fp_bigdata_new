import streamlit as st
import pandas as pd
from minio import Minio
import io
import plotly.express as px
import os

# =================================================================================
# BAGIAN 1: KONFIGURASI DAN KONEKSI (Tetap Sama)
# =================================================================================

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "processed-data"
DATA_FILE_KEY = "cleaned_flight_data.csv"

@st.cache_data
def load_data_from_minio():
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        response = client.get_object(BUCKET_NAME, DATA_FILE_KEY)
        csv_data = response.read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_data))
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data dari MinIO: {e}")
        return None
    finally:
        if 'response' in locals() and response:
            response.close()
            response.release_conn()

def reverse_one_hot_encoding(df, prefix):
    encoded_columns = [col for col in df.columns if col.startswith(prefix)]
    if not encoded_columns: return None
    original_series = df[encoded_columns].idxmax(axis=1).apply(lambda x: x.replace(prefix, ''))
    return original_series

# =================================================================================
# BAGIAN 2: FUNGSI STYLING KUSTOM (Disinilah "sihirnya")
# =================================================================================
def apply_custom_style():
    """Menerapkan CSS kustom untuk tema gelap yang profesional."""
    custom_css = """
        <style>
            /* Mengatur tema gelap pada elemen root */
            .stApp {
                background-color: #0f172a; /* Warna background utama (biru sangat gelap) */
            }
            
            /* Styling untuk sidebar */
            .st-emotion-cache-16txtl3 {
                background-color: #1e293b; /* Warna background sidebar (biru lebih terang) */
            }

            /* Styling untuk judul dan teks utama */
            h1, h2, h3 {
                color: #e2e8f0; /* Warna teks terang (putih keabuan) */
            }
            .stMarkdown, p, .st-emotion-cache-10trblm, .st-emotion-cache-1y4p8pa {
                color: #94a3b8; /* Warna teks sekunder (abu-abu kebiruan) */
            }

            /* Styling untuk kartu KPI (Key Performance Indicator) */
            .st-emotion-cache-1tpl0xr {
                background-color: #1e293b;
                border: 1px solid #334155;
                border-radius: 0.75rem; /* Sudut lebih bulat */
                padding: 1.25rem 1.5rem;
            }
            
            /* Styling untuk Expander (Lihat Data) */
            .st-emotion-cache-p5msec {
                border: 1px solid #334155;
                background-color: #1e293b;
                border-radius: 0.75rem;
            }
        </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

# =================================================================================
# BAGIAN 3: MEMBANGUN TAMPILAN DASHBOARD
# =================================================================================

st.set_page_config(page_title="Dashboard Keterlambatan Penerbangan", layout="wide", initial_sidebar_state="expanded")

# Terapkan style kustom kita di awal
apply_custom_style()

st.title("✈️ Dashboard Analisis Keterlambatan Penerbangan")

df_raw = load_data_from_minio()

if df_raw is not None:
    df = df_raw.copy()
    
    airline_series = reverse_one_hot_encoding(df, 'airline_')
    if airline_series is not None: df['airline_name'] = airline_series
    
    # --- SIDEBAR ---
    st.sidebar.header("Filter Data")
    
    airline_list = ['Semua'] + (sorted(df['airline_name'].unique().tolist()) if 'airline_name' in df.columns else [])
    selected_airline = st.sidebar.selectbox("Pilih Maskapai", airline_list)

    month_list = ['Semua'] + sorted(df['month'].unique().tolist())
    selected_month = st.sidebar.selectbox("Pilih Bulan", month_list)

    filtered_df = df.copy()
    if selected_airline != 'Semua': filtered_df = filtered_df[filtered_df['airline_name'] == selected_airline]
    if selected_month != 'Semua': filtered_df = filtered_df[filtered_df['month'] == selected_month]

    # --- HEADER & KPI ---
    st.markdown("Analisis interaktif untuk memahami pola keterlambatan penerbangan di AS.")
    
    kpi1, kpi2, kpi3 = st.columns(3)
    avg_dep_delay = filtered_df['departure_delay'].mean()
    avg_arr_delay = filtered_df['arrival_delay'].mean()
    total_flights = len(filtered_df)

    kpi1.metric("Rata-rata Delay Keberangkatan", f"{avg_dep_delay:.1f} menit")
    kpi2.metric("Rata-rata Delay Kedatangan", f"{avg_arr_delay:.1f} menit")
    kpi3.metric("Total Penerbangan (Filtered)", f"{total_flights:,}")
    
    st.divider()

    # --- VISUALISASI UTAMA ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Rata-rata Keterlambatan per Maskapai")
        if 'airline_name' in filtered_df.columns:
            avg_delay_per_airline = filtered_df.groupby('airline_name')['arrival_delay'].mean().sort_values().reset_index()
            fig1 = px.bar(avg_delay_per_airline, x='arrival_delay', y='airline_name', orientation='h',
                          labels={'arrival_delay': 'Rata-rata Delay (menit)', 'airline_name': 'Maskapai'},
                          color='arrival_delay', color_continuous_scale='Plasma')
            fig1.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#e2e8f0', yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("Tren Keterlambatan Bulanan")
        avg_delay_per_month = filtered_df.groupby('month')[['departure_delay', 'arrival_delay']].mean().reset_index()
        fig2 = px.line(avg_delay_per_month, x='month', y=['departure_delay', 'arrival_delay'],
                       labels={'value': 'Rata-rata Delay (menit)', 'month': 'Bulan'}, markers=True)
        fig2.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color='#e2e8f0', legend_title_text='Tipe Delay')
        st.plotly_chart(fig2, use_container_width=True)

    # --- TABEL DATA ---
    with st.expander("Lihat Sampel Data (setelah difilter)"):
        st.dataframe(filtered_df.head(1000000))
        st.info(f"Menampilkan 1000000 baris pertama dari total {total_flights:,} baris data yang difilter.")

else:
    st.warning("Gagal memuat data. Pastikan proses machine learning sudah selesai dijalankan.")