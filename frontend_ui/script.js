document.addEventListener('DOMContentLoaded', () => {
    // Ambil semua elemen yang dibutuhkan dari halaman
    const form = document.getElementById('prediction-form');
    const resultDiv = document.getElementById('result');
    const predictionOutput = document.getElementById('prediction-output');
    const submitBtn = document.getElementById('submit-btn');
    const reviewsContainer = document.getElementById('reviews-container');
    const reviewsList = document.getElementById('reviews-list');

    // --- FUNGSI BARU: Mengubah sentimen menjadi bintang ---
    function mapSentimentToStars(sentiment) {
        switch (sentiment) {
            case 'Positive':
                return '★★★★★'; // 5 bintang untuk positif
            case 'Negative':
                return '★☆☆☆☆'; // 1 bintang untuk negatif
            case 'Neutral':
            default:
                return '★★★☆☆'; // 3 bintang untuk netral
        }
    }

    // --- Fungsi untuk mengambil dan menampilkan ulasan ---
    async function fetchAndDisplayReviews() {
        try {
            const response = await fetch('http://localhost:8000/reviews?count=3');
            if (!response.ok) return;
            
            const reviews = await response.json();
            reviewsList.innerHTML = ''; // Kosongkan daftar sebelum mengisi
            
            reviews.forEach(review => {
                const listItem = document.createElement('li');
                listItem.className = 'review-item'; // Kelas generik untuk semua item

                // PERBAIKAN UTAMA: Buat struktur HTML baru yang lebih kaya
                const stars = mapSentimentToStars(review.sentiment);
                
                listItem.innerHTML = `
                    <div class="review-header">
                        <span class="review-author">Penumpang Anonim</span>
                        <span class="review-stars">${stars}</span>
                    </div>
                    <p class="review-text">"${review.text}"</p>
                `;
                reviewsList.appendChild(listItem);
            });

            reviewsContainer.style.display = 'block';

        } catch (error) {
            console.error("Terjadi error saat mengambil ulasan:", error);
        }
    }

    // Panggil fungsi ulasan saat halaman pertama kali dimuat
    fetchAndDisplayReviews();

    // Event listener untuk form prediksi (tidak ada perubahan di sini)
    if (form) {
        form.addEventListener('submit', async (event) => {
            event.preventDefault();
            const originalButtonText = submitBtn.textContent;
            submitBtn.textContent = 'Menganalisis...';
            submitBtn.disabled = true;

            const formData = new FormData(form);
            
            const data = {
                month: parseInt(formData.get('month')),
                day: parseInt(formData.get('day')),
                airline: formData.get('airline'),
                departure_delay: parseInt(formData.get('departure_delay')),
                distance: parseInt(formData.get('distance'))
            };

            try {
                const response = await fetch('http://localhost:8000/predict', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });

                const result = await response.json();

                if (!response.ok) {
                    throw new Error(result.detail || 'Terjadi kesalahan pada server.');
                }
                
                const delay = result.predicted_arrival_delay;
                
                if (delay > 15) {
                    predictionOutput.textContent = `Diperkirakan sangat telat (~${delay.toFixed(0)} menit)`;
                    predictionOutput.style.color = "#ef4444";
                } else if (delay > 0) {
                    predictionOutput.textContent = `Diperkirakan sedikit telat (~${delay.toFixed(0)} menit)`;
                    predictionOutput.style.color = "#f97316";
                } else {
                    const earlyMinutes = Math.abs(delay).toFixed(0);
                    predictionOutput.textContent = `Diperkirakan Tepat Waktu atau Lebih Awal ${earlyMinutes} menit!`;
                    predictionOutput.style.color = "#22c55e";
                }
                resultDiv.style.display = 'block';

            } catch (error) {
                predictionOutput.textContent = `Error: ${error.message}`;
                predictionOutput.style.color = "#ef4444";
                resultDiv.style.display = 'block';
            } finally {
                submitBtn.textContent = originalButtonText;
                submitBtn.disabled = false;
            }
        });
    }
});
