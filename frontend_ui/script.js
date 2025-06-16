document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('prediction-form');
    const resultDiv = document.getElementById('result');
    const predictionOutput = document.getElementById('prediction-output');
    const submitBtn = document.getElementById('submit-btn');

    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const originalButtonText = submitBtn.textContent;
        submitBtn.textContent = 'Menganalisis...';
        submitBtn.disabled = true;

        const formData = new FormData(form);
        const data = {
            year: parseInt(formData.get('year')),
            month: parseInt(formData.get('month')),
            day: parseInt(formData.get('day')),
            day_of_week: parseInt(formData.get('day_of_week')),
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

            if (result.error) { throw new Error(result.error); }
            
            const delay = result.predicted_arrival_delay;
            
            // Logika pewarnaan baru sesuai tema
            if (delay > 15) {
                predictionOutput.textContent = `~${delay.toFixed(1)} menit telat`;
                predictionOutput.style.color = "#ef4444"; // Merah terang
            } else if (delay > 0) {
                 predictionOutput.textContent = `~${delay.toFixed(1)} menit telat`;
                 predictionOutput.style.color = "#f97316"; // Oranye terang
            } else {
                predictionOutput.textContent = `Tepat Waktu!`;
                predictionOutput.style.color = "#22c55e"; // Hijau terang
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
});