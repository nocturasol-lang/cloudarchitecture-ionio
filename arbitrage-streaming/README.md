# Arbitrage Streaming Detection

Εξαμηνιαία εργασία στο μάθημα **Πλατφόρμες και Αρχιτεκτονικές Νέφους** (Ιόνιο Πανεπιστήμιο).

## Τι είναι αυτό

Σύστημα που εντοπίζει σε real-time arbitrage ευκαιρίες σε αποδόσεις στοιχηματικών εταιριών χρησιμοποιώντας **Apache Kafka** + **Apache Spark**. Συγκρίνει δύο προσεγγίσεις:

1. **Batch Processing** — μαζεύει δεδομένα σε χρονικά παράθυρα και επεξεργάζεται όλα μαζί.
2. **Stream Processing** — επεξεργάζεται κάθε νέα απόδοση αμέσως μόλις φτάσει.

## Αρχιτεκτονική

```
CSV (ιστορικά odds) → producer.py → Kafka → [Batch Consumer + Stream Consumer] → arbitrage + metrics
```

## Setup

```bash
# 1. Virtual environment
python3.11 -m venv venv
source scripts/activate.sh
pip install -r requirements.txt

# 2. Δεδομένα
bash scripts/download_data.sh

# 3. Kafka (Docker)
docker compose -f docker/docker-compose.yml up -d

# 4. Producer (σε ένα terminal)
python -m producer.producer --speed 0 --limit 5000

# 5. Consumers (σε δύο ξεχωριστά terminals)
python -m consumers.batch.batch_consumer --window-seconds 10 --duration-seconds 60
python -m consumers.stream.stream_consumer --trigger-ms 200 --duration-seconds 60

# 6. Πειραματική σύγκριση
python -m benchmarks.run_experiment --tag fast --speed 0 --producer-limit 10000 \
    --batch-window 10 --stream-trigger-ms 200 --drain-seconds 25
python -m benchmarks.analyze_results --tag fast
```

## Δομή

| Φάκελος | Περιεχόμενο |
|---|---|
| `data/` | Datasets (gitignored) |
| `docker/` | Kafka docker-compose |
| `producer/` | Replay CSV ως Kafka stream |
| `arbitrage/` | Κοινή logic για detection |
| `consumers/batch/` | Spark Batch consumer |
| `consumers/stream/` | Spark Streaming consumer |
| `benchmarks/` | Πειραματικές μετρήσεις + γραφήματα |
| `notebooks/` | Jupyter exploration |
| `report/` | Τελική αναφορά |
