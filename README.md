# Εντοπισμός Arbitrage σε Πραγματικό Χρόνο με Apache Kafka & Apache Spark

Εξαμηνιαία εργασία στο μάθημα **Πλατφόρμες και Αρχιτεκτονικές Νέφους** (Ιόνιο Πανεπιστήμιο, Τμήμα Πληροφορικής, 2025–26).

Σύγκριση **Batch vs Stream Processing** για την ανίχνευση arbitrage ευκαιριών σε αποδόσεις στοιχηματικών εταιριών, πάνω σε ροή δεδομένων από 14.802 ποδοσφαιρικούς αγώνες (8 ευρωπαϊκές λίγκες × 5 σεζόν, 6 bookmakers).

**Φοιτητές:** Χρήστος Καρασακαλίδης (inf2021082), Ευγένιος Χριστόπουλος (inf2021249)

## Δομή

| Φάκελος | Περιεχόμενο |
|---|---|
| [`arbitrage-streaming/`](arbitrage-streaming/) | Ο κώδικας: producer, Batch & Stream consumers, κοινή λογική ανίχνευσης, benchmarks |
| [`report/`](report/) | Η αναφορά ([HTML](report/report.html) / [PDF](report/report.pdf)) με 18 σχήματα και την πειραματική αξιολόγηση |

## Αρχιτεκτονική

```
CSV (ιστορικά odds) → producer.py → Kafka (3 partitions) → [Batch Consumer | Stream Consumer] → arbitrage + metrics
```

Και οι δύο consumers μοιράζονται την ίδια `detect_arbitrage()` — ό,τι διαφορά μετράμε οφείλεται αποκλειστικά στο processing paradigm.

## Βασικά ευρήματα

- **Ίδια ορθότητα**: αμφότερες οι μέθοδοι εντοπίζουν τις ίδιες ευκαιρίες.
- **Latency**: Stream ~1,5 s σταθερά· Batch ≈ το παράθυρό του (10 s window → 9,75 s, 30 s → 29,87 s).
- **Throughput**: Stream 7–8× υψηλότερο στο συγκεκριμένο setup (ο Batch χάνει χρόνο σε idle waits).

## Εκτέλεση

Δείτε τις αναλυτικές οδηγίες στο [`arbitrage-streaming/README.md`](arbitrage-streaming/README.md). Συνοπτικά:

```bash
cd arbitrage-streaming
python3.11 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
bash scripts/download_data.sh                     # κατέβασμα dataset
docker compose -f docker/docker-compose.yml up -d # Kafka (KRaft) + Kafka UI
bash demo.sh clean && bash demo.sh stream         # terminal 2
bash demo.sh batch 8                              # terminal 3
bash demo.sh send 5000                            # terminal 4
bash demo.sh results
```
