#!/usr/bin/env bash
# Helper gia ti zwntani parousiasi.
#
# Vasiki xrhsh (kathe entoli se diaforetiko terminal):
#   bash demo.sh clean              # Sviniei kai ksanaftiaxnei to Kafka topic
#   bash demo.sh stream             # Trexei to Stream consumer (default: 200ms trigger)
#   bash demo.sh batch              # Trexei to Batch consumer (default: 8s window)
#   bash demo.sh send               # Stelnei 5000 events
#   bash demo.sh results            # Deixnei posa arbitrages vrike kathe methodos
#
# Me allagmenes times:
#   bash demo.sh stream 100         # Stream me 100ms trigger
#   bash demo.sh batch 30           # Batch me 30s window
#   bash demo.sh send 10000         # Stelnei 10000 events
#   bash demo.sh send 5000 1        # Stelnei 5000 events se real-time (speed=1)
#
# Optional flag --verbose (i -v) sto telos gia na deis OLA ta Spark logs:
#   bash demo.sh stream -v

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"
PY="$PROJECT_ROOT/venv/bin/python"

if [ ! -x "$PY" ]; then
    echo "ERROR: Den vrethike to Python sto venv ($PY)"
    exit 1
fi

export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PROJECT_ROOT/venv/bin:$PATH"
export DOCKER_CONTEXT="desktop-linux"
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

# Detect verbose flag (-v i --verbose se opoiadipote thesi)
VERBOSE=0
args=()
for a in "$@"; do
    if [ "$a" = "-v" ] || [ "$a" = "--verbose" ]; then
        VERBOSE=1
    else
        args+=("$a")
    fi
done
set -- "${args[@]}"

# Filtraroume to thoryvodes Spark output. Krataei mono tis simantikes grammes
# (started/stopped, batch=N rows=X opps=Y, errors, etc).
run_filtered() {
    if [ "$VERBOSE" -eq 1 ]; then
        # Verbose mode -> deixne ola
        "$@"
    else
        # Filter mode -> krata mono ta xrisima
        "$@" 2>&1 | grep --line-buffered -E \
            'consumer (started|stopped)|batch=[0-9]|sent=[0-9]+ |DONE\. sent|Loaded [0-9]+ matches|Reached --|Created topic|^READY|Traceback|^Error|stream_opps|batch_opps' \
            | grep --line-buffered -vE 'BrokerConnection|Broker version|Set configuration|Probing node|Closing connection'
    fi
}

cmd="${1:-help}"

case "$cmd" in
  clean)
    echo "Adeiazoume to topic betting-odds..."
    docker exec arbitrage-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic betting-odds 2>/dev/null || true
    sleep 2
    docker exec arbitrage-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic betting-odds \
      --partitions 3 --replication-factor 1
    rm -rf benchmarks/stream_checkpoint
    rm -f benchmarks/results/*.jsonl
    echo "READY"
    ;;

  stream)
    trigger="${2:-200}"
    duration="${3:-180}"
    echo ">>> STREAM consumer ksekinaei (trigger=${trigger}ms, duration=${duration}s)"
    echo ">>> Perimeneis 10sec gia Spark startup..."
    run_filtered "$PY" -m consumers.stream.stream_consumer \
      --trigger-ms "$trigger" --duration-seconds "$duration"
    ;;

  batch)
    window="${2:-8}"
    duration="${3:-180}"
    echo ">>> BATCH consumer ksekinaei (window=${window}s, duration=${duration}s)"
    echo ">>> Perimeneis 10sec gia Spark startup..."
    run_filtered "$PY" -m consumers.batch.batch_consumer \
      --window-seconds "$window" --duration-seconds "$duration"
    ;;

  send)
    limit="${2:-5000}"
    speed="${3:-0}"
    echo ">>> Stelnoume $limit events (speed=$speed, 0=max)"
    run_filtered "$PY" -m producer.producer --speed "$speed" --limit "$limit"
    ;;

  results)
    echo "=== STREAM opps ==="
    if [ -f benchmarks/results/stream_opps.jsonl ]; then
      n=$(wc -l < benchmarks/results/stream_opps.jsonl)
      echo "Synolo: $n arbitrages"
      cat benchmarks/results/stream_opps.jsonl | "$PY" -c "
import json, sys
for i, line in enumerate(sys.stdin, 1):
    o = json.loads(line)
    print(f'  {i}. margin={o[\"profit_margin\"]*100:5.2f}%  {o[\"match_id\"]}')
" 2>/dev/null
    fi
    echo ""
    echo "=== BATCH opps ==="
    if [ -f benchmarks/results/batch_opps.jsonl ]; then
      n=$(wc -l < benchmarks/results/batch_opps.jsonl)
      echo "Synolo: $n arbitrages"
      cat benchmarks/results/batch_opps.jsonl | "$PY" -c "
import json, sys
for i, line in enumerate(sys.stdin, 1):
    o = json.loads(line)
    print(f'  {i}. margin={o[\"profit_margin\"]*100:5.2f}%  {o[\"match_id\"]}')
" 2>/dev/null
    fi
    ;;

  *)
    echo "Xrhsh: bash demo.sh [clean|stream|batch|send|results] [params...] [-v]"
    echo ""
    echo "  clean                    Adeiazei to topic"
    echo "  stream [trigger_ms]      Default 200ms"
    echo "  batch  [window_sec]      Default 8s"
    echo "  send   [limit] [speed]   Default 5000 events, speed=0 (max)"
    echo "  results                  Deixnei opps"
    echo ""
    echo "  -v / --verbose           Deixne kai ta Spark logs"
    echo ""
    echo "Paradeigmata:"
    echo "  bash demo.sh stream 50          # pio responsive Stream"
    echo "  bash demo.sh batch 30           # megalo window — fanerin i diafora"
    echo "  bash demo.sh send 10000         # diplo dataset"
    echo "  bash demo.sh batch -v           # me ola ta Spark logs"
    ;;
esac
