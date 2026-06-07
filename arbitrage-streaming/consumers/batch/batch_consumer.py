"""
Batch consumer (Methodos A) — Spark Batch.

Eimaste san ena "scheduled job pou trexei kathe N deuterolepta". Kathe iteration:
  1. Diavazei ola ta messages pou exoun mpei sto topic apo to teleutaio commit.
  2. Ta omadopoiei ana match.
  3. Gia kathe match, kratei to LATEST odds ana stoixhmatiki.
  4. Trexei to arbitrage detection ana match.
  5. Grafei opoies eukairies vrei sto results/batch_opps.jsonl.
  6. Grafei ta timing metrics sto results/batch_metrics.jsonl.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from kafka import KafkaConsumer, TopicPartition

from arbitrage.detector import detect_arbitrage  # noqa: E402


logger = logging.getLogger("batch_consumer")


# Schema enos OddsUpdate JSON message — gia to parsing twn Kafka values
ODDS_SCHEMA = StructType([
    StructField("match_id",      StringType(), False),
    StructField("league",        StringType(), True),
    StructField("home_team",     StringType(), True),
    StructField("away_team",     StringType(), True),
    StructField("match_date",    StringType(), True),
    StructField("bookmaker",     StringType(), False),
    StructField("odds_home",     DoubleType(), False),
    StructField("odds_draw",     DoubleType(), False),
    StructField("odds_away",     DoubleType(), False),
    StructField("event_time_ms", LongType(),   False),
    StructField("real_time_ms",  LongType(),   False),
])


def fetch_end_offsets(bootstrap_servers: str, topic: str) -> dict[int, int]:
    """Epistrefei {partition_id: next_offset} me to current tail tou topic."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in partitions]
        if not tps:
            return {}
        end = consumer.end_offsets(tps)
        return {tp.partition: int(off) for tp, off in end.items()}
    finally:
        consumer.close()


def build_spark(app_name: str) -> SparkSession:
    """Ftiaxnei ena SparkSession me to Kafka package sto classpath."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # To Spark thelei to Kafka connector jar sto runtime.
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )


def latest_per_bookmaker(df):
    """Gia kathe (match_id, bookmaker), kratei tin pio prosfati grammi odds."""
    w = F.expr("row_number() over (partition by match_id, bookmaker "
               "order by event_time_ms desc, real_time_ms desc)")
    return df.withColumn("_rn", w).filter("_rn = 1").drop("_rn")


def detect_for_partition(rows_iter):
    """Pernei iterator apo Row objects, omadopoiei ana match kai trexei
    arbitrage detection se kathari Python. Vgazei 0 i 1 opportunity dict ana match.
    """
    by_match: dict[str, list] = {}
    for r in rows_iter:
        by_match.setdefault(r.match_id, []).append(r)

    now_ms = int(time.time() * 1000)
    for match_id, rows in by_match.items():
        home_odds = {r.bookmaker: float(r.odds_home) for r in rows}
        draw_odds = {r.bookmaker: float(r.odds_draw) for r in rows}
        away_odds = {r.bookmaker: float(r.odds_away) for r in rows}
        if len(home_odds) < 2:
            continue
        max_event_time = max(int(r.event_time_ms) for r in rows)
        max_real_time = max(int(r.real_time_ms) for r in rows)
        opp = detect_arbitrage(
            match_id=match_id,
            home_odds=home_odds,
            draw_odds=draw_odds,
            away_odds=away_odds,
            detected_at_ms=now_ms,
        )
        if opp is None:
            continue
        yield {
            "method": "batch",
            "match_id": opp.match_id,
            "best_book_home": opp.best_book_home,
            "best_odds_home": opp.best_odds_home,
            "best_book_draw": opp.best_book_draw,
            "best_odds_draw": opp.best_odds_draw,
            "best_book_away": opp.best_book_away,
            "best_odds_away": opp.best_odds_away,
            "profit_margin": opp.profit_margin,
            "implied_prob_sum": opp.implied_prob_sum,
            "detected_at_ms": opp.detected_at_ms,
            "max_event_time_ms": max_event_time,
            "max_real_time_ms": max_real_time,
            # End-to-end latency = detection wall-clock - teleutaio producer send
            "e2e_latency_ms": now_ms - max_real_time,
        }


def run(
    bootstrap_servers: str,
    topic: str,
    window_seconds: float,
    max_batches: int | None,
    duration_seconds: float | None,
    results_dir: Path,
) -> None:
    spark = build_spark("arbitrage-batch")
    spark.sparkContext.setLogLevel("WARN")

    results_dir.mkdir(parents=True, exist_ok=True)
    opps_path = results_dir / "batch_opps.jsonl"
    metrics_path = results_dir / "batch_metrics.jsonl"

    stop = {"value": False}

    def _sig(signum, frame):
        logger.warning("Signal %d -- finishing current batch and exiting.", signum)
        stop["value"] = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    # Kratame Kafka offsets gia na diavazei kathe batch MONO ta nea messages.
    # cursor[partition] = next_offset_to_read gia kathe partition
    cursor: dict[int, int] = {}
    batch_num = 0
    start_wall = time.monotonic()

    # Adeiasma twn output files gia fresh run
    opps_path.write_text("")
    metrics_path.write_text("")

    logger.info("Batch consumer started. window=%.1fs  topic=%s", window_seconds, topic)

    while not stop["value"]:
        if max_batches is not None and batch_num >= max_batches:
            logger.info("Reached --max-batches %d, stopping.", max_batches)
            break
        if duration_seconds is not None and (time.monotonic() - start_wall) >= duration_seconds:
            logger.info("Reached --duration-seconds %.1f, stopping.", duration_seconds)
            break

        batch_num += 1
        t0 = time.monotonic()

        # Vima 1: vriskoume ta bookends gia auto to batch read
        end_offsets = fetch_end_offsets(bootstrap_servers, topic)
        if not end_offsets:
            logger.warning("Topic %s has no partitions yet — waiting.", topic)
            time.sleep(window_seconds)
            continue

        # Prwth iteration: arxizoume apo EARLIEST gia na piasoume to backlog
        if not cursor:
            cursor = {p: 0 for p in end_offsets}

        starting_offsets_json = json.dumps({topic: {str(p): cursor.get(p, 0) for p in end_offsets}})
        ending_offsets_json = json.dumps({topic: {str(p): end_offsets[p] for p in end_offsets}})

        # An den irthe tipota neo apo to proigoumeno batch -> skip
        new_msgs = sum(max(end_offsets[p] - cursor.get(p, 0), 0) for p in end_offsets)
        if new_msgs == 0:
            t1 = time.monotonic()
            metric = {
                "batch": batch_num, "rows_read": 0, "opps_found": 0,
                "processing_ms": int((t1 - t0) * 1000),
                "wall_time_ms": int(time.time() * 1000),
            }
            with metrics_path.open("a") as f:
                f.write(json.dumps(metric) + "\n")
            logger.info("batch=%d  empty  ms=%d", batch_num, metric["processing_ms"])
            time.sleep(max(window_seconds - (t1 - t0), 0))
            continue

        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets_json)
            .option("endingOffsets", ending_offsets_json)
            .option("failOnDataLoss", "false")
            .load()
        )

        events = (
            raw.select(F.from_json(F.col("value").cast("string"), ODDS_SCHEMA).alias("e"))
               .select("e.*")
               .filter("match_id is not null")
        )

        # Vima 2: latest odds ana (match, bookmaker), meta arbitrage detection
        snapshot = latest_per_bookmaker(events)
        rows_read = snapshot.count()

        if rows_read == 0:
            t1 = time.monotonic()
            metric = {
                "batch": batch_num,
                "rows_read": 0,
                "opps_found": 0,
                "processing_ms": int((t1 - t0) * 1000),
                "wall_time_ms": int(time.time() * 1000),
            }
            with metrics_path.open("a") as f:
                f.write(json.dumps(metric) + "\n")
            logger.info("batch=%d  empty  ms=%d", batch_num, metric["processing_ms"])
            # Den irthe tipota neo => perimenoume ena window
            time.sleep(max(window_seconds - (t1 - t0), 0))
            continue

        # Vima 3: arbitrage detection sto driver-side Python.
        # Gia mas (xiliades matches) einai entaksei kai diatirei tin idia logiki me to stream.
        opps = list(detect_for_partition(snapshot.collect()))

        # Vima 4: grafoume tis eukairies kai to metric
        if opps:
            with opps_path.open("a") as f:
                for o in opps:
                    f.write(json.dumps(o) + "\n")

        t1 = time.monotonic()
        metric = {
            "batch": batch_num,
            "rows_read": int(rows_read),
            "opps_found": len(opps),
            "processing_ms": int((t1 - t0) * 1000),
            "wall_time_ms": int(time.time() * 1000),
        }
        with metrics_path.open("a") as f:
            f.write(json.dumps(metric) + "\n")

        logger.info(
            "batch=%d  rows=%d  opps=%d  ms=%d",
            batch_num, rows_read, len(opps), metric["processing_ms"],
        )

        # Proxoroume ton cursor: to epomeno batch ksekinaei opou stamatise auto
        cursor = dict(end_offsets)

        # Sleep gia na pernaei to window (afairwntas to processing time)
        elapsed = t1 - t0
        sleep_for = max(window_seconds - elapsed, 0)
        time.sleep(sleep_for)

    spark.stop()
    logger.info("Batch consumer stopped after %d batches.", batch_num)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="betting-odds")
    parser.add_argument("--window-seconds", type=float, default=30.0,
                        help="Polling window — kathe poso trexei ena batch.")
    parser.add_argument("--max-batches", type=int, default=None,
                        help="Stamatima meta apo N batches.")
    parser.add_argument("--duration-seconds", type=float, default=None,
                        help="Stamatima meta apo N deuterolepta synolika.")
    parser.add_argument("--results-dir", type=Path,
                        default=PROJECT_ROOT / "benchmarks" / "results")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    run(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        window_seconds=args.window_seconds,
        max_batches=args.max_batches,
        duration_seconds=args.duration_seconds,
        results_dir=args.results_dir,
    )


if __name__ == "__main__":
    main()
