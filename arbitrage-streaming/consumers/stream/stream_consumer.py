"""
Stream consumer (Methodos B) — Spark Structured Streaming.

Idea:
To Spark Structured Streaming diavazei to Kafka se micro-batches molis ftasoun
nea messages. Gia kathe micro-batch trexoume ena `foreachBatch` callback pou:
  1. Enimerwnei ena in-memory state dict: latest odds ana (match_id, bookmaker).
  2. Gia kathe match pou perase update se auto to batch, ksanatrexei arbitrage detection.
  3. Grafei tis eukairies sto results/stream_opps.jsonl.
  4. Grafei to micro-batch metric sto results/stream_metrics.jsonl.
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import signal
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from arbitrage.detector import detect_arbitrage  # noqa: E402


logger = logging.getLogger("stream_consumer")


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


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )


class StreamingState:
    """In-memory state me ta latest odds ana (match_id, bookmaker).

    Domi:
      latest[match_id][bookmaker] = (event_time_ms, odds_home, odds_draw, odds_away, real_time_ms)

    Episis kratame ta opps pou exoume idi emit-arei, wste na min ksanagrafontai
    sto output kathe fora pou ta odds zaliountai liges fores enw to arb einai
    akoma energo. Idia opp ksanagrafetai mono an to margin allaksei panw apo
    ena mikro epsilon.
    """

    def __init__(
        self,
        opps_path: Path,
        metrics_path: Path,
        max_duration_s: float | None,
        max_batches: int | None,
    ):
        self.latest: dict[str, dict[str, tuple[int, float, float, float, int]]] = {}
        self.last_emitted_margin: dict[str, float] = {}
        self.opps_path = opps_path
        self.metrics_path = metrics_path
        self.batch_idx = 0
        self.total_events = 0
        self.total_opps = 0
        self.start_wall = time.monotonic()
        self.max_duration_s = max_duration_s
        self.max_batches = max_batches
        self.stop = False

    def process_batch(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        """Auto kaleitai apo to Spark Structured Streaming gia kathe micro-batch.

        Kanoume oli ti douleia se driver-side Python — gia to scale mas einai
        fine, kai mas afinei na ksanaxrisimopoiisoume tin idia detect_arbitrage()
        opws kai sto batch consumer.
        """
        t0 = time.monotonic()
        rows = micro_batch_df.collect()
        n_rows = len(rows)
        self.batch_idx += 1
        self.total_events += n_rows

        touched_matches: set[str] = set()
        for r in rows:
            mid = r["match_id"]
            book = r["bookmaker"]
            ev = (
                int(r["event_time_ms"]),
                float(r["odds_home"]),
                float(r["odds_draw"]),
                float(r["odds_away"]),
                int(r["real_time_ms"]),
            )
            cur = self.latest.setdefault(mid, {}).get(book)
            # Kratame mono to pio prosfato update ana (match, bookmaker)
            if cur is None or ev[0] >= cur[0]:
                self.latest[mid][book] = ev
                touched_matches.add(mid)

        # Ksanaelenxoume arbitrage mono gia ta matches pou pirane nea data se auto to batch
        now_ms = int(time.time() * 1000)
        new_opps: list[dict] = []
        for mid in touched_matches:
            book_map = self.latest[mid]
            if len(book_map) < 2:
                continue
            home_odds = {b: v[1] for b, v in book_map.items()}
            draw_odds = {b: v[2] for b, v in book_map.items()}
            away_odds = {b: v[3] for b, v in book_map.items()}
            opp = detect_arbitrage(
                match_id=mid,
                home_odds=home_odds,
                draw_odds=draw_odds,
                away_odds=away_odds,
                detected_at_ms=now_ms,
            )
            if opp is None:
                # Den einai pia kerdofori — sviname to memo wste an ksanagini
                # arbitrage stin sygkekrimeni anametrisi na to grapsoume.
                self.last_emitted_margin.pop(mid, None)
                continue

            # De-dupe: skip an proigoumeniks emit me ~idio margin
            prev = self.last_emitted_margin.get(mid)
            if prev is not None and abs(prev - opp.profit_margin) < 1e-4:
                continue
            self.last_emitted_margin[mid] = opp.profit_margin

            max_real_time = max(v[4] for v in book_map.values())
            new_opps.append({
                "method": "stream",
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
                "max_real_time_ms": max_real_time,
                "e2e_latency_ms": now_ms - max_real_time,
            })

        if new_opps:
            with self.opps_path.open("a") as f:
                for o in new_opps:
                    f.write(json.dumps(o) + "\n")
            self.total_opps += len(new_opps)

        t1 = time.monotonic()
        metric = {
            "batch": self.batch_idx,
            "spark_batch_id": int(batch_id),
            "rows_read": n_rows,
            "touched_matches": len(touched_matches),
            "opps_found": len(new_opps),
            "processing_ms": int((t1 - t0) * 1000),
            "wall_time_ms": int(time.time() * 1000),
        }
        with self.metrics_path.open("a") as f:
            f.write(json.dumps(metric) + "\n")

        logger.info(
            "batch=%d  rows=%d  touched=%d  opps=%d  ms=%d  (total events=%d opps=%d)",
            self.batch_idx, n_rows, len(touched_matches), len(new_opps),
            metric["processing_ms"], self.total_events, self.total_opps,
        )

        # Stop conditions
        if self.max_batches is not None and self.batch_idx >= self.max_batches:
            logger.info("Reached --max-batches %d", self.max_batches)
            self.stop = True
        if self.max_duration_s is not None and \
           (time.monotonic() - self.start_wall) >= self.max_duration_s:
            logger.info("Reached --duration-seconds %.1f", self.max_duration_s)
            self.stop = True


def run(
    bootstrap_servers: str,
    topic: str,
    trigger_ms: int,
    starting_offsets: str,
    max_batches: int | None,
    duration_seconds: float | None,
    results_dir: Path,
    checkpoint_dir: Path,
) -> None:
    spark = build_spark("arbitrage-stream")
    spark.sparkContext.setLogLevel("WARN")

    results_dir.mkdir(parents=True, exist_ok=True)
    opps_path = results_dir / "stream_opps.jsonl"
    metrics_path = results_dir / "stream_metrics.jsonl"
    opps_path.write_text("")
    metrics_path.write_text("")

    # Fresh checkpoint gia kathe run, wste na min synexizei apo palio offset
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    state = StreamingState(
        opps_path=opps_path,
        metrics_path=metrics_path,
        max_duration_s=duration_seconds,
        max_batches=max_batches,
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), ODDS_SCHEMA).alias("e"))
           .select("e.*")
           .filter("match_id is not null")
    )

    def _foreach_batch(df: DataFrame, batch_id: int) -> None:
        state.process_batch(df, batch_id)

    query: StreamingQuery = (
        events.writeStream
        .foreachBatch(_foreach_batch)
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(processingTime=f"{trigger_ms} milliseconds")
        .start()
    )

    def _sig(signum, frame):
        logger.warning("Signal %d -- stopping stream gracefully.", signum)
        state.stop = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    logger.info(
        "Stream consumer started. trigger=%dms  topic=%s  starting_offsets=%s",
        trigger_ms, topic, starting_offsets,
    )

    try:
        # Polling tou stop flag oso to query trexei
        while query.isActive and not state.stop:
            query.awaitTermination(timeout=1.0)
    finally:
        query.stop()
        spark.stop()
        logger.info(
            "Stream consumer stopped. batches=%d  events=%d  opps=%d",
            state.batch_idx, state.total_events, state.total_opps,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="betting-odds")
    parser.add_argument("--trigger-ms", type=int, default=200,
                        help="Spark processing-time trigger interval se ms. "
                             "Mikrotero = pio responsive alla pio polly overhead.")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--duration-seconds", type=float, default=None)
    parser.add_argument("--results-dir", type=Path,
                        default=PROJECT_ROOT / "benchmarks" / "results")
    parser.add_argument("--checkpoint-dir", type=Path,
                        default=PROJECT_ROOT / "benchmarks" / "stream_checkpoint")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    run(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        trigger_ms=args.trigger_ms,
        starting_offsets=args.starting_offsets,
        max_batches=args.max_batches,
        duration_seconds=args.duration_seconds,
        results_dir=args.results_dir,
        checkpoint_dir=args.checkpoint_dir,
    )


if __name__ == "__main__":
    main()
