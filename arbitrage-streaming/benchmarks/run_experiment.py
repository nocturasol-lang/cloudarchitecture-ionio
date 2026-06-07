"""
Trexei ena head-to-head experiment: producer + batch + stream consumer
paralila se ena katharo Kafka topic.

Ti kanei:
  1. Ksanaftiaxnei to Kafka topic (clean state).
  2. Ksekinaei to batch consumer se subprocess.
  3. Ksekinaei to stream consumer se subprocess.
  4. Perimenei liga deuterolepta gia warmup tou Spark.
  5. Ksekinaei to producer.
  6. Perimenei na teleiosei o producer + ekstra drain period.
  7. Stamataei tous consumers omala.
  8. Antigrafei ta metrics se tagged subdir wste na min ksanagrafontai apo epomena runs.

Xrhsh apo to project root:
    source scripts/activate.sh
    python -m benchmarks.run_experiment \
        --tag fast --speed 0 --producer-limit 30000 \
        --batch-window 30 --stream-trigger-ms 200 \
        --drain-seconds 30
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


logger = logging.getLogger("experiment")


def recreate_topic(topic: str) -> None:
    """Svinei kai ksanaftiaxnei to Kafka topic gia katharo experiment."""
    env = {**os.environ, "DOCKER_CONTEXT": "desktop-linux"}
    for action in ("delete", "create"):
        cmd = [
            "docker", "exec", "arbitrage-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            f"--{action}", "--topic", topic,
        ]
        if action == "create":
            cmd += ["--partitions", "3", "--replication-factor", "1"]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        # To delete apotygxanei an den yparxei to topic — to agnooume
        if result.returncode != 0 and action == "create":
            raise RuntimeError(f"topic {action} failed: {result.stderr}")
    # To Kafka thelei ligo xrono na "katsei" meta to recreate
    time.sleep(2)
    logger.info("Topic %s recreated (3 partitions).", topic)


def start_consumer(
    module: str,
    log_file: Path,
    extra_args: list[str],
) -> subprocess.Popen:
    """Ksekinaei ena consumer se diko tou process wste na min mas blocarei."""
    python = sys.executable
    cmd = [python, "-m", module, *extra_args]
    f = log_file.open("w")
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=f,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,   # gia na mporoume na stilo signal sto group
    )
    logger.info("Started %s (pid=%d, log=%s)", module, proc.pid, log_file)
    return proc


def stop_process(proc: subprocess.Popen, name: str, timeout: float = 30.0) -> None:
    """Stelnei SIGINT sto process group, perimenei, meta SIGKILL an akoma trexei."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout)
        logger.info("%s stopped gracefully.", name)
    except subprocess.TimeoutExpired:
        logger.warning("%s didn't stop in %ds, killing.", name, timeout)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=5)


def save_tagged_results(results_dir: Path, tag: str) -> Path:
    """Antigrafei ta neografimena *_metrics.jsonl kai *_opps.jsonl se ena tagged
    subdir, etsi wste ta epomena experiments na min ta ksanagrapsoun."""
    out = results_dir / f"experiment_{tag}"
    out.mkdir(parents=True, exist_ok=True)
    for name in ("batch_metrics.jsonl", "batch_opps.jsonl",
                 "stream_metrics.jsonl", "stream_opps.jsonl"):
        src = results_dir / name
        if src.exists():
            shutil.copy(src, out / name)
    logger.info("Saved tagged results to %s", out)
    return out


def write_experiment_config(out_dir: Path, args: argparse.Namespace) -> None:
    """Sozei ta args pou xrisimopoiithikan wste na mporei na reproduce-aristi to context."""
    cfg = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    cfg["timestamp"] = int(time.time() * 1000)
    (out_dir / "config.json").write_text(json.dumps(cfg, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True,
                        help="Label gia auto to experiment, mpainei sto onoma tou folder.")
    parser.add_argument("--topic", default="betting-odds")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")

    # Producer params
    parser.add_argument("--speed", type=float, default=0,
                        help="Producer replay speed (0 = max throughput)")
    parser.add_argument("--producer-limit", type=int, default=None,
                        help="Posa events na paragei (None = ola)")
    parser.add_argument("--producer-warmup", type=float, default=5.0,
                        help="Deuterolepta anamonis meta to start ton consumers "
                             "prin to producer (gia na zestathei to Spark).")

    # Batch consumer params
    parser.add_argument("--batch-window", type=float, default=30.0,
                        help="Batch consumer polling window se deuterolepta")

    # Stream consumer params
    parser.add_argument("--stream-trigger-ms", type=int, default=200,
                        help="Stream consumer micro-batch trigger interval")

    # End conditions
    parser.add_argument("--drain-seconds", type=float, default=30.0,
                        help="Meta to producer, perimenei toso wste oi consumers "
                             "na epexergastoun olo to backlog.")

    parser.add_argument("--results-dir", type=Path,
                        default=PROJECT_ROOT / "benchmarks" / "results")
    parser.add_argument("--logs-dir", type=Path,
                        default=PROJECT_ROOT / "benchmarks" / "logs")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] [orchestrator] %(message)s",
    )

    args.results_dir.mkdir(parents=True, exist_ok=True)
    args.logs_dir.mkdir(parents=True, exist_ok=True)

    # Vima 1: katharos Kafka state
    recreate_topic(args.topic)

    # Vima 2: ksekiname consumers (tha piasoun apo tin arxh tou topic)
    batch_proc = start_consumer(
        "consumers.batch.batch_consumer",
        args.logs_dir / f"batch_{args.tag}.log",
        ["--bootstrap-servers", args.bootstrap_servers,
         "--topic", args.topic,
         "--window-seconds", str(args.batch_window)],
    )
    stream_proc = start_consumer(
        "consumers.stream.stream_consumer",
        args.logs_dir / f"stream_{args.tag}.log",
        ["--bootstrap-servers", args.bootstrap_servers,
         "--topic", args.topic,
         "--trigger-ms", str(args.stream_trigger_ms)],
    )

    try:
        # Vima 3: warmup — afinoume to Spark na zestathei prin steiloume data
        logger.info("Warming up consumers for %.1fs ...", args.producer_warmup)
        time.sleep(args.producer_warmup)

        # Vima 4: producer
        prod_cmd = [sys.executable, "-m", "producer.producer",
                    "--bootstrap-servers", args.bootstrap_servers,
                    "--topic", args.topic,
                    "--speed", str(args.speed)]
        if args.producer_limit is not None:
            prod_cmd += ["--limit", str(args.producer_limit)]

        prod_log = args.logs_dir / f"producer_{args.tag}.log"
        with prod_log.open("w") as pf:
            logger.info("Starting producer (limit=%s, speed=%s) -> %s",
                        args.producer_limit, args.speed, prod_log)
            t_prod_start = time.monotonic()
            prod_result = subprocess.run(
                prod_cmd, cwd=str(PROJECT_ROOT),
                stdout=pf, stderr=subprocess.STDOUT,
            )
            t_prod_end = time.monotonic()
            logger.info("Producer finished in %.1fs (exit=%d)",
                        t_prod_end - t_prod_start, prod_result.returncode)

        # Vima 5: drain — dinoume xrono stous consumers na teleiosoun to backlog
        logger.info("Draining for %.1fs to let consumers finish ...", args.drain_seconds)
        time.sleep(args.drain_seconds)

    finally:
        # Vima 6: stamatima consumers (panta, akoma kai an exoume error)
        stop_process(stream_proc, "stream_consumer")
        stop_process(batch_proc, "batch_consumer")

    # Vima 7: snapshot ton results
    out_dir = save_tagged_results(args.results_dir, args.tag)
    write_experiment_config(out_dir, args)
    logger.info("Experiment '%s' complete. See %s", args.tag, out_dir)


if __name__ == "__main__":
    main()
