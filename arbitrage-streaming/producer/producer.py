"""
Kafka producer pou kanei replay ta istorika football odds san live stream.

Pos doulevei:
1. Diavazei ola ta CSV apo data/raw/ kai tajinomei tis grammes me vasi (Date, Time).
2. Trexei xronologika. Gia kathe agona, stelnei ena OddsUpdate event ana
   stoixhmatiki pou eixe egkyres 1X2 apodoseis.
3. Anamesa stous diadoxikous agones kanei sleep gia (real_dt / SPEED), wste
   --speed 100 simenei 100x tahytero apo to real time.
4. Kathe event exei kai simulated (event_time_ms) kai wall-clock (real_time_ms)
   timestamps, wste oi consumers na ypologisoun to end-to-end latency.

"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from producer.schema import OddsUpdate, build_match_id  # noqa: E402


# Mapping: stoixhmatiki -> (home_col, draw_col, away_col)
# Auta einai ta onomata twn columns sta CSV tou football-data.co.uk.
BOOKMAKERS: dict[str, tuple[str, str, str]] = {
    "Bet365":      ("B365H", "B365D", "B365A"),
    "BetAndWin":   ("BWH",   "BWD",   "BWA"),
    "Betfair":     ("BFH",   "BFD",   "BFA"),
    "Pinnacle":    ("PSH",   "PSD",   "PSA"),
    "WilliamHill": ("WHH",   "WHD",   "WHA"),
    "1XBet":       ("1XBH",  "1XBD",  "1XBA"),
}


logger = logging.getLogger("producer")


def _parse_match_datetime(date_str: str, time_str: str | float | None) -> datetime | None:
    """Kanei parse to Date+Time tou football-data.co.uk se datetime.

    Date format poikilei: synithos 'DD/MM/YYYY' i 'DD/MM/YY'.
    Time einai 'HH:MM' string (mporei na leipei).
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    date_str = date_str.strip()
    time_part = time_str if (isinstance(time_str, str) and time_str.strip()) else "12:00"

    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%y %H:%M"):
        try:
            return datetime.strptime(f"{date_str} {time_part}", fmt)
        except ValueError:
            continue
    return None


def load_all_csvs(raw_dir: Path) -> pd.DataFrame:
    """Enwnei ola ta CSV se ena DataFrame, prosthetei league/season apo to filename."""
    frames: list[pd.DataFrame] = []
    for csv in sorted(raw_dir.glob("*.csv")):
        league, season = csv.stem.split("_", 1)
        df = pd.read_csv(csv, encoding="utf-8-sig", on_bad_lines="skip", low_memory=False)
        df["league"] = league
        df["season"] = season
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No CSVs found in {raw_dir}")

    df = pd.concat(frames, ignore_index=True)

    # Parse to kickoff datetime. Grammes pou den parsarontai paraleipontai
    df["_dt"] = [
        _parse_match_datetime(d, t)
        for d, t in zip(df.get("Date", []), df.get("Time", [None] * len(df)))
    ]
    df = df.dropna(subset=["_dt"]).sort_values("_dt").reset_index(drop=True)
    return df


def iter_events(df: pd.DataFrame) -> Iterator[OddsUpdate]:
    """Vgazei ena OddsUpdate ana (grammi x stoixhmatiki) xronologika."""
    for _, row in df.iterrows():
        match_date_iso = row["_dt"].strftime("%Y-%m-%d")
        match_id = build_match_id(
            league=row["league"],
            match_date=match_date_iso,
            home=row.get("HomeTeam", "?"),
            away=row.get("AwayTeam", "?"),
        )
        event_time_ms = int(row["_dt"].timestamp() * 1000)

        for book, (h, d, a) in BOOKMAKERS.items():
            if h not in row or d not in row or a not in row:
                continue
            try:
                vh = float(row[h]); vd = float(row[d]); va = float(row[a])
            except (TypeError, ValueError):
                continue
            if not (vh > 1.0 and vd > 1.0 and va > 1.0):
                continue
            if pd.isna(vh) or pd.isna(vd) or pd.isna(va):
                continue

            yield OddsUpdate(
                match_id=match_id,
                league=row["league"],
                home_team=str(row.get("HomeTeam", "?")),
                away_team=str(row.get("AwayTeam", "?")),
                match_date=match_date_iso,
                bookmaker=book,
                odds_home=vh,
                odds_draw=vd,
                odds_away=va,
                event_time_ms=event_time_ms,
                real_time_ms=0,   # mpainei sti stigmi tou send
            )


def run(
    raw_dir: Path,
    topic: str,
    bootstrap_servers: str,
    speed: float,
    limit: int | None,
    progress_every: int,
) -> None:
    logger.info("Loading CSVs from %s ...", raw_dir)
    df = load_all_csvs(raw_dir)
    logger.info("Loaded %d matches across %d leagues.", len(df), df["league"].nunique())

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        # To key kanei partition ana match, wste ola ta updates enos agona na
        # pigainoun sto idio Kafka partition (diatirei ti seira tous).
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks=1,
    )

    # Graceful Ctrl-C: flush prin to exit
    stop = {"value": False}

    def _handler(signum, frame):
        logger.warning("Signal %d received -- stopping after current event.", signum)
        stop["value"] = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    sent = 0
    prev_event_dt: datetime | None = None
    start_wall = time.monotonic()

    try:
        for ev in iter_events(df):
            if stop["value"]:
                break
            if limit is not None and sent >= limit:
                logger.info("Reached --limit %d, stopping.", limit)
                break

            this_dt = datetime.fromtimestamp(ev.event_time_ms / 1000)
            if prev_event_dt is not None and speed > 0:
                gap_s = (this_dt - prev_event_dt).total_seconds()
                if gap_s > 0:
                    sleep_s = gap_s / speed
                    # cap sta 5s gia na min kollaei to demo se kalokairina kena
                    sleep_s = min(sleep_s, 5.0)
                    if sleep_s > 0:
                        time.sleep(sleep_s)
            prev_event_dt = this_dt

            ev = OddsUpdate(**{**ev.to_dict(), "real_time_ms": int(time.time() * 1000)})
            producer.send(topic, key=ev.match_id, value=ev.to_dict())
            sent += 1

            if sent % progress_every == 0:
                elapsed = time.monotonic() - start_wall
                rate = sent / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "sent=%d  rate=%.0f msg/s  last=%s  bookmaker=%s",
                    sent, rate, this_dt.isoformat(), ev.bookmaker,
                )

    finally:
        logger.info("Flushing producer ...")
        producer.flush(timeout=10)
        producer.close(timeout=5)
        elapsed = time.monotonic() - start_wall
        logger.info(
            "DONE. sent=%d events in %.1fs (avg %.0f msg/s)",
            sent, elapsed, sent / elapsed if elapsed > 0 else 0.0,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path,
                        default=PROJECT_ROOT / "data" / "raw",
                        help="Fakelos me ta CSV tou football-data")
    parser.add_argument("--topic", default="betting-odds",
                        help="Kafka topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--speed", type=float, default=100.0,
                        help="Replay speed multiplier (1.0 = real time, "
                             "100.0 = 100x tahytera). 0 = no sleep (max throughput).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stamatima meta apo N events (None = ola)")
    parser.add_argument("--progress-every", type=int, default=500,
                        help="Print progress line kathe N events")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    run(
        raw_dir=args.raw_dir,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        speed=args.speed,
        limit=args.limit,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()
