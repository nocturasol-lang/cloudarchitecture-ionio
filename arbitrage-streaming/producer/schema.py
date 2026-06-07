"""
Schema gia ta events pou trexoun sto Kafka topic "betting-odds".

Stelnoume ena OddsUpdate ana (agonas x stoixhmatiki) — diladi kathe agonas sto CSV
parage N events, ena ana stoixhmatiki me odds. Mimeitai ton pragmatiko kosmo
opou kathe stoixhmatiki dimosieyei to diko tis feed.

Morfi event:
{
  "match_id":     "E0-2024-08-17-Arsenal-Wolves",
  "league":       "E0",
  "home_team":    "Arsenal",
  "away_team":    "Wolves",
  "match_date":   "2024-08-17",
  "bookmaker":    "Bet365",
  "odds_home":    1.18,
  "odds_draw":    7.50,
  "odds_away":    17.00,
  "event_time_ms":  1724000000000,    # logikos (simulated) xronos tou agona
  "real_time_ms":   1724000000000,    # pragmatiki ora pou estalei to event
}
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class OddsUpdate:
    match_id: str
    league: str
    home_team: str
    away_team: str
    match_date: str            # ISO date "YYYY-MM-DD"
    bookmaker: str
    odds_home: float
    odds_draw: float
    odds_away: float
    event_time_ms: int         # simulated timestamp tou agona
    real_time_ms: int          # wall-clock otan estalei (gia metrisi latency)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_match_id(league: str, match_date: str, home: str, away: str) -> str:
    """Ftiaxnei stable identifier gia kathe agona."""
    # Antikathistoume ta kena kai ta slashes wste to id na einai Kafka-safe
    def _clean(s: str) -> str:
        return str(s).replace(" ", "_").replace("/", "-")
    return f"{league}-{match_date}-{_clean(home)}-{_clean(away)}"
