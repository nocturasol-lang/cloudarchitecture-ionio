"""
Grigori eksereunisi ton CSV apo to football-data.co.uk.

Stoxoi:
  1. Synolikos arithmos agonon ana league/season
  2. Posa matches kalyptei kathe stoixhmatiki
  3. Posa cross-bookmaker arbitrages yparxoun pragmatika sto dataset
"""
import sys
from pathlib import Path

import pandas as pd

# Etsi wste to "from arbitrage import ..." na doulevei
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.detector import detect_arbitrage  # noqa: E402


RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Kathe stoixhmatiki exei 3 stiles (Home / Draw / Away).
# Mapping me to naming convention tou football-data.co.uk.
BOOKMAKERS = {
    "Bet365":     ("B365H", "B365D", "B365A"),
    "BetAndWin":  ("BWH",   "BWD",   "BWA"),
    "Betfair":    ("BFH",   "BFD",   "BFA"),
    "Pinnacle":   ("PSH",   "PSD",   "PSA"),
    "WilliamHill":("WHH",   "WHD",   "WHA"),
    "1XBet":      ("1XBH",  "1XBD",  "1XBA"),
}


def load_all_csvs(raw_dir: Path) -> pd.DataFrame:
    """Enwnei ola ta CSV se ena DataFrame. Prosthetei 'league' + 'season' apo to filename."""
    frames = []
    for csv in sorted(raw_dir.glob("*.csv")):
        league, season = csv.stem.split("_", 1)
        df = pd.read_csv(csv, encoding="utf-8-sig", on_bad_lines="skip", low_memory=False)
        df["league"] = league
        df["season"] = season
        df["source_file"] = csv.name
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def bookmaker_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Gia kathe stoixhmatiki, posa matches exoun kai tis 3 odds stiles."""
    rows = []
    for name, (h, d, a) in BOOKMAKERS.items():
        if h in df.columns and d in df.columns and a in df.columns:
            valid = df[[h, d, a]].notna().all(axis=1).sum()
        else:
            valid = 0
        rows.append({"bookmaker": name, "matches_with_odds": int(valid)})
    return pd.DataFrame(rows).sort_values("matches_with_odds", ascending=False)


def row_to_odds_dicts(row: pd.Series) -> tuple[dict, dict, dict]:
    """Apo mia grammi match, ftiaxnei dicts {stoixhmatiki: odds} gia H/D/A.
    Paraleipei stoixhmatikes me missing data."""
    home, draw, away = {}, {}, {}
    for name, (h, d, a) in BOOKMAKERS.items():
        if h in row and d in row and a in row:
            try:
                vh = float(row[h]); vd = float(row[d]); va = float(row[a])
            except (TypeError, ValueError):
                continue
            if pd.notna(vh) and pd.notna(vd) and pd.notna(va) and \
               vh > 1.0 and vd > 1.0 and va > 1.0:
                home[name] = vh
                draw[name] = vd
                away[name] = va
    return home, draw, away


def scan_arbitrage(df: pd.DataFrame) -> tuple[int, list]:
    """Pernaei ola ta matches kai psaxnei gia cross-bookmaker arbitrage.
    Epistrefei (matches_scanned, list_of_opportunities)."""
    opps = []
    scanned = 0
    for idx, row in df.iterrows():
        home, draw, away = row_to_odds_dicts(row)
        if len(home) < 2:
            # Xreiazomaste toulaxisto 2 stoixhmatikes gia na exei nohma
            continue
        scanned += 1
        match_id = f"{row.get('league','?')}-{row.get('Date','?')}-{row.get('HomeTeam','?')}-{row.get('AwayTeam','?')}"
        opp = detect_arbitrage(
            match_id=match_id,
            home_odds=home,
            draw_odds=draw,
            away_odds=away,
            detected_at_ms=0,
        )
        if opp is not None:
            opps.append(opp)
    return scanned, opps


def main() -> None:
    print(f"Loading CSVs from: {RAW_DIR}")
    df = load_all_csvs(RAW_DIR)
    print(f"Total matches: {len(df):,}")
    print()

    print("By league:")
    print(df.groupby("league").size().sort_values(ascending=False).to_string())
    print()

    print("By season:")
    print(df.groupby("season").size().sort_values(ascending=False).to_string())
    print()

    print("Bookmaker coverage:")
    print(bookmaker_coverage(df).to_string(index=False))
    print()

    print("Scanning for cross-bookmaker arbitrage opportunities ...")
    scanned, opps = scan_arbitrage(df)
    print(f"  Matches with >=2 bookmakers: {scanned:,}")
    print(f"  Arbitrage opportunities:     {len(opps):,}")
    if scanned > 0:
        pct = 100.0 * len(opps) / scanned
        print(f"  Rate:                        {pct:.3f}%")
    print()

    if opps:
        print("Top 10 by profit margin:")
        top = sorted(opps, key=lambda o: -o.profit_margin)[:10]
        for o in top:
            print(
                f"  margin={o.profit_margin*100:5.2f}%  "
                f"H={o.best_odds_home:.2f}({o.best_book_home}) "
                f"D={o.best_odds_draw:.2f}({o.best_book_draw}) "
                f"A={o.best_odds_away:.2f}({o.best_book_away})  "
                f"{o.match_id}"
            )


if __name__ == "__main__":
    main()
