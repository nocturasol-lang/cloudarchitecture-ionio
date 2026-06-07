#!/usr/bin/env bash
# Katevazei istorika football odds apo to football-data.co.uk
# URL format: https://www.football-data.co.uk/mmz4281/SSSS/LEAGUE.csv
#   SSSS = season (px 2425 = 2024-25)
#   LEAGUE = px E0 (Premier League), SP1 (La Liga), I1 (Serie A)
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="$PROJECT_ROOT/data/raw"
mkdir -p "$RAW_DIR"

# Top eyrwpaikes ligkes — kaliftontai apo polles stoixhmatikes
LEAGUES=(
    "E0"    # English Premier League
    "E1"    # English Championship
    "SP1"   # Spanish La Liga
    "I1"    # Italian Serie A
    "D1"    # German Bundesliga
    "F1"    # French Ligue 1
    "N1"    # Dutch Eredivisie
    "P1"    # Portuguese Primeira Liga
)

# Prosfates sezon (pio polles stoixhmatikes sta neotera data)
SEASONS=(
    "2021" "2122" "2223" "2324" "2425"
)

total=0
ok=0
fail=0

for season in "${SEASONS[@]}"; do
    for league in "${LEAGUES[@]}"; do
        total=$((total + 1))
        out="$RAW_DIR/${league}_${season}.csv"
        url="https://www.football-data.co.uk/mmz4281/${season}/${league}.csv"

        if [ -f "$out" ] && [ -s "$out" ]; then
            echo "  skip (exists): ${league}_${season}.csv"
            ok=$((ok + 1))
            continue
        fi

        if curl -fsS -o "$out" "$url"; then
            size=$(wc -c < "$out")
            echo "  ok:   ${league}_${season}.csv (${size} bytes)"
            ok=$((ok + 1))
        else
            echo "  FAIL: ${league}_${season}.csv"
            rm -f "$out"
            fail=$((fail + 1))
        fi
    done
done

echo
echo "Summary: $ok/$total downloaded, $fail failed"
echo "Files in $RAW_DIR:"
ls -la "$RAW_DIR" | tail -n +2
