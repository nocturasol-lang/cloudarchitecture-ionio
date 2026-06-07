"""
Anixneusi arbitrage gia podosfairikes apodoseis (agora 1X2).

Mathimatika:
Gia ena gegonos me 3 dynata apotelesmata (niki gipedouxou / isopalia / niki
filoksenoumenou) me dekadikes apodoseis (o1, oX, o2), i eksypakouomeni
pithanotita kathenos einai 1/o.

Otan pairnoume tin KALYTERI diathesimi apodosi apo N stoixhmatikes gia kathe
apotelesma, exoume arbitrage otan:

    1/best_o1 + 1/best_oX + 1/best_o2 < 1.0

To profit margin (% sigouro kerdos epi tou total stake) einai:

    margin = 1 - (sum of 1/best_o)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


# Mikri timi gia na agnooume "arbitrage" pou einai apla floating point noise.
EPSILON = 1e-9


@dataclass(frozen=True)
class ArbitrageOpportunity:
    """Mia eukaireia arbitrage pou entopistike se sygkekrimeni chroniki stigmi."""

    match_id: str
    # Ana outcome: poia stoixhmatiki dinei tin kalyteri apodosi kai poia einai
    best_book_home: str
    best_odds_home: float
    best_book_draw: str
    best_odds_draw: float
    best_book_away: str
    best_odds_away: float
    # Ypologismena pedia
    implied_prob_sum: float    # < 1.0 otan yparxei arbitrage
    profit_margin: float       # > 0.0 otan yparxei arbitrage (px 0.017 = 1.7%)
    # Pote entopistike (epoch ms)
    detected_at_ms: int

    @property
    def is_arbitrage(self) -> bool:
        return self.profit_margin > EPSILON


def implied_probability_sum(odds_home: float, odds_draw: float, odds_away: float) -> float:
    """Athroisma ton eksypakouomenwn pithanotitwn gia mia agora 1X2.

    < 1.0 => yparxei arbitrage
    = 1.0 => dikaii agora (xwris bookmaker margin)
    > 1.0 => i stoixhmatiki exei overround (kanoniki periptwsi)
    """
    if odds_home <= 1.0 or odds_draw <= 1.0 or odds_away <= 1.0:
        raise ValueError(f"Odds must be > 1.0, got {odds_home=}, {odds_draw=}, {odds_away=}")
    return (1.0 / odds_home) + (1.0 / odds_draw) + (1.0 / odds_away)


def _pick_best(book_to_odds: Mapping[str, float]) -> tuple[str, float]:
    """Epistrefei (bookmaker, best_odds) — kerdizei i megalyteri dekadiki apodosi."""
    if not book_to_odds:
        raise ValueError("book_to_odds is empty")
    best_book = max(book_to_odds, key=book_to_odds.__getitem__)
    return best_book, book_to_odds[best_book]


def detect_arbitrage(
    match_id: str,
    home_odds: Mapping[str, float],
    draw_odds: Mapping[str, float],
    away_odds: Mapping[str, float],
    detected_at_ms: int,
) -> ArbitrageOpportunity | None:
    """Elenxei an yparxei eukaireia arbitrage gia auto to snapshot agona.

    Parametroi:
      match_id: identifier tou agona (px "ARS-CHE-2025-09-15")
      home_odds / draw_odds / away_odds: dict { bookmaker -> dekadiki apodosi }
        Prepei na exei toulaxisto mia stoixhmatiki gia kathe outcome.
      detected_at_ms: epoch timestamp ms tis stigmis pou diavasame ta odds.

    Epistrefei to ArbitrageOpportunity an yparxei, alliws None.
    """
    best_book_home, best_odds_home = _pick_best(home_odds)
    best_book_draw, best_odds_draw = _pick_best(draw_odds)
    best_book_away, best_odds_away = _pick_best(away_odds)

    prob_sum = implied_probability_sum(best_odds_home, best_odds_draw, best_odds_away)
    margin = 1.0 - prob_sum

    if margin <= EPSILON:
        return None

    return ArbitrageOpportunity(
        match_id=match_id,
        best_book_home=best_book_home,
        best_odds_home=best_odds_home,
        best_book_draw=best_book_draw,
        best_odds_draw=best_odds_draw,
        best_book_away=best_book_away,
        best_odds_away=best_odds_away,
        implied_prob_sum=prob_sum,
        profit_margin=margin,
        detected_at_ms=detected_at_ms,
    )


def optimal_stakes(opp: ArbitrageOpportunity, total_stake: float) -> dict[str, float]:
    """Moirazei to `total_stake` sta 3 outcomes wste i epistrofi na einai isi.

    Epistrefei dict {"home": ..., "draw": ..., "away": ...} me ta stoixhmata.
    """
    s_home = total_stake * (1.0 / opp.best_odds_home) / opp.implied_prob_sum
    s_draw = total_stake * (1.0 / opp.best_odds_draw) / opp.implied_prob_sum
    s_away = total_stake * (1.0 / opp.best_odds_away) / opp.implied_prob_sum
    return {"home": s_home, "draw": s_draw, "away": s_away}
