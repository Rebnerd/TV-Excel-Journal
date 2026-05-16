"""Daily report metric calculations."""

from __future__ import annotations

from typing import Dict, List

from .config import COMMISSION_PER_SIDE_BY_INSTRUMENT


def commission_per_side_for_trade(trade: Dict) -> float:
    """Return the first-tier commission rate for a trade's detected instrument."""
    symbol = str(trade.get("instrument_symbol") or "").strip().upper()
    try:
        return float(COMMISSION_PER_SIDE_BY_INSTRUMENT[symbol])
    except KeyError as exc:
        supported = ", ".join(sorted(COMMISSION_PER_SIDE_BY_INSTRUMENT))
        raise ValueError(
            f"Cannot calculate commission for unsupported instrument {symbol!r}. "
            f"Supported instruments: {supported}."
        ) from exc


def compute_metrics(trades: List[Dict]) -> Dict[str, float]:
    n = len(trades)

    def _pnl_usd(t) -> float:
        try:
            return float(t.get("pnl_usd") or 0.0)
        except Exception:
            return 0.0

    def _pnl_pts(t) -> float:
        try:
            return float(t.get("pnl_points") or 0.0)
        except Exception:
            return 0.0

    wins = sum(1 for t in trades if _pnl_usd(t) > 0)
    losses = sum(1 for t in trades if _pnl_usd(t) < 0)
    break_even = sum(1 for t in trades if abs(_pnl_usd(t)) < 1e-12)

    # Win rate excludes break-even trades
    denom = wins + losses
    winrate = (wins / denom) if denom > 0 else 0.0

    total_pnl_usd = sum(_pnl_usd(t) for t in trades)
    total_pnl_points = sum(_pnl_pts(t) for t in trades)

    # Dollar side
    gross_profit = sum(_pnl_usd(t) for t in trades if _pnl_usd(t) > 0)
    gross_loss = -sum(_pnl_usd(t) for t in trades if _pnl_usd(t) < 0)  # positive magnitude

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    avg_win = (gross_profit / wins) if wins > 0 else None
    avg_loss = (gross_loss / losses) if losses > 0 else None
    payoff = (avg_win / avg_loss) if (avg_win is not None and avg_loss not in (None, 0)) else None

    # Points side (exclude quantity; relies on existing pnl_points logic)
    total_win_points = sum(_pnl_pts(t) for t in trades if _pnl_usd(t) > 0)
    total_loss_points = -sum(_pnl_pts(t) for t in trades if _pnl_usd(t) < 0)  # positive magnitude
    avg_win_points = (total_win_points / wins) if wins > 0 else None
    avg_loss_points = (total_loss_points / losses) if losses > 0 else None

    # Commission: 2 sides per contract per trade row, using each detected symbol.
    est_commission = 0.0
    for t in trades:
        try:
            q = float(t.get("qty") or 0.0)
        except Exception:
            q = 0.0
        est_commission += commission_per_side_for_trade(t) * 2.0 * q
    net_pnl = total_pnl_usd - est_commission

    out = {
        "trades": n,
        "wins": wins,
        "losses": losses,
        "break_even": break_even,
        "winrate": winrate,

        "total_pnl_points": round(total_pnl_points, 2),
        "total_win_points": round(total_win_points, 2),
        "total_loss_points": round(total_loss_points, 2),
        "avg_win_points": (round(avg_win_points, 2) if avg_win_points is not None else None),
        "avg_loss_points": (round(avg_loss_points, 2) if avg_loss_points is not None else None),

        "total_pnl_usd": round(total_pnl_usd, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": (round(profit_factor, 8) if profit_factor is not None else None),
        "avg_win": (round(avg_win, 2) if avg_win is not None else None),
        "avg_loss": (round(avg_loss, 2) if avg_loss is not None else None),
        "payoff": (round(payoff, 8) if payoff is not None else None),
        "est_commission": round(est_commission, 2),
        "net_pnl": round(net_pnl, 2),
    }
    return out
