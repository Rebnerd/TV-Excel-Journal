"""AMP order-history import and normalization helpers."""

from __future__ import annotations

import pandas as pd

from .time_utils import parse_amp_datetime

def order_typ_label(order_type: str) -> str:
    ot = (order_type or "").strip().lower()
    if "stop" in ot:
        return "Stop"
    if "limit" in ot:
        return "Limit"
    if "market" in ot:
        return "Market"
    return (order_type or "").strip() or "Other"

def exit_typ_label(order_type: str, pnl_usd: float) -> str:
    """
    Locked behavior:
    - Stop/Stop Limit -> Stop Loss
    - Limit -> Profit-taking, BUT if pnl_usd < 0 then Early exit
    - Market -> Market exit
    """
    ot = (order_type or "").strip().lower()
    if "stop" in ot:
        return "Stop Loss"
    if "limit" in ot:
        return "Early exit" if pnl_usd < 0 else "Profit-taking"
    if "market" in ot:
        return "Market exit"
    # Fallback
    return "Early exit" if pnl_usd < 0 else (order_type or "Exit")

def normalize_side(side: str) -> str:
    s = (side or "").strip().upper()
    if s in ("B", "BUY"):
        return "BUY"
    if s in ("S", "SELL"):
        return "SELL"
    return s


def load_executed_fills(csv_path: str) -> pd.DataFrame:
    """Load an AMP CSV and keep only rows that have an actual fill quantity."""
    df = pd.read_csv(csv_path)
    if "Fill Qty" in df.columns:
        fill_qty = pd.to_numeric(df["Fill Qty"], errors="coerce").fillna(0)
        return df[fill_qty > 0].copy()
    return df[df["Status"].astype(str).str.strip().str.lower() == "filled"].copy()
