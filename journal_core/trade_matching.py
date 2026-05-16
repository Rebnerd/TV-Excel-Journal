"""FIFO fill-to-trade matching for AMP execution rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import pandas as pd

from .amp_import import exit_typ_label, normalize_side, order_typ_label
from .instruments import chart_symbol_for_instrument, resolve_broker_symbol
from .time_utils import fmt_duration, parse_amp_datetime, safe_div


@dataclass
class OpenLot:
    """Internal helper to track an open position leg while pairing fills."""
    symbol: str
    direction: str  # "Long" or "Short"
    qty: float
    entry_price: float
    entry_time: datetime
    entry_order_id: str
    entry_typ: str
    scale_in: bool
    instrument_symbol: str
    contract_month: str
    exchange: str
    chart_symbol: str
    point_value: float

def build_trades_from_fills(fills: pd.DataFrame, point_value: float = 5.0) -> List[Dict]:
    """
    FIFO match fills into entry/exit trades.
    Each close event creates one trade row (can split if partial close).
    """
    # open lots by symbol
    open_lots: Dict[str, List[OpenLot]] = {}
    trades: List[Dict] = []
    trade_id = 1

    # Sort strictly by execution time (Status Time), then placing time, then order id
    fills = fills.copy()
    fills["__status_dt"] = fills["Status Time"].apply(parse_amp_datetime)
    fills["__placing_dt"] = fills["Placing Time"].apply(parse_amp_datetime)
    # order id sometimes numeric; keep as string for writing
    def _to_int_safe(x):
        try:
            return int(str(x))
        except Exception:
            return 0
    fills["__oid_int"] = fills["Order ID"].apply(_to_int_safe)
    fills = fills.sort_values(["__status_dt","__placing_dt","__oid_int"], ascending=True).reset_index(drop=True)

    for _, row in fills.iterrows():
        symbol = str(row.get("Symbol","")).strip()
        instrument = resolve_broker_symbol(symbol)
        chart_symbol = chart_symbol_for_instrument(instrument.symbol)
        side = normalize_side(row.get("Side",""))
        qty = float(row.get("Fill Qty", 0) or 0)
        if qty == 0:
            continue
        price = float(row.get("Avg Fill Price", 0) or 0)
        order_type = str(row.get("Type","")).strip()
        oid = str(row.get("Order ID","")).strip()
        t_exec = row["__status_dt"]
        typ_lbl = order_typ_label(order_type)

        # Signed qty: BUY +, SELL -
        signed_qty = qty if side == "BUY" else -qty

        lots = open_lots.setdefault(symbol, [])
        # Determine current position sign from open lots
        pos = sum(l.qty if l.direction=="Long" else -l.qty for l in lots)
        pos_sign = 0 if pos==0 else (1 if pos>0 else -1)
        fill_sign = 1 if signed_qty>0 else -1

        if pos_sign == 0 or pos_sign == fill_sign:
            # Opening or adding to same direction
            direction = "Long" if fill_sign>0 else "Short"
            scale_in = (pos_sign != 0)
            lots.append(OpenLot(
                symbol=symbol,
                direction=direction,
                qty=abs(qty),
                entry_price=price,
                entry_time=t_exec,
                entry_order_id=oid,
                entry_typ=typ_lbl,
                scale_in=scale_in,
                instrument_symbol=instrument.symbol,
                contract_month=instrument.contract_month,
                exchange=instrument.exchange,
                chart_symbol=chart_symbol,
                point_value=instrument.point_value,
            ))
            continue

        # Closing or reducing (opposite sign)
        remaining = abs(qty)
        while remaining > 1e-9 and lots:
            open_lot = lots[0]
            close_qty = min(open_lot.qty, remaining)

            # PnL calc
            if open_lot.direction == "Long":
                pnl_points = (price - open_lot.entry_price)
            else:
                pnl_points = (open_lot.entry_price - price)
            pnl_usd = pnl_points * close_qty * open_lot.point_value

            duration_min = max(0.0, (t_exec - open_lot.entry_time).total_seconds() / 60.0)
            pnl_usd_per_m = safe_div(pnl_usd, duration_min) if duration_min>0 else None
            pnl_points_per_m = safe_div(pnl_points, duration_min) if duration_min>0 else None

            trade = {
                "id": trade_id,
                "symbol": symbol,
                "instrument_symbol": open_lot.instrument_symbol,
                "contract_month": open_lot.contract_month,
                "exchange": open_lot.exchange,
                "chart_symbol": open_lot.chart_symbol,
                "point_value": open_lot.point_value,
                "direction": open_lot.direction,
                "qty": float(close_qty),
                "entry_type": open_lot.entry_typ,
                "exit_type": exit_typ_label(order_type, pnl_usd),
                "entry_price": float(open_lot.entry_price),
                "exit_price": float(price),
                "duration": fmt_duration(open_lot.entry_time, t_exec),
                "pnl_usd": float(round(pnl_usd, 2)),
                "pnl_usd_per_min": (float(pnl_usd_per_m) if pnl_usd_per_m is not None else None),
                "pnl_points": float(round(pnl_points, 2)),
                "pnl_points_per_min": (float(pnl_points_per_m) if pnl_points_per_m is not None else None),
                "mfe_time": None,
                "mae_time": None,
                "mfe_usd": None,
                "mae_usd": None,
                "mfe_points": None,
                "mae_points": None,
                "aligned": None,
                "scale_in": bool(open_lot.scale_in),
                "review": None,
                "entry_time": open_lot.entry_time.replace(tzinfo=None),
                "exit_time": t_exec.replace(tzinfo=None),
                "entry_order_id": open_lot.entry_order_id,
                "exit_order_id": oid,
                "atr14": None,
                "ema20sn": None,
                "regsn": None,
                "r2": None,
            }
            trades.append(trade)
            trade_id += 1

            open_lot.qty -= close_qty
            remaining -= close_qty

            if open_lot.qty <= 1e-9:
                lots.pop(0)

        # If we closed more than existing, the fill flipped the position past zero.
        # Treat the leftover as a NEW opening lot in the fill direction (same exec time/price).
        if remaining > 1e-9:
            direction = "Long" if fill_sign > 0 else "Short"
            lots.append(OpenLot(
                symbol=symbol,
                direction=direction,
                qty=float(remaining),
                entry_price=price,
                entry_time=t_exec,
                entry_order_id=oid,
                entry_typ=typ_lbl,
                scale_in=False,
                instrument_symbol=instrument.symbol,
                contract_month=instrument.contract_month,
                exchange=instrument.exchange,
                chart_symbol=chart_symbol,
                point_value=instrument.point_value,
            ))
            remaining = 0.0

    return trades
