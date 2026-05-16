"""IBKR trade enrichment adapter.

Thin wrapper module for the daily report runner.

Data flow:
  runner -> this adapter -> ibkr_market_data.compute_entry_features() -> adapter -> runner

This file DOES NOT re-implement indicator logic.
It only:
  - prepares/aligns parameters (timezone + 5-min candle alignment + contract month mapping)
  - calls compute_entry_features() from ibkr_market_data.py
  - extracts raw values (ATR, EMA20 slope norm by ATR, regression slope norm by ATR, regression R^2)
  - derives the "aligned" label (continuation/reversal/unclear) from the sign of regression slope vs trade direction
  - optionally calls compute_position_mfe_mae() from ibkr_market_data.py
  - writes everything back into the trade dicts (in-place)
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import redirect_stdout
from zoneinfo import ZoneInfo

# Import the user's intact calculation module.
from .ibkr_market_data import compute_entry_features

# Optional: tick-based excursion metrics (MFE/MAE) between entry and exit.
try:
    from .ibkr_market_data import compute_position_mfe_mae as _compute_position_mfe_mae
except Exception:
    _compute_position_mfe_mae = None  # type: ignore


TZ_LOCAL = ZoneInfo("Asia/Shanghai")
TZ_CME = ZoneInfo("America/Chicago")


@dataclass
class IBKRContextConfig:
    """Configuration passed from the runner to this wrapper."""

    port: int = 4001
    client_id: int = 101

    # Indicator parameters
    reg_window: int = 6
    ema_slope_k: int = 3
    atr_period: int = 14
    eth_context_bars: int = 15

    # We only need EMA20 right now
    ema_spans: tuple[int, ...] = (20,)

    # Instrument
    symbol: str = "MES"
    exchange: str = "CME"
    contract_month: str = "202606"

    # Optional: tick-based MFE/MAE between entry and exit
    mfe_contract_multiplier: float = 5.0   # MES = $5/point
    mfe_what_to_show: str = "TRADES"
    mfe_useRTH: bool = False
    mfe_ticks_per_page: int = 1000
    mfe_max_pages: int = 200


def _to_ct_5min_bar_start(dt_local_naive: datetime) -> datetime:
    """Treat dt_local_naive as Asia/Shanghai, convert to CT, floor to 5-min bar start."""
    if dt_local_naive.tzinfo is None:
        dt_local = dt_local_naive.replace(tzinfo=TZ_LOCAL)
    else:
        dt_local = dt_local_naive.astimezone(TZ_LOCAL)

    dt_ct = dt_local.astimezone(TZ_CME)
    minute = dt_ct.minute - (dt_ct.minute % 5)
    return dt_ct.replace(minute=minute, second=0, microsecond=0)


def _to_ct_exact(dt_local_naive: datetime) -> datetime:
    """Treat dt_local_naive as Asia/Shanghai, convert to CT (keep seconds)."""
    if dt_local_naive.tzinfo is None:
        dt_local = dt_local_naive.replace(tzinfo=TZ_LOCAL)
    else:
        dt_local = dt_local_naive.astimezone(TZ_LOCAL)
    return dt_local.astimezone(TZ_CME)


def _aligned_label_from_reg_slope(direction: str, reg_slope_per_bar: Any) -> str:
    """Return continuation/reversal/unclear using sign(regression slope) vs direction."""
    try:
        x = float(reg_slope_per_bar)
    except Exception:
        return "unclear"

    if x != x or x == 0:
        return "unclear"

    d = (direction or "").strip().lower()
    if d.startswith("long"):
        return "continuation" if x > 0 else "reversal"
    if d.startswith("short"):
        return "continuation" if x < 0 else "reversal"
    return "unclear"


def _parse_local_naive_datetime(x: Any) -> Optional[datetime]:
    """Parse common datetime inputs used in the trade dict."""
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s[:19], fmt)
            except Exception:
                pass
    return None


def get_trade_context(
    *,
    entry_time_local_naive: datetime,
    direction: str,
    cfg: IBKRContextConfig,
    symbol: Optional[str] = None,
    contract_month: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch raw context + compute aligned label for ONE trade."""
    ct_bar_start = _to_ct_5min_bar_start(entry_time_local_naive)
    entry_start_ct_str = ct_bar_start.strftime("%Y-%m-%d %H:%M")

    buf = io.StringIO()
    with redirect_stdout(buf):
        result = compute_entry_features(
            port=cfg.port,
            client_id=cfg.client_id,
            symbol=symbol or cfg.symbol,
            contract_month=contract_month or cfg.contract_month,
            exchange=exchange or cfg.exchange,
            entry_start_ct_str=entry_start_ct_str,
            reg_window=cfg.reg_window,
            ema_spans=cfg.ema_spans,
            ema_slope_k=cfg.ema_slope_k,
            atr_period=cfg.atr_period,
            eth_context_bars=cfg.eth_context_bars,
        )

    atr14 = (result.get("atr", {}) or {}).get("atr_at_entry")
    reg = (result.get("regression", {}) or {})
    regsn = reg.get("slope_norm_by_ATR")
    r2 = reg.get("r2")
    reg_slope_per_bar = reg.get("slope_per_bar")

    ema = (result.get("ema_slope", {}) or {})
    ema_key = f"EMA20_slope_norm_by_ATR_k{cfg.ema_slope_k}"
    ema20sn = ema.get(ema_key)

    aligned = _aligned_label_from_reg_slope(direction, reg_slope_per_bar)

    return {
        "atr14": atr14,
        "ema20sn": ema20sn,
        "regsn": regsn,
        "r2": r2,
        "aligned": aligned,
    }


def enrich_trades_inplace(trades: List[Dict[str, Any]], cfg: Optional[IBKRContextConfig] = None) -> None:
    """Enrich the runner's trades list in-place.

    Expected trade keys:
      - entry_time (naive, interpreted as Asia/Shanghai)
      - exit_time  (naive, interpreted as Asia/Shanghai)   [for MFE/MAE]
      - direction  (Long/Short)
      - symbol
      - entry_price                                  [for MFE/MAE]
      - qty                                          [for MFE/MAE]

    Added keys:
      - atr14, ema20sn, regsn, r2, aligned
      - mfe_points, mfe_usd, mfe_time
      - mae_points, mae_usd, mae_time

    Any failures are swallowed per-trade (best-effort enrichment).
    """
    cfg = cfg or IBKRContextConfig()

    for tr in trades:
        try:
            entry_dt = _parse_local_naive_datetime(tr.get("entry_time"))
            if entry_dt is None:
                continue

            # ----- context metrics -----
            try:
                ctx = get_trade_context(
                        entry_time_local_naive=entry_dt,
                        direction=str(tr.get("direction", "")),
                        cfg=cfg,
                        symbol=str(tr.get("instrument_symbol") or cfg.symbol),
                        contract_month=str(tr.get("contract_month") or cfg.contract_month),
                        exchange=str(tr.get("exchange") or cfg.exchange),
                    )
                tr["atr14"] = ctx.get("atr14")
                tr["ema20sn"] = ctx.get("ema20sn")
                tr["regsn"] = ctx.get("regsn")
                tr["r2"] = ctx.get("r2")
                tr["aligned"] = ctx.get("aligned")
            except Exception:
                pass

            # ----- position MFE / MAE -----
            if _compute_position_mfe_mae is not None:
                try:
                    exit_dt = _parse_local_naive_datetime(tr.get("exit_time"))
                    entry_price = tr.get("entry_price")
                    qty = tr.get("qty")

                    if exit_dt is None or entry_price is None or qty is None:
                        continue

                    entry_ct = _to_ct_exact(entry_dt)
                    exit_ct = _to_ct_exact(exit_dt)

                    if exit_ct <= entry_ct:
                        continue

                    exc = _compute_position_mfe_mae(
                        port=cfg.port,
                        client_id=cfg.client_id,
                        symbol=str(tr.get("instrument_symbol") or cfg.symbol),
                        contract_month=str(tr.get("contract_month") or cfg.contract_month),
                        exchange=str(tr.get("exchange") or cfg.exchange),
                        entry_time_ct=entry_ct,
                        exit_time_ct=exit_ct,
                        entry_price=float(entry_price),
                        direction=str(tr.get("direction", "")),
                        position_qty=float(qty),
                        contract_multiplier=float(tr.get("point_value") or cfg.mfe_contract_multiplier),
                        whatToShow=str(cfg.mfe_what_to_show),
                        useRTH=bool(cfg.mfe_useRTH),
                        ticks_per_page=int(cfg.mfe_ticks_per_page),
                        max_pages=int(cfg.mfe_max_pages),
                    )

                    if exc:
                        tr["mfe_points"] = exc.get("mfe_points")
                        tr["mfe_usd"] = exc.get("mfe_usd")
                        tr["mfe_time"] = exc.get("mfe_time")

                        tr["mae_points"] = exc.get("mae_points")
                        tr["mae_usd"] = exc.get("mae_usd")
                        tr["mae_time"] = exc.get("mae_time")
                except Exception:
                    pass

        except Exception:
            continue
