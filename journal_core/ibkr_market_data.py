"""IBKR market-data fetchers and indicator/excursion calculations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import pytz
import numpy as np
import pandas as pd
from typing import Any, Dict, Optional, Union
from ib_insync import IB, Future, util


@dataclass(frozen=True)
class SessionWindow:
    ref_date: dt.date
    start_ct: dt.datetime
    end_ct: dt.datetime


def linreg_slope_r2(y: np.ndarray) -> tuple[float, float]:
    n = len(y)
    if n < 2:
        return (np.nan, np.nan)
    x = np.arange(n, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_hat = slope * x + intercept
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    return float(slope), round(float(r2), 2)


def ema_series(closes: pd.Series, span: int) -> pd.Series:
    return closes.ewm(span=span, adjust=False).mean()


def true_range_series(df: pd.DataFrame) -> pd.Series:
    """TR = max(H-L, |H-prevC|, |L-prevC|)"""
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1
    ).max(axis=1)
    return tr


def atr_wilder_from_ohlc(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Wilder ATR on OHLC with columns: high, low, close.
    Seed: SMA of first n TRs. Then Wilder smoothing.
    Returns a Series aligned to df index with NaNs until seeded.
    """
    tr = true_range_series(df)
    atr = pd.Series(index=df.index, dtype=float)

    # Need at least period+1 bars (because first TR uses prev_close)
    if len(tr.dropna()) < period + 1:
        return atr

    # Seed at index=period using TR[1..period]
    atr.iloc[period] = tr.iloc[1:period + 1].mean()

    for i in range(period + 1, len(tr)):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period

    return atr


def compute_atr_at_entry(calc_df: pd.DataFrame, atr_period: int) -> float:
    """
    ATR as-of entry using Wilder (RMA) smoothing.

    Key behavior (per requirement):
    - The *period* only affects the initial seeding / warm-up.
    - Once there are >= (period+1) bars available, ATR is computed recursively over
      the *entire* provided `calc_df` (e.g., full RTH session from 08:30 -> entry),
      so earlier same-session candles continue to contribute with decaying weight.
    - This also ensures we don't accidentally "leak" ETH candles into an RTH-only
      calculation if the caller has already filtered `calc_df` to RTH.

    Fallback when data is insufficient:
    - If < (period+1) bars but >= 2 bars: return mean(TR) over available TR values
      (excluding the first row which lacks prev_close).
    - Else: NaN.
    """
    if calc_df is None or len(calc_df) < 2:
        return np.nan

    need = atr_period + 1
    if len(calc_df) >= need:
        # Compute Wilder ATR across the full `calc_df` so all same-session history contributes.
        atr_series = atr_wilder_from_ohlc(calc_df[["high", "low", "close"]], period=atr_period)
        atr_valid = atr_series.dropna()
        return float(atr_valid.iloc[-1]) if atr_valid.size > 0 else np.nan

    # Partial fallback: average TR of available bars (excluding first, which lacks prev_close)
    tr = true_range_series(calc_df[["high", "low", "close"]]).dropna()
    if tr.size >= 1:
        return float(tr.mean())
    return np.nan


def fetch_5m_bars(ib: IB, contract: Future, end_time_ct: dt.datetime, duration_seconds: int, useRTH: bool = False) -> pd.DataFrame:
    tz = pytz.timezone("America/Chicago")
    if end_time_ct.tzinfo is None:
        end_time_ct = tz.localize(end_time_ct)
    else:
        end_time_ct = end_time_ct.astimezone(tz)

    bars = ib.reqHistoricalData(
        contract=contract,
        endDateTime=end_time_ct,
        durationStr=f"{int(duration_seconds)} S",
        barSizeSetting="5 mins",
        whatToShow="TRADES",
        useRTH=useRTH,
        formatDate=1,
    )
    if not bars:
        return pd.DataFrame()

    df = util.df(bars)
    df["date"] = df["date"].apply(lambda t: tz.localize(t) if t.tzinfo is None else t.astimezone(tz))
    return df


def _normalize_to_ct(dt_obj: dt.datetime, tz_ct) -> dt.datetime:
    if dt_obj.tzinfo is None:
        return tz_ct.localize(dt_obj)
    return dt_obj.astimezone(tz_ct)


def _coerce_cme_trade_date(cme_trade_date: Union[str, dt.date, dt.datetime]) -> dt.date:
    if isinstance(cme_trade_date, dt.datetime):
        return cme_trade_date.date()
    if isinstance(cme_trade_date, dt.date):
        return cme_trade_date
    if isinstance(cme_trade_date, str):
        s = cme_trade_date.strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return dt.datetime.strptime(s, fmt).date()
            except ValueError:
                pass
    raise ValueError("cme_trade_date must be a date/datetime or a string like '2026-03-13'.")


def _parse_ib_schedule_dt(dt_str: str, tz_name: str) -> dt.datetime:
    """Parse IBKR historical schedule datetime strings into tz-aware datetimes."""
    if not dt_str:
        raise ValueError("Empty IBKR schedule datetime string.")

    tz = pytz.timezone(tz_name or "America/Chicago")
    s = str(dt_str).strip()
    for fmt in ("%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            naive = dt.datetime.strptime(s, fmt)
            return tz.localize(naive)
        except ValueError:
            pass

    parsed = pd.to_datetime(s)
    if pd.isna(parsed):
        raise ValueError(f"Could not parse IBKR schedule datetime: {dt_str!r}")
    py_dt = parsed.to_pydatetime()
    return _normalize_to_ct(py_dt, tz) if py_dt.tzinfo else tz.localize(py_dt)



def get_full_cme_session_bounds_ct(
    ib: IB,
    contract: Future,
    cme_trade_date: Union[str, dt.date, dt.datetime],
    *,
    num_schedule_days: int = 7,
    useRTH: bool = False,
) -> tuple[dt.datetime, dt.datetime]:
    """Resolve the exact CME session bounds from IBKR historical schedule.

    It requests IBKR's session schedule and selects the session(s) whose
    ``refDate`` matches the requested CME trade date. If IBKR returns multiple
    schedule fragments for that trade date, they are merged into a single
    [start, end) window using the earliest start and latest end.
    """
    td = _coerce_cme_trade_date(cme_trade_date)
    sessions = get_cme_session_windows_ct(
        ib,
        contract,
        td,
        num_schedule_days=num_schedule_days,
        useRTH=useRTH,
    )
    target = find_session_window(sessions, td)
    if target is None:
        available = sorted(s.ref_date.strftime("%Y%m%d") for s in sessions)
        raise RuntimeError(
            f"No IBKR schedule session matched CME trade date {td:%Y%m%d}. "
            f"Available refDate values: {available}"
        )
    return target.start_ct, target.end_ct


def get_cme_session_windows_ct(
    ib: IB,
    contract: Future,
    cme_trade_date: Union[str, dt.date, dt.datetime],
    *,
    num_schedule_days: int = 7,
    useRTH: bool = False,
) -> list[SessionWindow]:
    """Return ordered session windows from IBKR historical schedule."""
    tz_ct = pytz.timezone("America/Chicago")
    td = _coerce_cme_trade_date(cme_trade_date)
    end_lookup_ct = tz_ct.localize(dt.datetime.combine(td, dt.time(23, 59, 59)))
    schedule = ib.reqHistoricalSchedule(
        contract=contract,
        numDays=int(num_schedule_days),
        endDateTime=end_lookup_ct,
        useRTH=useRTH,
    )

    sessions = getattr(schedule, "sessions", None) or []
    if not sessions:
        raise RuntimeError(f"IBKR returned no historical schedule sessions for {contract}.")

    schedule_tz_name = getattr(schedule, "timeZone", "") or "America/Chicago"
    by_ref: dict[dt.date, list] = {}
    for session in sessions:
        ref_raw = str(getattr(session, "refDate", "")).strip()
        if not ref_raw:
            continue
        try:
            ref_date = dt.datetime.strptime(ref_raw, "%Y%m%d").date()
        except ValueError:
            continue
        by_ref.setdefault(ref_date, []).append(session)

    windows: list[SessionWindow] = []
    for ref_date, parts in by_ref.items():
        starts = [_parse_ib_schedule_dt(s.startDateTime, schedule_tz_name) for s in parts]
        ends = [_parse_ib_schedule_dt(s.endDateTime, schedule_tz_name) for s in parts]
        windows.append(
            SessionWindow(
                ref_date=ref_date,
                start_ct=min(starts).astimezone(tz_ct),
                end_ct=max(ends).astimezone(tz_ct),
            )
        )
    return sorted(windows, key=lambda s: (s.ref_date, s.start_ct))


def find_session_window(
    sessions: list[SessionWindow],
    cme_trade_date: Union[str, dt.date, dt.datetime],
) -> Optional[SessionWindow]:
    """Find the scheduled session whose refDate matches a CME trade date."""
    td = _coerce_cme_trade_date(cme_trade_date)
    for session in sessions:
        if session.ref_date == td:
            return session
    return None


def find_prior_session_window(
    sessions: list[SessionWindow],
    cme_trade_date: Union[str, dt.date, dt.datetime],
) -> Optional[SessionWindow]:
    """Find the scheduled trading session immediately before a CME trade date."""
    td = _coerce_cme_trade_date(cme_trade_date)
    prior_sessions = [session for session in sessions if session.ref_date < td]
    return prior_sessions[-1] if prior_sessions else None


def _fetch_1m_close_for_session_window(
    ib: IB,
    contract: Future,
    session_window: SessionWindow,
    *,
    whatToShow: str = "TRADES",
    useRTH: bool = False,
    strict_expected_minutes: bool = False,
) -> pd.DataFrame:
    duration_seconds = int((session_window.end_ct - session_window.start_ct).total_seconds())
    if duration_seconds <= 0:
        raise RuntimeError(
            f"IBKR schedule returned a non-positive session window for {session_window.ref_date}: "
            f"{session_window.start_ct} -> {session_window.end_ct}"
        )

    bars = ib.reqHistoricalData(
        contract=contract,
        endDateTime=session_window.end_ct,
        durationStr=f"{duration_seconds} S",
        barSizeSetting="1 min",
        whatToShow=whatToShow,
        useRTH=useRTH,
        formatDate=1,
    )

    columns = ["time_ct", "close", "cme_trade_date"]
    if not bars:
        return pd.DataFrame(columns=columns)

    df = util.df(bars)
    if df.empty or "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=columns)

    tz_ct = pytz.timezone("America/Chicago")
    df["date"] = df["date"].apply(lambda t: _normalize_to_ct(t, tz_ct))
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).copy()
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    mask = (df["date"] >= session_window.start_ct) & (df["date"] < session_window.end_ct)
    out = df.loc[mask, ["date", "close"]].copy()
    out = out.rename(columns={"date": "time_ct"}).reset_index(drop=True)
    out["cme_trade_date"] = session_window.ref_date

    if strict_expected_minutes:
        expected_minutes = int((session_window.end_ct - session_window.start_ct).total_seconds() // 60)
        if len(out) != expected_minutes:
            raise RuntimeError(
                f"Expected {expected_minutes} one-minute bars for CME session "
                f"{session_window.ref_date}, got {len(out)}."
            )

    return out


def _last_close_from_1m_close_df(close_df: pd.DataFrame) -> Optional[float]:
    if close_df is None or close_df.empty or "close" not in close_df.columns:
        return None
    close_values = pd.to_numeric(close_df["close"], errors="coerce").dropna()
    if close_values.empty:
        return None
    return float(close_values.iloc[-1])



def fetch_full_session_1m_close(
    *,
    port: int = 4001,
    client_id: int = 101,
    symbol: str = "MES",
    contract_month: str = "202603",
    exchange: str = "CME",
    cme_trade_date: Union[str, dt.date, dt.datetime],
    whatToShow: str = "TRADES",
    useRTH: bool = False,
    host: str = "127.0.0.1",
    strict_expected_minutes: bool = False,
    num_schedule_days: int = 7,
) -> pd.DataFrame:
    """Fetch one-minute close prices for one exact CME session from IBKR.

    This version is session-aware: it first asks IBKR for the instrument's
    historical schedule, finds the session whose ``refDate`` equals the
    requested CME trade date, and then requests exactly that session window.

    Returns a DataFrame with:
      - time_ct: tz-aware Chicago timestamps at one-minute precision
      - close: close price only
      - cme_trade_date: the requested CME trade date
    """
    tz_ct = pytz.timezone("America/Chicago")
    td = _coerce_cme_trade_date(cme_trade_date)

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id)

        contract = Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=contract_month,
            exchange=exchange,
            currency="USD",
        )
        ib.qualifyContracts(contract)

        sessions = get_cme_session_windows_ct(
            ib,
            contract,
            td,
            num_schedule_days=num_schedule_days,
            useRTH=useRTH,
        )
        session_window = find_session_window(sessions, td)
        if session_window is None:
            available = sorted(s.ref_date.strftime("%Y%m%d") for s in sessions)
            raise RuntimeError(
                f"No IBKR schedule session matched CME trade date {td:%Y%m%d}. "
                f"Available refDate values: {available}"
            )
        return _fetch_1m_close_for_session_window(
            ib,
            contract,
            session_window,
            whatToShow=whatToShow,
            useRTH=useRTH,
            strict_expected_minutes=strict_expected_minutes,
        )
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def fetch_prior_session_close(
    *,
    port: int = 4001,
    client_id: int = 101,
    symbol: str = "MES",
    contract_month: str = "202603",
    exchange: str = "CME",
    cme_trade_date: Union[str, dt.date, dt.datetime],
    whatToShow: str = "TRADES",
    useRTH: bool = False,
    host: str = "127.0.0.1",
    strict_expected_minutes: bool = False,
    num_schedule_days: int = 10,
) -> Optional[float]:
    """Fetch the final close from the scheduled session before a CME trade date."""
    td = _coerce_cme_trade_date(cme_trade_date)

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id)

        contract = Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=contract_month,
            exchange=exchange,
            currency="USD",
        )
        ib.qualifyContracts(contract)

        sessions = get_cme_session_windows_ct(
            ib,
            contract,
            td,
            num_schedule_days=num_schedule_days,
            useRTH=useRTH,
        )
        if find_session_window(sessions, td) is None:
            return None
        prior_session = find_prior_session_window(sessions, td)
        if prior_session is None:
            return None

        close_df = _fetch_1m_close_for_session_window(
            ib,
            contract,
            prior_session,
            whatToShow=whatToShow,
            useRTH=useRTH,
            strict_expected_minutes=strict_expected_minutes,
        )
        return _last_close_from_1m_close_df(close_df)
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def compute_entry_features(
    port: int = 4001,
    client_id: int = 101,
    symbol: str = "MES",
    contract_month: str = "202603",
    exchange: str = "CME",
    entry_start_ct_str: str = "2026-02-20 09:00",
    reg_window: int = 12,
    ema_spans: tuple[int, ...] = (20, 50),
    ema_slope_k: int = 3,
    atr_period: int = 14,
    eth_context_bars: int = 300,
):
    tz = pytz.timezone("America/Chicago")
    entry_start = tz.localize(dt.datetime.strptime(entry_start_ct_str, "%Y-%m-%d %H:%M"))
    entry_end = entry_start + dt.timedelta(minutes=5)

    # RTH: 08:30–15:00 CT
    rth_start = entry_start.replace(hour=8, minute=30, second=0, microsecond=0)
    rth_end   = entry_start.replace(hour=15, minute=0, second=0, microsecond=0)
    is_rth = (entry_start >= rth_start) and (entry_start < rth_end)

    # Request window
    if is_rth:
        start_needed = rth_start
    else:
        start_needed = entry_start - dt.timedelta(minutes=eth_context_bars * 5)

    duration_seconds = int((entry_end - start_needed).total_seconds())
    if duration_seconds <= 0:
        raise ValueError("duration_seconds <= 0, check time inputs.")

    ib = IB()
    ib.connect("127.0.0.1", port, clientId=client_id)

    contract = Future(symbol=symbol, lastTradeDateOrContractMonth=contract_month, exchange=exchange, currency="USD")
    ib.qualifyContracts(contract)

    df = fetch_5m_bars(ib, contract, end_time_ct=entry_end, duration_seconds=duration_seconds, useRTH=False)
    ib.disconnect()

    if df.empty:
        raise RuntimeError("No bars returned (permissions/subscription/contract/pacing).")

    # Entry candle (printed only)
    entry_row = df.loc[df["date"] == entry_start]
    if entry_row.empty:
        raise RuntimeError("Entry candle not found.")
    entry_row = entry_row.iloc[0]

    # Exclude entry candle from ALL calculations
    pre_df = df[df["date"] < entry_start].copy()

    # Calc window (RTH uses 08:30->entry; ETH uses previous bars)
    if is_rth:
        calc_df = pre_df[(pre_df["date"] >= rth_start) & (pre_df["date"] < entry_start)].copy()
        calc_mode = "RTH: 08:30 -> entry"
    else:
        calc_df = pre_df.copy()
        calc_mode = "ETH: previous N bars"

    # ✅ ATR: prioritize atr_period window (within calc_df), fallback to available
    atr_at_entry = compute_atr_at_entry(calc_df, atr_period=atr_period)

    def norm_by_atr(x: float) -> float:
        return (x / atr_at_entry) if (np.isfinite(atr_at_entry) and atr_at_entry > 0) else np.nan

    # Regression: RTH uses last reg_window within 08:30->entry if possible; else all available
    if is_rth:
        reg_slice = calc_df.tail(reg_window) if len(calc_df) >= reg_window else calc_df
    else:
        reg_slice = calc_df.tail(reg_window)

    reg_slope_per_bar, reg_r2 = linreg_slope_r2(reg_slice["close"].to_numpy(dtype=float) if len(reg_slice) else np.array([]))
    reg_slope_norm = norm_by_atr(reg_slope_per_bar)

    # EMA slopes (UNCHANGED behavior): computed on calc_df, slope over k bars, normalized by ATR
    ema_out = {}
    closes = calc_df["close"].astype(float) if len(calc_df) else pd.Series(dtype=float)

    for span in ema_spans:
        if len(closes) < (ema_slope_k + 1):
            ema_out[f"EMA{span}_slope_per_bar_k{ema_slope_k}"] = np.nan
            ema_out[f"EMA{span}_slope_norm_by_ATR_k{ema_slope_k}"] = np.nan
            continue

        ema_ser = ema_series(closes, span=span)
        ema_now = float(ema_ser.iloc[-1])
        ema_prev = float(ema_ser.iloc[-1 - ema_slope_k])
        ema_slope_per_bar = (ema_now - ema_prev) / ema_slope_k

        ema_out[f"EMA{span}_slope_per_bar_k{ema_slope_k}"] = ema_slope_per_bar
        ema_out[f"EMA{span}_slope_norm_by_ATR_k{ema_slope_k}"] = norm_by_atr(ema_slope_per_bar)

    # Print values only
    print("=== Mode ===")
    print(calc_mode)

    print("\n=== Entry candle (printed only; excluded from calculations) ===")
    print(f"time_start={entry_start}  O={entry_row['open']} H={entry_row['high']} L={entry_row['low']} C={entry_row['close']}")

    print("\n=== ATR (as-of entry; excluded entry candle) ===")
    print(f"ATR_period={atr_period}  ATR_at_entry={atr_at_entry}")

    print("\n=== Regression (excluded entry candle) ===")
    if len(reg_slice) > 0:
        print(f"bars_used={len(reg_slice)}  start={reg_slice['date'].iloc[0]}  end={reg_slice['date'].iloc[-1]}")
    else:
        print("bars_used=0")
    print(f"slope_per_bar={reg_slope_per_bar}  slope_norm_by_ATR={reg_slope_norm}  r2={reg_r2}")

    print("\n=== EMA slopes (excluded entry candle) ===")
    for k, v in ema_out.items():
        print(f"{k}={v}")

    return {
        "mode": calc_mode,
        "entry_candle": {
            "time_start": entry_start,
            "open": float(entry_row["open"]),
            "high": float(entry_row["high"]),
            "low": float(entry_row["low"]),
            "close": float(entry_row["close"]),
        },
        "atr": {"period": int(atr_period), "atr_at_entry": atr_at_entry},
        "regression": {
            "bars_used": int(len(reg_slice)),
            "slope_per_bar": reg_slope_per_bar,
            "slope_norm_by_ATR": reg_slope_norm,
            "r2": reg_r2,
            "window_start": reg_slice["date"].iloc[0] if len(reg_slice) else None,
            "window_end": reg_slice["date"].iloc[-1] if len(reg_slice) else None,
        },
        "ema_slope": {"k": int(ema_slope_k), **ema_out},
    }


# -----------------------------------------------------------------------------
# New feature: tick-based MFE (max favorable excursion) between entry and exit
# -----------------------------------------------------------------------------
def _as_tz(dt_obj: dt.datetime, tz) -> dt.datetime:
    """Return tz-aware datetime. If naive, assume UTC and localize."""
    if dt_obj.tzinfo is None:
        return pytz.UTC.localize(dt_obj).astimezone(tz)
    return dt_obj.astimezone(tz)


def _tick_time_to_ct(t, tz_ct) -> Optional[dt.datetime]:
    """Convert IB tick time (datetime or epoch seconds) to CT tz-aware datetime."""
    try:
        if isinstance(t, dt.datetime):
            return _as_tz(t, tz_ct)
        # many IB tick types use epoch seconds
        ts = int(t)
        return dt.datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(tz_ct)
    except Exception:
        return None


def compute_position_mfe_mae(
    *,
    port: int = 4001,
    client_id: int = 101,
    symbol: str = "MES",
    contract_month: str = "202603",
    exchange: str = "CME",
    entry_time_ct: dt.datetime,
    exit_time_ct: dt.datetime,
    entry_price: float,
    direction: str,
    position_qty: float = 1.0,
    contract_multiplier: float = 5.0,
    whatToShow: str = "TRADES",
    useRTH: bool = False,
    ticks_per_page: int = 1000,
    max_pages: int = 200,
) -> Optional[Dict[str, Any]]:
    """Compute tick-based MFE and MAE between entry and exit.

    Uses IBKR historical ticks and pages backwards from `exit_time_ct` to `entry_time_ct`.

    Definitions (all in *points*, returned as NON-negative magnitudes):
      - Long:
          MFE = max(0, max_price_between(entry, exit) - entry_price)
          MAE = max(0, entry_price - min_price_between(entry, exit))
      - Short:
          MFE = max(0, entry_price - min_price_between(entry, exit))
          MAE = max(0, max_price_between(entry, exit) - entry_price)

    Also records:
      - mfe_time / mae_time: time offset from entry_time_ct, formatted as "mm:ss".

    Notes:
      - Best-effort: returns None if no ticks are available.
      - Standalone helper: does not change existing indicator logic.
    """
    tz_ct = pytz.timezone("America/Chicago")
    entry_ct = entry_time_ct if entry_time_ct.tzinfo else tz_ct.localize(entry_time_ct)
    exit_ct = exit_time_ct if exit_time_ct.tzinfo else tz_ct.localize(exit_time_ct)

    if exit_ct <= entry_ct:
        return None

    d = (direction or "").strip().lower()
    is_long = d.startswith("long")
    is_short = d.startswith("short")
    if not (is_long or is_short):
        return None

    def _fmt_mmss(offset_sec: float) -> str:
        s = max(0.0, float(offset_sec))
        minutes = int(s // 60.0)
        seconds = int(round(s % 60.0))
        if seconds == 60:
            minutes += 1
            seconds = 0
        return f"{minutes:02d}:{seconds:02d}"

    ib = IB()
    try:
        ib.connect("127.0.0.1", port, clientId=client_id)
        contract = Future(symbol=symbol, lastTradeDateOrContractMonth=contract_month, exchange=exchange, currency="USD")
        ib.qualifyContracts(contract)

        high_price: Optional[float] = None
        high_time_ct: Optional[dt.datetime] = None
        low_price: Optional[float] = None
        low_time_ct: Optional[dt.datetime] = None

        end_cursor = exit_ct
        pages = 0

        while pages < max_pages:
            pages += 1
            try:
                ticks = ib.reqHistoricalTicks(
                    contract,
                    startDateTime="",
                    endDateTime=end_cursor,
                    numberOfTicks=int(ticks_per_page),
                    whatToShow=whatToShow,
                    useRth=useRTH,
                    ignoreSize=True,
                )
            except Exception:
                break

            if not ticks:
                break

            oldest_time_ct: Optional[dt.datetime] = None

            for tk in ticks:
                t_ct = _tick_time_to_ct(getattr(tk, "time", None), tz_ct)
                if t_ct is None:
                    continue

                if oldest_time_ct is None or t_ct < oldest_time_ct:
                    oldest_time_ct = t_ct

                # enforce window
                if t_ct < entry_ct or t_ct > exit_ct:
                    continue

                px = getattr(tk, "price", None)
                if px is None:
                    continue
                try:
                    px_f = float(px)
                except Exception:
                    continue

                if high_price is None or px_f > high_price:
                    high_price = px_f
                    high_time_ct = t_ct
                if low_price is None or px_f < low_price:
                    low_price = px_f
                    low_time_ct = t_ct

            if oldest_time_ct is None:
                break
            if oldest_time_ct <= entry_ct:
                break

            end_cursor = oldest_time_ct - dt.timedelta(seconds=1)

        if high_price is None or low_price is None or high_time_ct is None or low_time_ct is None:
            return None

        entry_px = float(entry_price)
        qty = float(position_qty)
        mult = float(contract_multiplier)

        if is_long:
            mfe_points = max(0.0, high_price - entry_px)
            mae_points = max(0.0, entry_px - low_price)
            mfe_price = float(high_price)
            mae_price = float(low_price)
            mfe_time_ct = high_time_ct
            mae_time_ct = low_time_ct
        else:
            mfe_points = max(0.0, entry_px - low_price)
            mae_points = max(0.0, high_price - entry_px)
            mfe_price = float(low_price)
            mae_price = float(high_price)
            mfe_time_ct = low_time_ct
            mae_time_ct = high_time_ct

        mfe_usd = mfe_points * qty * mult
        mae_usd = mae_points * qty * mult

        mfe_offset_sec = float((mfe_time_ct - entry_ct).total_seconds())
        mae_offset_sec = float((mae_time_ct - entry_ct).total_seconds())

        return {
            "mfe_points": float(mfe_points),
            "mfe_usd": float(mfe_usd),
            "mfe_price": float(mfe_price),
            "mfe_time_ct": mfe_time_ct,
            "mfe_offset_sec": mfe_offset_sec,
            "mfe_time": _fmt_mmss(mfe_offset_sec),

            "mae_points": float(mae_points),
            "mae_usd": float(mae_usd),
            "mae_price": float(mae_price),
            "mae_time_ct": mae_time_ct,
            "mae_offset_sec": mae_offset_sec,
            "mae_time": _fmt_mmss(mae_offset_sec),
        }
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

def try_fetch_investing_spx_ohlc(cash_date: date) -> Optional[Tuple[float, float, float, float]]:
    """
    Fetch SPX cash OHLC for a given (ET) cash date.

    IMPORTANT (locked behavior):
    - Call sites still use this function name; implementation has been switched from Investing.com HTML scraping
      to Stooq CSV (free, no key) for reliability.

    Data source:
      https://stooq.com/q/d/l/?s=^spx&d1=YYYYMMDD&d2=YYYYMMDD&i=d
    Returns (open, high, low, close) as floats, or None on failure / missing date.
    """
    if requests is None:
        return None

    # Cache per process to avoid repeated network calls (used by True Range lookup too)
    global _SPX_CACHE
    try:
        _SPX_CACHE
    except NameError:
        _SPX_CACHE = {}  # type: ignore

    if cash_date in _SPX_CACHE:
        return _SPX_CACHE[cash_date]

    d1 = cash_date.strftime("%Y%m%d")
    # Stooq endpoint can accept d1/d2; use same date for a single-day fetch.
    url = f"https://stooq.com/q/d/l/?s=^spx&d1={d1}&d2={d1}&i=d"

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None

        # Parse CSV content
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        if df.empty:
            return None

        # Expected columns: Date, Open, High, Low, Close, Volume
        # Date format: YYYY-MM-DD
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
        row = df[df["Date"] == cash_date]
        if row.empty:
            return None
        r = row.iloc[0]

        open_ = float(r["Open"])
        high = float(r["High"])
        low = float(r["Low"])
        close = float(r["Close"])

        ohlc = (open_, high, low, close)
        _SPX_CACHE[cash_date] = ohlc
        return ohlc
    except Exception:
        return None


def main() -> None:
    compute_entry_features(
        port=4001,
        client_id=101,
        entry_start_ct_str="2026-02-12 10:15",
        reg_window=12,
        ema_spans=(20, 50),
        ema_slope_k=3,
        atr_period=14,
        eth_context_bars=15,
    )


if __name__ == "__main__":
    main()
