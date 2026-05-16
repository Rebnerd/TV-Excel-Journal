"""Timezone, date, and duration helpers for report generation."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional

import pandas as pd

from .config import TZ_CME, TZ_LOCAL

def parse_amp_datetime(s: str) -> datetime:
    """
    Parse AMP CSV datetime fields.

    Observed formats:
      - '01/23/26 21:01:10'      (MM/DD/YY HH:MM:SS)
      - '2026-01-24 03:48:45'    (YYYY-MM-DD HH:MM:SS)
      - Some exports may include milliseconds.
    Interpret as Asia/Shanghai local time (naive -> attach TZ_LOCAL).
    """
    s = str(s).strip()
    fmts = (
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S.%f",
    )
    for fmt in fmts:
        try:
            dt_naive = datetime.strptime(s, fmt)
            return dt_naive.replace(tzinfo=TZ_LOCAL)
        except ValueError:
            continue

    # Last resort: pandas parser (still no timezone in source)
    try:
        dt_naive = pd.to_datetime(s).to_pydatetime()
        if isinstance(dt_naive, datetime):
            return dt_naive.replace(tzinfo=TZ_LOCAL)
    except Exception:
        pass

    raise ValueError(f"Unrecognized AMP datetime format: {s!r}")

def cme_trade_date_from_local(dt_local: datetime) -> date:
    """
    CME trade date is defined by Globex session 17:00–16:00 CT.
    We label the session by its **end date** (the CT calendar date on which the session ends at 16:00 CT).

    For a CT timestamp:
      - if time >= 17:00 -> belongs to the **next** trade date (ct_date + 1)
      - if time < 16:00  -> belongs to the **same** trade date (ct_date)
      - 16:00–17:00 is the CME maintenance break (should not contain executions).
    """
    dt_ct = dt_local.astimezone(TZ_CME)
    t = dt_ct.timetz().replace(tzinfo=None)
    d = dt_ct.date()
    if t >= time(17, 0, 0):
        return d + timedelta(days=1)
    if t < time(16, 0, 0):
        return d
    return d

def safe_div(a: float, b: float) -> Optional[float]:
    if b is None or b == 0:
        return None
    return a / b

def fmt_duration(entry: datetime, exit: datetime) -> str:
    secs = max(0, int(round((exit - entry).total_seconds())))
    m = secs // 60
    s = secs % 60
    return f"{m}:{s:02d}"

def parse_duration_to_seconds(value) -> Optional[int]:
    """Parse duration values like M:SS or H:MM:SS into total seconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < 0:
            return None
        return int(round(float(value)))

    s = str(value).strip()
    if not s:
        return None

    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
    except Exception:
        return None

    if len(nums) == 2:
        minutes, seconds = nums
        if minutes < 0 or seconds < 0:
            return None
        return minutes * 60 + seconds
    if len(nums) == 3:
        hours, minutes, seconds = nums
        if hours < 0 or minutes < 0 or seconds < 0:
            return None
        return hours * 3600 + minutes * 60 + seconds
    return None

def format_duration_label(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"

def floor_datetime_to_minute(dt_obj: datetime) -> datetime:
    return dt_obj.replace(second=0, microsecond=0)
