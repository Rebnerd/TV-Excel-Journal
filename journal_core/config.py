"""Shared constants for the trading journal report pipeline."""

from __future__ import annotations

from zoneinfo import ZoneInfo

TZ_LOCAL = ZoneInfo("Asia/Shanghai")
TZ_CME = ZoneInfo("America/Chicago")

COMMISSION_PER_SIDE_BY_INSTRUMENT = {
    "MES": 0.62,
    "MNQ": 0.62,
    "ES": 2.25,
    "NQ": 2.25,
    "GC": 2.52,
    "MGC": 0.97,
}

DEFAULT_SUMMARY_CELLS = {
    "local_date": "B3",
    "cme_trade_date": "B4",
    "commission_per_side": "B7",
    "trades": "B21",
    "wins": "B22",
    "losses": "B23",
    "break_even": "B24",
    "winrate": "B25",
    "total_pnl_points": "B26",
    "total_win_points": "B27",
    "total_loss_points": "B28",
    "avg_win_points": "B29",
    "avg_loss_points": "B30",
    "total_pnl_usd": "B31",
    "gross_profit": "B32",
    "gross_loss": "B33",
    "profit_factor": "B34",
    "avg_win": "B35",
    "avg_loss": "B36",
    "payoff": "B37",
    "est_commission": "B38",
    "net_pnl": "B39",
}

ES_SUMMARY_CELLS = {
    "local_date": "B3",
    "cme_trade_date": "B4",
    "market_blocks": {
        "ES": {
            "open": "B9",
            "high": "B10",
            "low": "B11",
            "close": "B12",
            "direction": "B13",
            "eth_range": "B14",
            "eth_range_pct": "B15",
            "eth_true_range": "B16",
            "rth_range": "B17",
            "rth_range_pct": "B18",
            "rth_true_range": "B19",
        },
    },
    "trades": "B22",
    "wins": "B23",
    "losses": "B24",
    "break_even": "B25",
    "winrate": "B26",
    "total_pnl_points": "B27",
    "total_win_points": "B28",
    "total_loss_points": "B29",
    "avg_win_points": "B30",
    "avg_loss_points": "B31",
    "total_pnl_usd": "B32",
    "gross_profit": "B33",
    "gross_loss": "B34",
    "profit_factor": "B35",
    "avg_win": "B36",
    "avg_loss": "B37",
    "payoff": "B38",
    "est_commission": "B39",
    "net_pnl": "B40",
}

NQ_GC_SUMMARY_CELLS = {
    "local_date": "B3",
    "cme_trade_date": "B4",
    "commission_per_side": "B7",
    "market_blocks": {
        "NQ": {
            "open": "B10",
            "high": "B11",
            "low": "B12",
            "close": "B13",
            "direction": "B14",
            "eth_range": "B15",
            "eth_range_pct": "B16",
            "eth_true_range": "B17",
            "rth_range": "B18",
            "rth_range_pct": "B19",
            "rth_true_range": "B20",
        },
        "GC": {
            "open": "B23",
            "high": "B24",
            "low": "B25",
            "close": "B26",
            "direction": "B27",
            "range": "B28",
            "range_pct": "B29",
            "true_range": "B30",
        },
    },
    "trades": "B33",
    "wins": "B34",
    "losses": "B35",
    "break_even": "B36",
    "winrate": "B37",
    "total_pnl_points": "B38",
    "total_win_points": "B39",
    "total_loss_points": "B40",
    "avg_win_points": "B41",
    "avg_loss_points": "B42",
    "total_pnl_usd": "B43",
    "gross_profit": "B44",
    "gross_loss": "B45",
    "profit_factor": "B46",
    "avg_win": "B47",
    "avg_loss": "B48",
    "payoff": "B49",
    "est_commission": "B50",
    "net_pnl": "B51",
}

SUMMARY_CELLS_BY_LAYOUT = {
    "default": DEFAULT_SUMMARY_CELLS,
    "es_shifted": ES_SUMMARY_CELLS,
    "nq_gc_market": NQ_GC_SUMMARY_CELLS,
}


def summary_cells_for_chart_symbols(chart_symbols: tuple[str, ...]) -> dict:
    """Return the Summary cell map for the selected template layout."""
    if chart_symbols == ("ES",):
        return ES_SUMMARY_CELLS
    if chart_symbols == ("NQ", "GC"):
        return NQ_GC_SUMMARY_CELLS
    return DEFAULT_SUMMARY_CELLS

CURVE_START_ROW = 5
CURVE_COLS = {
    "time_label": 1,
    "cum_pnl": 2,
    "cum_pos": 3,
    "cum_neg": 4,
}

DURATION_START_ROW = 2
DURATION_BOUNDARY_PAD_SECONDS = 20
DURATION_COLS = {
    "duration_label": 1,
    "winning_amount_usd": 2,
    "losing_amount_usd": 3,
    "winning_mfe_usd": 4,
    "winning_mae_usd": 5,
    "losing_mfe_usd": 6,
    "losing_mae_usd": 7,
}

CHART_DATA_START_ROW = 2
CHART_DATA_COLS = {
    "time": 1,
    "close": 2,
    "long_win": 3,
    "long_loss": 4,
    "short_win": 5,
    "short_loss": 6,
}

TRADE_COLS = [
    "id",
    "symbol",
    "direction",
    "qty",
    "entry_type",
    "exit_type",
    "entry_price",
    "exit_price",
    "duration",
    "mfe_time",
    "mae_time",
    "pnl_usd",
    "mfe_usd",
    "mae_usd",
    "pnl_usd_per_min",
    "pnl_points",
    "mfe_points",
    "mae_points",
    "pnl_points_per_min",
    "aligned",
    "scale_in",
    "atr14",
    "ema20sn",
    "regsn",
    "r2",
    "review",
    "entry_time",
    "exit_time",
    "entry_order_id",
    "exit_order_id",
]
