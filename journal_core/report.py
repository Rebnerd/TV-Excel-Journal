"""Public orchestration for daily trading journal report generation."""

from __future__ import annotations

from datetime import date, datetime, time

import pandas as pd
from openpyxl import load_workbook

from .amp_import import load_executed_fills
from .chart_xml import patch_curve_chart_xml_from_template
from .config import (
    CHART_DATA_START_ROW,
    CURVE_START_ROW,
    DURATION_START_ROW,
    TZ_LOCAL,
    summary_cells_for_chart_symbols,
)
from .ibkr_enrichment import IBKRContextConfig as _IBKRContextConfig
from .ibkr_enrichment import enrich_trades_inplace as _enrich_trades_inplace
from .ibkr_market_data import fetch_full_session_1m_close, fetch_prior_session_close
from .instruments import (
    chart_data_sheet_name,
    chart_symbol_for_instrument,
    ordered_chart_symbols,
    resolve_instrument_list,
    resolve_template_path,
)
from .metrics import compute_metrics
from .time_utils import cme_trade_date_from_local, parse_amp_datetime
from .trade_matching import build_trades_from_fills
from .workbook_writer import (
    build_chart_data_rows,
    build_market_summary,
    get_chart_data_rth_split_rows,
    require_sheet,
    write_chart_data,
    write_curve,
    write_duration_distribution,
    write_summary,
    write_trades,
)


def generate_daily_report(
    *,
    csv_path: str,
    local_date,
    out_path: str,
    template_path: str | None = None,
    template_dir: str | None = None,
    instrument_symbols: list[str] | tuple[str, ...] | None = None,
    instrument_symbol: str = "MESM26",
    contract_month: str | None = None,
    exchange: str | None = None,

    # Optional: IBKR enrichment config
    ibkr_port: int = 4001,
    ibkr_client_id: int = 101,
    enable_historical_context: bool = True,

    # Optional: full-session 1-minute close chart data
    chart_host: str = "127.0.0.1",
    chart_useRTH: bool = False,
    chart_strict_expected_minutes: bool = False,
) -> str:
    """Generate a daily report from AMP fills and the configured Excel template."""

    # Parse local date
    if isinstance(local_date, datetime):
        local_day = local_date.date()
    elif isinstance(local_date, date):
        local_day = local_date
    elif isinstance(local_date, str):
        local_day = datetime.strptime(local_date, "%Y-%m-%d").date()
    else:
        raise TypeError(f"local_date must be str/date/datetime, got: {type(local_date)!r}")

    chart_instrument_configs = resolve_instrument_list(
        instrument_symbols=instrument_symbols,
        fallback_instrument_symbol=instrument_symbol,
        contract_month=contract_month,
        exchange=exchange,
    )
    fallback_instrument = chart_instrument_configs[0]
    chart_symbols = ordered_chart_symbols([inst.symbol for inst in chart_instrument_configs])
    if not chart_symbols:
        chart_symbols = (chart_symbol_for_instrument(fallback_instrument.symbol),)
    chart_instruments = {}
    for inst in chart_instrument_configs:
        chart_symbol = chart_symbol_for_instrument(inst.symbol)
        existing = chart_instruments.get(chart_symbol)
        if existing is None or existing.symbol != chart_symbol and inst.symbol == chart_symbol:
            chart_instruments[chart_symbol] = inst

    template_path = resolve_template_path(
        chart_symbols=chart_symbols,
        template_path=template_path,
        template_dir=template_dir,
    )

    df = load_executed_fills(csv_path)

    # Parse status time (Asia/Shanghai)
    status_dt = df["Status Time"].apply(parse_amp_datetime)
    df = df.assign(__status_dt=status_dt)

    # Locked convention: report by CME "trade date" session for a given local date.
    target_cme_trade_date = cme_trade_date_from_local(
        datetime.combine(local_day, time(0, 0)).replace(tzinfo=TZ_LOCAL)
    )

    # Filter fills by CME trade date (session 17:00-16:00 CT)
    df["__cme_trade_date"] = df["__status_dt"].apply(cme_trade_date_from_local)
    df = df[df["__cme_trade_date"] == target_cme_trade_date].copy()

    # Sort by time to enforce deterministic sequencing
    df = df.sort_values("__status_dt").reset_index(drop=True)

    # Build trades
    trades = build_trades_from_fills(df, point_value=fallback_instrument.point_value)

    # Enrich trades with IBKR context / MFE / MAE (best-effort; leaves blanks if unavailable)
    if enable_historical_context and _enrich_trades_inplace is not None:
        try:
            if _IBKRContextConfig is not None:
                cfg = _IBKRContextConfig(
                    port=int(ibkr_port),
                    client_id=int(ibkr_client_id),
                    symbol=chart_instruments[chart_symbols[0]].symbol,
                    contract_month=chart_instruments[chart_symbols[0]].contract_month,
                    exchange=chart_instruments[chart_symbols[0]].exchange,
                    mfe_contract_multiplier=chart_instruments[chart_symbols[0]].point_value,
                )
                _enrich_trades_inplace(trades, cfg=cfg)
            else:
                _enrich_trades_inplace(trades)
        except Exception:
            pass

    # CME trade date for this report
    cme_td = target_cme_trade_date

    summary_cells = summary_cells_for_chart_symbols(chart_symbols)
    market_summary_symbols = tuple(summary_cells.get("market_blocks", {}))
    chart_close_by_symbol = {}
    if enable_historical_context:
        for chart_symbol in chart_symbols:
            chart_inst = chart_instruments[chart_symbol]
            chart_close_by_symbol[chart_symbol] = fetch_full_session_1m_close(
                port=int(ibkr_port),
                client_id=int(ibkr_client_id),
                symbol=chart_symbol,
                contract_month=chart_inst.contract_month,
                exchange=chart_inst.exchange,
                cme_trade_date=cme_td,
                host=str(chart_host),
                useRTH=bool(chart_useRTH),
                strict_expected_minutes=bool(chart_strict_expected_minutes),
            )

    market_summary_by_symbol = {}
    if enable_historical_context:
        for chart_symbol in market_summary_symbols:
            chart_inst = chart_instruments[chart_symbol]
            prior_close = fetch_prior_session_close(
                port=int(ibkr_port),
                client_id=int(ibkr_client_id),
                symbol=chart_symbol,
                contract_month=chart_inst.contract_month,
                exchange=chart_inst.exchange,
                cme_trade_date=cme_td,
                host=str(chart_host),
                useRTH=bool(chart_useRTH),
                strict_expected_minutes=bool(chart_strict_expected_minutes),
            )
            market_summary_by_symbol[chart_symbol] = build_market_summary(
                chart_close_by_symbol.get(chart_symbol),
                cme_td,
                prior_close,
            )

    # Compute metrics
    metrics = compute_metrics(trades)

    # Load template
    wb = load_workbook(template_path)

    # Write sections
    write_summary(
        require_sheet(wb, "Summary"),
        local_day,
        cme_td,
        market_summary_by_symbol,
        metrics,
        summary_cells=summary_cells,
    )
    write_trades(require_sheet(wb, "Trades"), trades)
    curve_ws = require_sheet(wb, "Curve")
    duration_ws = require_sheet(wb, "DurationDist")
    curve_end_row = write_curve(curve_ws, trades, cme_td)
    duration_end_row = write_duration_distribution(duration_ws, trades)
    multi_symbol_chart = len(chart_symbols) > 1
    chart_data_ranges = {}
    total_chart_rows = 0
    for chart_symbol in chart_symbols:
        sheet_name = chart_data_sheet_name(chart_symbol, multi_symbol=multi_symbol_chart)
        chart_data_ws = require_sheet(wb, sheet_name)
        symbol_trades = [tr for tr in trades if tr.get("chart_symbol") == chart_symbol]
        close_df = chart_close_by_symbol.get(
            chart_symbol,
            pd.DataFrame(columns=["time_ct", "close", "cme_trade_date"]),
        )
        chart_rows = build_chart_data_rows(close_df, symbol_trades) if enable_historical_context else []
        chart_data_end_row = write_chart_data(chart_data_ws, chart_rows)
        chart_data_eth_end_row, chart_data_rth_start_row = get_chart_data_rth_split_rows(chart_rows, cme_td)
        chart_data_ranges[sheet_name] = {
            "start_row": CHART_DATA_START_ROW,
            "end_row": chart_data_end_row,
            "eth_end_row": chart_data_eth_end_row,
            "rth_start_row": chart_data_rth_start_row,
        }
        total_chart_rows += len(chart_rows)

    # Save
    wb.save(out_path)

    # Preserve template chart styling, while rewriting Curve, DurationDist, and Chart Data ranges.
    patch_curve_chart_xml_from_template(
        template_path,
        out_path,
        CURVE_START_ROW,
        curve_end_row,
        curve_data_sheet_name="Curve",
        duration_start_row=DURATION_START_ROW,
        duration_end_row=duration_end_row,
        duration_data_sheet_name="DurationDist",
        chart_data_start_row=CHART_DATA_START_ROW,
        chart_data_end_row=next(iter(chart_data_ranges.values()))["end_row"],
        chart_data_eth_end_row=next(iter(chart_data_ranges.values()))["eth_end_row"],
        chart_data_rth_start_row=next(iter(chart_data_ranges.values()))["rth_start_row"],
        chart_data_sheet_name=next(iter(chart_data_ranges)),
        chart_data_ranges=chart_data_ranges,
    )

    # Console hint (optional)
    print(f"OK: wrote {out_path}")
    print(f"Trades: {len(trades)} | CME trade date: {cme_td} | Net P&L: {metrics['net_pnl']:.2f}")
    if enable_historical_context:
        print(f"Chart Data rows: {total_chart_rows}")

    return out_path
