from pathlib import Path

from journal_core import generate_daily_report


RAW_ORDER_HISTORY_DIR = Path("../raw_order_history")


def latest_order_history_csv(folder: Path = RAW_ORDER_HISTORY_DIR) -> str:
    folder = Path(folder)
    candidates = sorted(
        folder.glob("amp-order-history-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        candidates = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No CSV files found in {folder.resolve()}")
    return str(candidates[0])


OUTPUT_DIR = "../daily_report/"
# OUTPUT_DIR = "../daily_report1/"

REPORT_DAYS = [16]

# Supported instrument roots:
# - Equity index: ES, MES, NQ, MNQ
# - Metals: GC, MGC
#
# Futures month codes:
# F Jan, G Feb, H Mar, J Apr, K May, M Jun,
# N Jul, Q Aug, U Sep, V Oct, X Nov, Z Dec
#
# Examples:
# - ESM26 -> ES June 2026
# - NQM26 -> NQ June 2026
# - GCM26 -> GC June 2026
#
# Template selection is automatic from instrument_symbols:
# - ["ESM26"] -> templates/daily_report_template_es.xlsx
# - ["NQM26", "GCM26"] -> templates/daily_report_template_nq_gc.xlsx
# - ["ESM26", "NQM26", "GCM26"] -> templates/daily_report_template_es_nq_gc.xlsx
REPORT_CONFIG = {
    "csv_path": latest_order_history_csv(),
    # Choose exactly one instrument_symbols line by commenting lines on/off.
    "instrument_symbols": ["ESM26"],
    # "instrument_symbols": ["NQM26"],
    # "instrument_symbols": ["GCM26"],
    # "instrument_symbols": ["ESM26", "NQM26"],
    # "instrument_symbols": ["ESM26", "GCM26"],
    # "instrument_symbols": ["NQM26", "GCM26"],
    # "instrument_symbols": ["ESM26", "NQM26", "GCM26"],

    # Optional template controls:
    # "template_dir": r"templates",
    # "template_path": r"templates/daily_report_template_es_nq.xlsx",

    # IBKR / historical context controls.
    "ibkr_port": 4001,
    "ibkr_client_id": 101,
    "enable_historical_context": True,

    # Optional chart fetch controls.
    # "chart_host": "127.0.0.1",
    # "chart_useRTH": False,
    # "chart_strict_expected_minutes": False,
}


for day in REPORT_DAYS:
    local_date = f"2026-05-{day}"
    out_path = OUTPUT_DIR + f"daily_report_{local_date}.xlsx"

    print(f"Using order history CSV: {REPORT_CONFIG['csv_path']}")
    generate_daily_report(
        local_date=local_date,
        out_path=out_path,
        **REPORT_CONFIG,
    )
