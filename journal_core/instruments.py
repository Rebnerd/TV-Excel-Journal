"""Supported futures instruments and fixed contract metadata."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class InstrumentConfig:
    """Resolved instrument metadata used throughout one report run."""

    symbol: str
    contract_month: str
    exchange: str
    point_value: float


_INSTRUMENT_REGISTRY = {
    "MES": {"point_value": 5.0, "exchange": "CME"},
    "MNQ": {"point_value": 2.0, "exchange": "CME"},
    "ES": {"point_value": 50.0, "exchange": "CME"},
    "NQ": {"point_value": 20.0, "exchange": "CME"},
    "GC": {"point_value": 100.0, "exchange": "COMEX"},
    "MGC": {"point_value": 10.0, "exchange": "COMEX"},
}

_CHART_SYMBOL_BY_INSTRUMENT = {
    "MES": "ES",
    "MNQ": "NQ",
    "ES": "ES",
    "NQ": "NQ",
    "GC": "GC",
    "MGC": "GC",
}

_TEMPLATE_SYMBOL_ORDER = ("ES", "NQ", "GC")

_FUTURES_MONTH_CODES = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}


def _contract_month_from_code(month_code: str, year_code: str) -> str:
    month = _FUTURES_MONTH_CODES.get(month_code.upper())
    if month is None:
        supported = ", ".join(_FUTURES_MONTH_CODES)
        raise ValueError(f"Unsupported futures month code {month_code!r}. Supported month codes: {supported}.")

    year_raw = int(year_code)
    year = 2000 + year_raw if year_raw >= 10 else 2020 + year_raw
    return f"{year:04d}{month:02d}"


def _split_instrument_symbol(instrument_symbol: str) -> tuple[str, str | None]:
    value = str(instrument_symbol or "").strip().upper()
    if not value:
        raise ValueError("instrument_symbol is required, for example 'MESM26'.")

    for symbol in sorted(_INSTRUMENT_REGISTRY, key=len, reverse=True):
        suffix = value.removeprefix(symbol)
        if suffix == "":
            return symbol, None
        if len(suffix) in (2, 3) and suffix[0] in _FUTURES_MONTH_CODES and suffix[1:].isdigit():
            return symbol, _contract_month_from_code(suffix[0], suffix[1:])

    supported = ", ".join(sorted(_INSTRUMENT_REGISTRY))
    raise ValueError(
        f"Unsupported instrument_symbol {instrument_symbol!r}. "
        f"Use one of {supported}, optionally with a futures month code like MESM26."
    )


def resolve_instrument(
    *,
    instrument_symbol: str = "MESM26",
    contract_month: str | None = None,
    exchange: str | None = None,
) -> InstrumentConfig:
    """Resolve user-facing instrument inputs into fixed report metadata."""
    symbol, parsed_contract_month = _split_instrument_symbol(instrument_symbol)

    month = str(contract_month or parsed_contract_month or "").strip()
    if not month:
        raise ValueError(
            "contract_month is required when instrument_symbol omits the futures month code. "
            "Pass instrument_symbol like 'MESM26' or contract_month like '202606'."
        )

    metadata = _INSTRUMENT_REGISTRY[symbol]
    resolved_exchange = str(exchange or metadata["exchange"]).strip().upper()
    if not resolved_exchange:
        raise ValueError("exchange cannot be blank.")

    return InstrumentConfig(
        symbol=symbol,
        contract_month=month,
        exchange=resolved_exchange,
        point_value=float(metadata["point_value"]),
    )


def resolve_broker_symbol(raw_symbol: str) -> InstrumentConfig:
    """Resolve broker symbols such as F.US.MESH26, MESM6, or ESM26."""
    value = str(raw_symbol or "").strip().upper()
    if not value:
        raise ValueError("Broker symbol is blank.")

    for symbol in sorted(_INSTRUMENT_REGISTRY, key=len, reverse=True):
        pattern = rf"{re.escape(symbol)}([{''.join(_FUTURES_MONTH_CODES)}])(\d{{1,2}})\b"
        match = re.search(pattern, value)
        if match:
            return resolve_instrument(
                instrument_symbol=f"{symbol}{match.group(1)}{match.group(2)}",
            )

    return resolve_instrument(instrument_symbol=value)


def chart_symbol_for_instrument(symbol: str) -> str:
    """Map micro/full contracts into the chart/template symbol family."""
    root = str(symbol or "").strip().upper()
    try:
        return _CHART_SYMBOL_BY_INSTRUMENT[root]
    except KeyError as exc:
        supported = ", ".join(sorted(_CHART_SYMBOL_BY_INSTRUMENT))
        raise ValueError(f"Unsupported chart symbol root {symbol!r}. Supported roots: {supported}.") from exc


def ordered_chart_symbols(symbols) -> tuple[str, ...]:
    """Return detected chart symbols in template order."""
    unique = {chart_symbol_for_instrument(s) for s in symbols}
    return tuple(symbol for symbol in _TEMPLATE_SYMBOL_ORDER if symbol in unique)


def resolve_instrument_list(
    *,
    instrument_symbols: list[str] | tuple[str, ...] | str | None = None,
    fallback_instrument_symbol: str = "MESM26",
    contract_month: str | None = None,
    exchange: str | None = None,
) -> tuple[InstrumentConfig, ...]:
    """Resolve the user-requested instruments that drive chart/template output."""
    if instrument_symbols is None:
        return (
            resolve_instrument(
                instrument_symbol=fallback_instrument_symbol,
                contract_month=contract_month,
                exchange=exchange,
            ),
        )

    requested = (instrument_symbols,) if isinstance(instrument_symbols, str) else tuple(instrument_symbols)
    if not requested:
        raise ValueError("instrument_symbols cannot be empty.")

    return tuple(resolve_instrument(instrument_symbol=symbol) for symbol in requested)


def chart_data_sheet_name(chart_symbol: str, *, multi_symbol: bool) -> str:
    """Return the helper sheet name for a chart symbol."""
    return f"Chart Data {chart_symbol}" if multi_symbol else "Chart Data"


def template_name_for_chart_symbols(chart_symbols: tuple[str, ...]) -> str:
    """Return the workbook template filename for detected chart symbols."""
    if not chart_symbols:
        raise ValueError("Cannot select a report template without at least one chart symbol.")

    unsupported = [s for s in chart_symbols if s not in _TEMPLATE_SYMBOL_ORDER]
    if unsupported:
        raise ValueError(f"Unsupported template chart symbol set: {chart_symbols!r}.")

    slug = "_".join(symbol.lower() for symbol in chart_symbols)
    return f"daily_report_template_{slug}.xlsx"


def resolve_template_path(
    *,
    chart_symbols: tuple[str, ...],
    template_path: str | None = None,
    template_dir: str | None = None,
) -> str:
    """Resolve explicit or automatically selected template path."""
    if template_path:
        return str(template_path)

    root = Path(template_dir) if template_dir else Path(__file__).resolve().parent.parent / "templates"
    candidate = root / template_name_for_chart_symbols(chart_symbols)
    if not candidate.exists():
        raise FileNotFoundError(f"Could not find template for {chart_symbols}: {candidate}")
    return str(candidate)


def supported_instrument_symbols() -> tuple[str, ...]:
    """Return supported instrument symbols in display order."""
    return tuple(_INSTRUMENT_REGISTRY)
