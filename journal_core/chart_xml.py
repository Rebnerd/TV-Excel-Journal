"""Preserve template chart packages while rewriting data ranges."""

from __future__ import annotations

import os
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

from .config import CHART_DATA_START_ROW, DURATION_START_ROW

def patch_curve_chart_xml_from_template(
    template_xlsx: str,
    output_xlsx: str,
    start_row: int,
    end_row: int,
    curve_data_sheet_name: str = "Curve",
    duration_start_row: int = DURATION_START_ROW,
    duration_end_row: int = DURATION_START_ROW,
    duration_data_sheet_name: str = "DurationDist",
    chart_data_start_row: int = CHART_DATA_START_ROW,
    chart_data_end_row: int = CHART_DATA_START_ROW,
    chart_data_eth_end_row: int = CHART_DATA_START_ROW,
    chart_data_rth_start_row: int = CHART_DATA_START_ROW,
    chart_data_sheet_name: str = "Chart Data",
    chart_data_ranges: Optional[Dict[str, Dict[str, int]]] = None,
) -> None:
    """
    Restore chart packages from the template for *all* charts in the workbook.

    Important fixes:
    1) The P&L curve chart can now live on Summary while still reading its data
       from the sheet named "Curve". So the range rewrite must
       identify the *curve chart XML itself*, not assume the chart is hosted on
       a sheet named "Curve".
    1b) The duration distribution chart can live on Summary while reading its
       helper data from the sheet named "DurationDist". Its ranges must also be
       rewritten to the actual output extent.
    2) Matching charts by (sheet name, chart order) becomes unreliable once
       Summary contains multiple charts. Openpyxl / Excel can reorder chart
       entries inside a drawing, which makes "Summary chart #2" ambiguous.
       Match by (sheet name, anchor position) instead.
    """
    if end_row < start_row:
        end_row = start_row
    if duration_end_row < duration_start_row:
        duration_end_row = duration_start_row
    if chart_data_end_row < chart_data_start_row:
        chart_data_end_row = chart_data_start_row
    if chart_data_eth_end_row < chart_data_start_row:
        chart_data_eth_end_row = chart_data_start_row
    if chart_data_eth_end_row > chart_data_end_row:
        chart_data_eth_end_row = chart_data_end_row
    if chart_data_rth_start_row < chart_data_start_row:
        chart_data_rth_start_row = chart_data_start_row
    if chart_data_rth_start_row > chart_data_end_row:
        chart_data_rth_start_row = chart_data_end_row

    NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    NS_REL_DOC = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    NS_REL_PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
    NS_CHART = "http://schemas.openxmlformats.org/drawingml/2006/chart"
    NS_CT = "http://schemas.openxmlformats.org/package/2006/content-types"
    NS_XDR = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing"

    curve_data_sheet_name = curve_data_sheet_name or "Curve"
    duration_data_sheet_name = duration_data_sheet_name or "DurationDist"
    chart_data_sheet_name = chart_data_sheet_name or "Chart Data"

    def _norm_zip_path(p: str) -> str:
        return posixpath.normpath(str(p).replace("\\", "/").lstrip("/"))

    def _rels_path_for_part(part_path: str) -> str:
        part_path = _norm_zip_path(part_path)
        return _norm_zip_path(
            posixpath.join(posixpath.dirname(part_path), "_rels", posixpath.basename(part_path) + ".rels")
        )

    def _resolve_target(source_part: str, target: str) -> str:
        target = str(target or "").strip()
        if not target:
            raise ValueError(f"Empty relationship target for {source_part}")
        if target.startswith("/"):
            return _norm_zip_path(target)
        return _norm_zip_path(posixpath.join(posixpath.dirname(source_part), target))

    def _read_xml_from_zip(zf: zipfile.ZipFile, part_path: str) -> ET.Element:
        return ET.fromstring(zf.read(part_path))

    def _read_relationships(zf: zipfile.ZipFile, rels_path: str) -> List[ET.Element]:
        if rels_path not in zf.namelist():
            return []
        root = _read_xml_from_zip(zf, rels_path)
        return list(root.findall(f"{{{NS_REL_PKG}}}Relationship"))

    def _workbook_sheet_map(zf: zipfile.ZipFile) -> Dict[str, str]:
        workbook_path = "xl/workbook.xml"
        workbook_root = _read_xml_from_zip(zf, workbook_path)
        rel_map = {
            rel.attrib.get("Id"): _resolve_target(workbook_path, rel.attrib.get("Target", ""))
            for rel in _read_relationships(zf, "xl/_rels/workbook.xml.rels")
            if rel.attrib.get("Id") and rel.attrib.get("Target") and rel.attrib.get("TargetMode") != "External"
        }
        out: Dict[str, str] = {}
        for sheet in workbook_root.findall(f".//{{{NS_MAIN}}}sheet"):
            name = sheet.attrib.get("name")
            rid = sheet.attrib.get(f"{{{NS_REL_DOC}}}id")
            if name and rid and rid in rel_map:
                out[name] = rel_map[rid]
        return out

    def _find_drawing_part_for_sheet(zf: zipfile.ZipFile, sheet_part: str) -> Optional[str]:
        sheet_root = _read_xml_from_zip(zf, sheet_part)
        drawing = sheet_root.find(f".//{{{NS_MAIN}}}drawing")
        if drawing is None:
            return None
        rid = drawing.attrib.get(f"{{{NS_REL_DOC}}}id")
        if not rid:
            return None
        rel_map = {
            rel.attrib.get("Id"): _resolve_target(sheet_part, rel.attrib.get("Target", ""))
            for rel in _read_relationships(zf, _rels_path_for_part(sheet_part))
            if rel.attrib.get("Id") and rel.attrib.get("Target") and rel.attrib.get("TargetMode") != "External"
        }
        return rel_map.get(rid)

    def _anchor_signature(anchor_elem: ET.Element) -> Tuple:
        def _child_int(parent: ET.Element, local_name: str) -> Optional[int]:
            child = parent.find(f"{{{NS_XDR}}}{local_name}")
            if child is None or child.text is None:
                return None
            try:
                return int(child.text)
            except Exception:
                return None

        from_elem = anchor_elem.find(f"{{{NS_XDR}}}from")
        to_elem = anchor_elem.find(f"{{{NS_XDR}}}to")
        ext_elem = anchor_elem.find(f"{{{NS_XDR}}}ext")

        sig = [anchor_elem.tag]
        if from_elem is not None:
            sig.extend([
                _child_int(from_elem, "col"),
                _child_int(from_elem, "row"),
                _child_int(from_elem, "colOff"),
                _child_int(from_elem, "rowOff"),
            ])
        else:
            sig.extend([None, None, None, None])

        if to_elem is not None:
            sig.extend([
                _child_int(to_elem, "col"),
                _child_int(to_elem, "row"),
                _child_int(to_elem, "colOff"),
                _child_int(to_elem, "rowOff"),
            ])
        else:
            sig.extend([None, None, None, None])

        if ext_elem is not None:
            sig.extend([
                int(ext_elem.attrib.get("cx", "0")),
                int(ext_elem.attrib.get("cy", "0")),
            ])
        else:
            sig.extend([None, None])

        return tuple(sig)

    def _chart_bindings_for_workbook(zf: zipfile.ZipFile) -> Dict[Tuple, str]:
        bindings: Dict[Tuple, str] = {}
        for sheet_name, sheet_part in _workbook_sheet_map(zf).items():
            drawing_part = _find_drawing_part_for_sheet(zf, sheet_part)
            if not drawing_part:
                continue

            drawing_root = _read_xml_from_zip(zf, drawing_part)
            rel_map = {
                rel.attrib.get("Id"): _resolve_target(drawing_part, rel.attrib.get("Target", ""))
                for rel in _read_relationships(zf, _rels_path_for_part(drawing_part))
                if rel.attrib.get("Id") and rel.attrib.get("Target") and rel.attrib.get("TargetMode") != "External"
            }

            seen_sig_counts: Dict[Tuple, int] = {}
            for anchor_elem in list(drawing_root):
                chart_elem = anchor_elem.find(f".//{{{NS_CHART}}}chart")
                if chart_elem is None:
                    continue
                rid = chart_elem.attrib.get(f"{{{NS_REL_DOC}}}id")
                if not rid or rid not in rel_map:
                    continue

                sig = _anchor_signature(anchor_elem)
                seen_sig_counts[sig] = seen_sig_counts.get(sig, 0) + 1
                key = (sheet_name, sig, seen_sig_counts[sig])
                bindings[key] = rel_map[rid]
        return bindings

    def _quote_sheet_name(sheet_name: str) -> str:
        sheet_name = str(sheet_name)
        escaped = sheet_name.replace("'", "''")
        return f"'{escaped}'"

    def _sheet_range_pattern(sheet_name: str, col: str) -> str:
        alias = re.escape(sheet_name)
        alias_quoted = re.escape(sheet_name.replace("'", "''"))
        return rf"(?:'(?:{alias_quoted})'|(?:{alias}))!\${col}\$\d+:\${col}\$\d+"

    def _sheet_ref(sheet_name: str, col: str, row0: int, row1: int) -> str:
        return f"{_quote_sheet_name(sheet_name)}!${col}${row0}:${col}${row1}"

    CURVE_USD_FORMAT = r"\$#,##0.00;\-\$#,##0.00"

    def _rewrite_curve_ranges(xml_text: str) -> str:
        xml_text = re.sub(_sheet_range_pattern(curve_data_sheet_name, "A"), _sheet_ref(curve_data_sheet_name, "A", start_row, end_row), xml_text)
        xml_text = re.sub(_sheet_range_pattern(curve_data_sheet_name, "C"), _sheet_ref(curve_data_sheet_name, "C", start_row, end_row), xml_text)
        xml_text = re.sub(_sheet_range_pattern(curve_data_sheet_name, "D"), _sheet_ref(curve_data_sheet_name, "D", start_row, end_row), xml_text)

        # Template chart caches can carry mixed per-point formats, which makes
        # some data labels render as plain numbers and others as dollars.
        curve_ref_pat = rf"{_sheet_range_pattern(curve_data_sheet_name, '[CD]')}"
        tag = r"(?:\w+:)?"
        val_pat = re.compile(rf"(<{tag}val\b.*?</{tag}val>)", re.S)

        def _normalize_curve_value_cache(match: re.Match) -> str:
            block = match.group(1)
            if re.search(curve_ref_pat, block) is None:
                return block

            new_block = re.sub(
                rf"(<{tag}numCache\b[^>]*>\s*<{tag}formatCode>)(.*?)(</{tag}formatCode>)",
                lambda m: m.group(1) + CURVE_USD_FORMAT + m.group(3),
                block,
                count=1,
                flags=re.S,
            )
            if new_block == block:
                new_block = re.sub(
                    rf"(<{tag}numCache\b[^>]*>)",
                    lambda m: m.group(1) + f"<c:formatCode>{CURVE_USD_FORMAT}</c:formatCode>",
                    new_block,
                    count=1,
                    flags=re.S,
                )

            return re.sub(
                rf"(<{tag}pt\b[^>]*?)\s+formatCode=\"[^\"]*\"",
                r"\1",
                new_block,
            )

        xml_text = val_pat.sub(_normalize_curve_value_cache, xml_text)
        return xml_text

    def _rewrite_duration_ranges(xml_text: str) -> str:
        replacements = {
            "A": _sheet_ref(duration_data_sheet_name, "A", duration_start_row, duration_end_row),
            "B": _sheet_ref(duration_data_sheet_name, "B", duration_start_row, duration_end_row),
            "C": _sheet_ref(duration_data_sheet_name, "C", duration_start_row, duration_end_row),
            "D": _sheet_ref(duration_data_sheet_name, "D", duration_start_row, duration_end_row),
            "E": _sheet_ref(duration_data_sheet_name, "E", duration_start_row, duration_end_row),
            "F": _sheet_ref(duration_data_sheet_name, "F", duration_start_row, duration_end_row),
            "G": _sheet_ref(duration_data_sheet_name, "G", duration_start_row, duration_end_row),
        }
        for col, ref in replacements.items():
            xml_text = re.sub(_sheet_range_pattern(duration_data_sheet_name, col), ref, xml_text)
        return xml_text

    legacy_chart_data_chart_index = 0

    def _rewrite_chart_data_ranges(xml_text: str) -> str:
        """Rewrite only the Summary close+trade chart formulas using plain text replacement.

        Why text replacement instead of XML re-serialization:
        - Excel absolutely supports different X/Y ranges for different series in one chart.
        - The recurring "Removed Part: /xl/drawings/drawing1.xml" issue was not a limitation
          of Excel; it was the chart XML being re-serialized too aggressively.
        - Keep the chart XML package as close to the template as possible and only replace
          the `<c:f>` range formulas inside the intended `<c:ser>` blocks.
        """
        range_map = chart_data_ranges or {
            chart_data_sheet_name: {
                "start_row": chart_data_start_row,
                "end_row": chart_data_end_row,
                "eth_end_row": chart_data_eth_end_row,
                "rth_start_row": chart_data_rth_start_row,
            }
        }

        nonlocal legacy_chart_data_chart_index

        target_sheet = None
        for sheet_name in range_map:
            quoted = _quote_sheet_name(sheet_name)
            if f"{quoted}!" in xml_text or f"{sheet_name}!" in xml_text:
                target_sheet = sheet_name
                break
        if target_sheet is None:
            if ("'Chart Data'!" in xml_text or "Chart Data!" in xml_text) and len(range_map) > 1:
                sheets = list(range_map)
                target_sheet = sheets[min(legacy_chart_data_chart_index, len(sheets) - 1)]
                legacy_chart_data_chart_index += 1
            else:
                target_sheet = chart_data_sheet_name if chart_data_sheet_name in range_map else next(iter(range_map))

        ranges = range_map[target_sheet]
        data_start_row = int(ranges["start_row"])
        data_end_row = int(ranges["end_row"])
        data_eth_end_row = int(ranges["eth_end_row"])
        data_rth_start_row = int(ranges["rth_start_row"])

        series_map = {
            "ETH Close": (
                _sheet_ref(target_sheet, "A", data_start_row, data_eth_end_row),
                _sheet_ref(target_sheet, "B", data_start_row, data_eth_end_row),
            ),
            "RTH Close": (
                _sheet_ref(target_sheet, "A", data_rth_start_row, data_end_row),
                _sheet_ref(target_sheet, "B", data_rth_start_row, data_end_row),
            ),
            "Long Win": (
                _sheet_ref(target_sheet, "A", data_start_row, data_end_row),
                _sheet_ref(target_sheet, "C", data_start_row, data_end_row),
            ),
            "Long Loss": (
                _sheet_ref(target_sheet, "A", data_start_row, data_end_row),
                _sheet_ref(target_sheet, "D", data_start_row, data_end_row),
            ),
            "Short Win": (
                _sheet_ref(target_sheet, "A", data_start_row, data_end_row),
                _sheet_ref(target_sheet, "E", data_start_row, data_end_row),
            ),
            "Short Loss": (
                _sheet_ref(target_sheet, "A", data_start_row, data_end_row),
                _sheet_ref(target_sheet, "F", data_start_row, data_end_row),
            ),
        }

        tag = r"(?:\w+:)?"
        ser_pat = re.compile(rf"(<{tag}ser\b.*?</{tag}ser>)", re.S)

        def _replace_formula(block: str, tag_pat: str, new_ref: str) -> str:
            return re.sub(
                tag_pat,
                lambda m: m.group(1) + new_ref + m.group(3),
                block,
                count=1,
                flags=re.S,
            )

        def _rewrite_one_block(block: str, x_ref: str, y_ref: str) -> str:
            # Replace whichever x-axis form the template chart uses.
            new_block = _replace_formula(
                block,
                rf"(<{tag}xVal>.*?<{tag}(?:numRef|strRef)>.*?<{tag}f>)(.*?)(</{tag}f>)",
                x_ref,
            )
            new_block = _replace_formula(
                new_block,
                rf"(<{tag}cat>.*?<{tag}(?:numRef|strRef)>.*?<{tag}f>)(.*?)(</{tag}f>)",
                x_ref,
            )
            # Replace whichever y-axis/value form the template chart uses.
            new_block = _replace_formula(
                new_block,
                rf"(<{tag}yVal>.*?<{tag}numRef>.*?<{tag}f>)(.*?)(</{tag}f>)",
                y_ref,
            )
            new_block = _replace_formula(
                new_block,
                rf"(<{tag}val>.*?<{tag}numRef>.*?<{tag}f>)(.*?)(</{tag}f>)",
                y_ref,
            )
            return new_block

        def _block_matches_series(block: str, series_name: str) -> bool:
            return (
                re.search(rf"<{tag}v>{re.escape(series_name)}</{tag}v>", block) is not None
                or re.search(rf"<{tag}f>\"{re.escape(series_name)}\"</{tag}f>", block) is not None
                or re.search(rf"<{tag}f>=\"{re.escape(series_name)}\"</{tag}f>", block) is not None
            )

        changed = False

        def _ser_repl(match: re.Match) -> str:
            nonlocal changed
            block = match.group(1)
            for series_name, (x_ref, y_ref) in series_map.items():
                if _block_matches_series(block, series_name):
                    new_block = _rewrite_one_block(block, x_ref, y_ref)
                    if new_block != block:
                        changed = True
                    return new_block
            return block

        out = ser_pat.sub(_ser_repl, xml_text)
        return out if changed else xml_text

    def _chart_related_parts(zf: zipfile.ZipFile, chart_part: str) -> Tuple[Optional[str], List[str]]:
        rels_part = _rels_path_for_part(chart_part)
        if rels_part not in zf.namelist():
            return None, []
        related_parts: List[str] = []
        for rel in _read_relationships(zf, rels_part):
            if rel.attrib.get("TargetMode") == "External":
                continue
            target = rel.attrib.get("Target")
            if not target:
                continue
            part = _resolve_target(chart_part, target)
            if part in zf.namelist():
                related_parts.append(part)
        return rels_part, related_parts

    def _copy_content_type_overrides(template_parts: Dict[str, bytes], output_parts: Dict[str, bytes], copied_parts: List[str]) -> None:
        if "[Content_Types].xml" not in template_parts or "[Content_Types].xml" not in output_parts:
            return
        tpl_root = ET.fromstring(template_parts["[Content_Types].xml"])
        out_root = ET.fromstring(output_parts["[Content_Types].xml"])
        existing = {
            node.attrib.get("PartName")
            for node in out_root.findall(f"{{{NS_CT}}}Override")
            if node.attrib.get("PartName")
        }
        appended = False
        wanted = {"/" + _norm_zip_path(p) for p in copied_parts}
        for node in tpl_root.findall(f"{{{NS_CT}}}Override"):
            part_name = node.attrib.get("PartName")
            if not part_name or part_name not in wanted or part_name in existing:
                continue
            out_root.append(ET.Element(f"{{{NS_CT}}}Override", attrib=dict(node.attrib)))
            existing.add(part_name)
            appended = True
        if appended:
            output_parts["[Content_Types].xml"] = ET.tostring(out_root, encoding="utf-8", xml_declaration=True)

    with zipfile.ZipFile(output_xlsx, "r") as zout:
        out_order = list(zout.namelist())
        out_parts = {name: zout.read(name) for name in out_order}
        out_bindings = _chart_bindings_for_workbook(zout)
        out_chart_rels_map = {
            key: _chart_related_parts(zout, chart_part)[0]
            for key, chart_part in out_bindings.items()
        }

    with zipfile.ZipFile(template_xlsx, "r") as ztpl:
        tpl_parts = {name: ztpl.read(name) for name in ztpl.namelist()}
        tpl_bindings = _chart_bindings_for_workbook(ztpl)

        copied_parts_for_ct: List[str] = []
        shared_keys = [key for key in tpl_bindings.keys() if key in out_bindings]

        for key in shared_keys:
            tpl_chart_part = tpl_bindings[key]
            out_chart_part = out_bindings[key]

            tpl_chart_xml = tpl_parts[tpl_chart_part].decode("utf-8")
            tpl_chart_xml = _rewrite_curve_ranges(tpl_chart_xml)
            tpl_chart_xml = _rewrite_duration_ranges(tpl_chart_xml)
            tpl_chart_xml = _rewrite_chart_data_ranges(tpl_chart_xml)
            out_parts[out_chart_part] = tpl_chart_xml.encode("utf-8")
            if out_chart_part not in out_order:
                out_order.append(out_chart_part)

            tpl_chart_rels_part, tpl_sidecars = _chart_related_parts(ztpl, tpl_chart_part)
            target_out_rels_part = _rels_path_for_part(out_chart_part)

            if tpl_chart_rels_part is not None:
                out_parts[target_out_rels_part] = tpl_parts[tpl_chart_rels_part]
                if target_out_rels_part not in out_order:
                    out_order.append(target_out_rels_part)

                copied_parts_for_ct.extend(tpl_sidecars)
                for sidecar_part in tpl_sidecars:
                    out_parts[sidecar_part] = tpl_parts[sidecar_part]
                    if sidecar_part not in out_order:
                        out_order.append(sidecar_part)
            else:
                out_chart_rels_part = out_chart_rels_map.get(key)
                if out_chart_rels_part and out_chart_rels_part in out_parts:
                    del out_parts[out_chart_rels_part]
                    out_order = [p for p in out_order if p != out_chart_rels_part]

        _copy_content_type_overrides(tpl_parts, out_parts, copied_parts_for_ct)

    tmp_path = output_xlsx + ".tmp"
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in out_order:
            if name in out_parts:
                zf.writestr(name, out_parts[name])

    os.replace(tmp_path, output_xlsx)
