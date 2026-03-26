"""
GST Refund Calculator — Robust File-Based Value Extractor

Extracts relevant values from uploaded GSTR-3B, GSTR-1, Purchase Register,
and Shipping Bills for automatic refund calculation.

Each file type has its own dedicated parser that understands the actual
table layout and structure of these GST documents:

 • GSTR-3B  → Table 3.1 (outward supplies), Table 4 (ITC), Table 6 (payment)
 • GSTR-1   → Table 6A (exports), B2B (domestic supplies)
 • Purchase Register → Line items with ITC eligibility, ITC summary section
 • Shipping Bills → FOB values, cross-check against GSTR-1

The extractor uses row-scanning to find table markers (e.g. "Table 3.1",
"Table 4") rather than assuming fixed row positions, making it resilient
to formatting variations across different CA firms.
"""

import logging
import io
import os
import json
import re
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def extract_refund_values(
    file_bytes_list: List[bytes],
    filenames: List[str],
    refund_type: str,
) -> Dict[str, Any]:
    """
    Extract values from uploaded files and return a dict that can be
    fed directly into calculate_refund().

    Returns:
        Dict with keys matching the refund_type field IDs,
        plus _extraction_notes and _file_types_found metadata.
    """
    extracted: Dict[str, Any] = {"refund_type": refund_type}
    extraction_notes: List[str] = []
    file_types_found: List[str] = []

    for file_bytes, filename in zip(file_bytes_list, filenames):
        ext = os.path.splitext(filename)[1].lower()
        fn_lower = filename.lower()

        try:
            if ext == '.json':
                data = json.loads(file_bytes.decode('utf-8'))
                file_info = _extract_from_json(data, fn_lower, refund_type)
            elif ext in ['.xlsx', '.xls', '.csv']:
                file_info = _extract_from_tabular(file_bytes, filename, ext, refund_type)
            elif ext == '.pdf':
                extraction_notes.append(f"⚠️ PDF file '{filename}' skipped — PDF extraction requires OCR (not yet supported).")
                continue
            else:
                extraction_notes.append(f"⚠️ Unsupported file format: '{filename}'")
                continue

            # Merge extracted values (later files can override earlier)
            for key, val in file_info.get("values", {}).items():
                if val is not None and val != 0:
                    extracted[key] = val

            if file_info.get("file_type"):
                file_types_found.append(file_info["file_type"])
            if file_info.get("notes"):
                extraction_notes.extend(file_info["notes"])

            logger.info(f"  ✓ Extracted from '{filename}': type={file_info.get('file_type')}, values={file_info.get('values', {})}")

        except Exception as e:
            logger.warning(f"  ✗ Failed to extract from '{filename}': {e}", exc_info=True)
            extraction_notes.append(f"❌ Error processing '{filename}': {str(e)}")

    extracted["_extraction_notes"] = extraction_notes
    extracted["_file_types_found"] = file_types_found

    return extracted


# ═══════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════

def _safe_float(val) -> float:
    """Convert any value to float safely, handling None, NaN, strings with commas, Dr/Cr."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    s = str(val).strip().replace(',', '')
    if s in ('', 'nan', 'None', 'NaN', 'NA', '-', 'N/A'):
        return 0.0
    # Handle Tally Dr/Cr format
    if s.endswith(' Dr'):
        try:
            return float(s.replace(' Dr', ''))
        except ValueError:
            return 0.0
    if s.endswith(' Cr'):
        try:
            return -float(s.replace(' Cr', ''))
        except ValueError:
            return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _row_text(row) -> str:
    """Join all non-null values in a row into a lowercase string for keyword searching."""
    parts = []
    for v in row:
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            parts.append(str(v).strip().lower())
    return " ".join(parts)


def _find_first_numeric(row) -> float:
    """Find the first numeric value in a row (skipping text cells)."""
    for v in row:
        f = _safe_float(v)
        if f != 0:
            return f
    return 0.0


def _read_excel_raw(file_bytes: bytes, sheet_name=None) -> pd.DataFrame:
    """Read an Excel sheet with no header, all values raw."""
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    if sheet_name:
        return pd.read_excel(xls, sheet_name=sheet_name, header=None)
    return pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None)


def _get_sheet_names(file_bytes: bytes) -> List[str]:
    """Get list of sheet names from an Excel file."""
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    return xls.sheet_names


# ═══════════════════════════════════════════════════════════════
# FILE TYPE DETECTION
# ═══════════════════════════════════════════════════════════════

def _detect_file_type(filename: str, file_bytes: bytes, ext: str) -> str:
    """
    Detect file type by examining filename and sheet contents.
    Returns: 'gstr3b', 'gstr1', 'purchase_register', 'shipping_bills',
             'sales_register', 'cash_ledger', or 'unknown'
    """
    fn = filename.lower()

    # ── Filename-based detection ──
    if any(kw in fn for kw in ['3b', 'gstr3b', 'gstr-3b']):
        return 'gstr3b'
    if any(kw in fn for kw in ['gstr1', 'gstr-1']) or fn.startswith('gstr1'):
        return 'gstr1'
    if any(kw in fn for kw in ['shipping', 'sb-', 'shipping_bill', 'shipping bill']):
        return 'shipping_bills'
    if any(kw in fn for kw in ['purchase', 'pr_', 'purchase_reg', 'purchase register']):
        return 'purchase_register'
    if any(kw in fn for kw in ['sales', 'sr_', 'sales_reg', 'sales register']):
        return 'sales_register'
    if any(kw in fn for kw in ['cash', 'ledger', 'cash_ledger']):
        return 'cash_ledger'

    # ── Content-based detection (scan first 15 rows of first sheet) ──
    if ext in ['.xlsx', '.xls']:
        try:
            df = _read_excel_raw(file_bytes)
            content = ""
            for idx in range(min(15, len(df))):
                content += _row_text(df.iloc[idx].values) + " "

            sheets = _get_sheet_names(file_bytes)
            sheet_text = " ".join(s.lower() for s in sheets)

            if 'table 3.1' in content or 'table 4' in content or 'eligible itc' in content:
                return 'gstr3b'
            if '6a' in sheet_text or 'export' in sheet_text or 'table 6a' in content:
                return 'gstr1'
            if 'shipping bill' in content or 'fob value' in content or 'leo date' in content:
                return 'shipping_bills'
            if 'itc eligible' in content or 'purchase register' in content:
                return 'purchase_register'
            if 'sales register' in content:
                return 'sales_register'
            if 'cash ledger' in content or 'electronic cash' in content:
                return 'cash_ledger'
        except Exception:
            pass

    return 'unknown'


# ═══════════════════════════════════════════════════════════════
# TABULAR FILE ROUTER
# ═══════════════════════════════════════════════════════════════

def _extract_from_tabular(
    file_bytes: bytes, filename: str, ext: str, refund_type: str
) -> Dict[str, Any]:
    """Route to the correct parser based on detected file type."""
    file_type = _detect_file_type(filename, file_bytes, ext)
    logger.info(f"  📄 '{filename}' detected as: {file_type}")

    parsers = {
        'gstr3b': _parse_gstr3b,
        'gstr1': _parse_gstr1,
        'purchase_register': _parse_purchase_register,
        'shipping_bills': _parse_shipping_bills,
        'sales_register': _parse_sales_register,
        'cash_ledger': _parse_cash_ledger,
    }

    parser = parsers.get(file_type)
    if parser:
        values, notes = parser(file_bytes, ext, refund_type)
        return {"file_type": file_type, "values": values, "notes": notes}

    # Fallback: generic extraction
    values, notes = _parse_generic(file_bytes, ext, refund_type)
    return {"file_type": "generic", "values": values, "notes": notes}


# ═══════════════════════════════════════════════════════════════
# GSTR-3B PARSER — Table-aware row scanning
# ═══════════════════════════════════════════════════════════════

def _parse_gstr3b(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """
    Parse GSTR-3B Excel by scanning for table markers.

    Extracts:
      Table 3.1 → Zero-rated turnover, domestic turnover, exempt turnover
      Table 4   → ITC (imports IGST, CGST, SGST), capital goods, blocked credit
    """
    values = {}
    notes = []

    try:
        df = _read_excel_raw(file_bytes)
        rows = df.values.tolist()
        num_rows = len(rows)

        # ── Scan Table 3.1 ──
        table31_start = None
        table4_start = None

        for i, row in enumerate(rows):
            rt = _row_text(row)
            if 'table 3.1' in rt or '3.1' in rt and 'outward' in rt:
                table31_start = i
            if 'table 4' in rt and ('itc' in rt or 'eligible' in rt or 'input tax' in rt):
                table4_start = i
                break  # Table 4 comes after 3.1

        # Parse Table 3.1: scan rows after marker for supply categories
        # Use the (a)/(b)/(c)/(d) prefixes for reliable matching, because
        # row (a) text says "other than zero rated, nil rated and exempted"
        # which would falsely match zero-rated/exempt keyword checks.
        if table31_start is not None:
            # First, find the header row within Table 3.1 (has "Nature of Supplies")
            header_found = False
            for i in range(table31_start + 1, min(table31_start + 15, num_rows)):
                rt = _row_text(rows[i])

                # Skip the column header row
                if 'nature of supplies' in rt or ('total taxable' in rt and 'igst' in rt):
                    header_found = True
                    continue

                # Identify rows by (a)/(b)/(c)/(d) prefix or keyword matching
                first_cell = str(rows[i][0] or '').strip().lower() if len(rows[i]) > 0 else ''

                # (a) Outward taxable (domestic) — "(a)" prefix or starts with "outward taxable"
                if first_cell.startswith('(a)') or (
                    'outward' in first_cell and 'taxable' in first_cell
                    and 'zero' in first_cell and 'other than' in first_cell
                ):
                    val = _find_numeric_in_cols(rows[i], start_col=1)
                    if val > 0:
                        values['total_turnover'] = round(val, 2)
                        notes.append(f"GSTR-3B Table 3.1(a): Domestic turnover ₹{val:,.2f}")

                # (b) Zero-rated supply — "(b)" prefix or row starts with "zero rated"
                elif first_cell.startswith('(b)') or (
                    first_cell.startswith('outward') and 'zero rated' in first_cell
                    and 'other than' not in first_cell
                ):
                    val = _find_numeric_in_cols(rows[i], start_col=1)
                    if val > 0:
                        if refund_type in ('export_goods_lut', 'deemed_export'):
                            values['turnover_zero_rated_goods'] = round(val, 2)
                        elif refund_type == 'export_service_lut':
                            values['turnover_zero_rated_services'] = round(val, 2)
                        elif refund_type == 'export_igst':
                            # For IGST exports, get IGST amount from col 2 (IGST column)
                            igst_val = _safe_float(rows[i][2]) if len(rows[i]) > 2 else 0
                            if igst_val > 0:
                                values['igst_paid_on_exports'] = round(igst_val, 2)
                        elif refund_type == 'inverted_duty':
                            pass  # Zero-rated not relevant for IDS
                        notes.append(f"GSTR-3B Table 3.1(b): Zero-rated turnover ₹{val:,.2f}")

                # (c) Nil/Exempt — "(c)" prefix
                elif first_cell.startswith('(c)') or (
                    'nil' in first_cell and ('exempt' in first_cell or 'rated' in first_cell)
                    and 'other than' not in first_cell
                ):
                    val = _find_numeric_in_cols(rows[i], start_col=1)
                    if val > 0:
                        values['exempt_turnover'] = round(val, 2)
                        notes.append(f"GSTR-3B Table 3.1(c): Exempt turnover ₹{val:,.2f}")

                # (d) Inward supplies RCM
                elif first_cell.startswith('(d)') or ('inward' in rt and 'reverse' in rt):
                    pass  # Not typically needed for refund calc

                # (e) Non-GST
                elif first_cell.startswith('(e)') or 'non-gst' in first_cell:
                    pass  # Not needed for refund calc

        # ── Parse Table 4: ITC ──
        if table4_start is not None:
            itc_igst = 0.0
            itc_cgst = 0.0
            itc_sgst = 0.0
            itc_capital_igst = 0.0
            itc_capital_cgst = 0.0
            itc_capital_sgst = 0.0
            itc_input_services_igst = 0.0
            blocked_credit = 0.0
            net_itc_found = False

            for i in range(table4_start + 1, min(table4_start + 30, num_rows)):
                rt = _row_text(rows[i])
                row_vals = rows[i]

                # Import of goods — row (1)
                if 'import of goods' in rt or ('import' in rt and 'goods' in rt):
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    itc_igst += igst
                    notes.append(f"GSTR-3B Table 4: Import goods IGST ₹{igst:,.2f}")

                # Import of services — row (2)
                elif 'import of service' in rt or ('import' in rt and 'service' in rt):
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    itc_input_services_igst += igst
                    itc_igst += igst
                    notes.append(f"GSTR-3B Table 4: Import services IGST ₹{igst:,.2f}")

                # Inward supplies RCM — row (3)
                elif 'reverse charge' in rt or ('inward' in rt and 'reverse' in rt):
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    cgst = _safe_float(row_vals[2]) if len(row_vals) > 2 else 0
                    sgst = _safe_float(row_vals[3]) if len(row_vals) > 3 else 0
                    itc_igst += igst
                    itc_cgst += cgst
                    itc_sgst += sgst

                # Inward from ISD — row (4)
                elif 'isd' in rt and 'inward' in rt:
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    cgst = _safe_float(row_vals[2]) if len(row_vals) > 2 else 0
                    sgst = _safe_float(row_vals[3]) if len(row_vals) > 3 else 0
                    itc_igst += igst
                    itc_cgst += cgst
                    itc_sgst += sgst

                # All other ITC — row (5)
                elif 'all other' in rt or ('other' in rt and 'itc' in rt):
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    cgst = _safe_float(row_vals[2]) if len(row_vals) > 2 else 0
                    sgst = _safe_float(row_vals[3]) if len(row_vals) > 3 else 0
                    itc_igst += igst
                    itc_cgst += cgst
                    itc_sgst += sgst
                    notes.append(f"GSTR-3B Table 4: Other ITC — IGST ₹{igst:,.2f}, CGST ₹{cgst:,.2f}, SGST ₹{sgst:,.2f}")

                # Sub-Total (A) or Net ITC (C) — use as validation
                elif 'net itc' in rt or ('(c)' in rt and 'net' in rt):
                    net_itc_igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    net_itc_cgst = _safe_float(row_vals[2]) if len(row_vals) > 2 else 0
                    net_itc_sgst = _safe_float(row_vals[3]) if len(row_vals) > 3 else 0
                    notes.append(f"GSTR-3B Table 4(C): Net ITC — IGST ₹{net_itc_igst:,.2f}, CGST ₹{net_itc_cgst:,.2f}, SGST ₹{net_itc_sgst:,.2f}")
                    # Use net values directly if found
                    itc_igst = net_itc_igst
                    itc_cgst = net_itc_cgst
                    itc_sgst = net_itc_sgst
                    net_itc_found = True

                # ITC Reversed — Section (B)
                elif 'reversed' in rt and ('(b)' in rt or 'sub-total' in rt):
                    pass  # Already accounted for in Net ITC (C)

                # Blocked credit u/s 17(5) — in section (D)
                elif '17(5)' in rt or ('ineligible' in rt and '17' in rt):
                    igst = _safe_float(row_vals[1]) if len(row_vals) > 1 else 0
                    cgst = _safe_float(row_vals[2]) if len(row_vals) > 2 else 0
                    sgst = _safe_float(row_vals[3]) if len(row_vals) > 3 else 0
                    blocked_credit = igst + cgst + sgst
                    notes.append(f"GSTR-3B Table 4(D): Blocked u/s 17(5) ₹{blocked_credit:,.2f}")

            # Compute total ITC
            total_itc = itc_igst + itc_cgst + itc_sgst
            if total_itc > 0:
                values['itc_availed'] = round(total_itc, 2)
                notes.append(f"GSTR-3B Total ITC: ₹{total_itc:,.2f} (IGST ₹{itc_igst:,.2f} + CGST ₹{itc_cgst:,.2f} + SGST ₹{itc_sgst:,.2f})")

            if itc_input_services_igst > 0:
                values['itc_input_services'] = round(itc_input_services_igst, 2)

            if blocked_credit > 0:
                values['blocked_credit'] = round(blocked_credit, 2)

        else:
            notes.append("⚠️ Could not find Table 4 (ITC) marker in GSTR-3B")

    except Exception as e:
        logger.error(f"GSTR-3B parsing error: {e}", exc_info=True)
        notes.append(f"❌ GSTR-3B parsing error: {str(e)}")

    return values, notes


def _find_numeric_in_cols(row, start_col=1) -> float:
    """Find the first non-zero numeric value starting from a given column."""
    for i in range(start_col, len(row)):
        v = _safe_float(row[i])
        if v != 0:
            return v
    return 0.0


# ═══════════════════════════════════════════════════════════════
# GSTR-1 PARSER — Multi-sheet (Table 6A + B2B)
# ═══════════════════════════════════════════════════════════════

def _parse_gstr1(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """
    Parse GSTR-1 Excel with multiple sheets.

    Sheet "6A" / "Exports" → Export invoices with WPAY/WOPAY, IGST amounts
    Sheet "B2B"            → Domestic supplies for total turnover
    """
    values = {}
    notes = []

    try:
        sheets = _get_sheet_names(file_bytes)
        logger.info(f"  GSTR-1 sheets: {sheets}")

        # ── Find and parse Table 6A (Exports) ──
        export_sheet = None
        for s in sheets:
            sl = s.lower()
            if '6a' in sl or 'export' in sl:
                export_sheet = s
                break
        if not export_sheet and sheets:
            export_sheet = sheets[0]  # Default to first sheet

        if export_sheet:
            df = _read_excel_raw(file_bytes, sheet_name=export_sheet)
            rows = df.values.tolist()

            # Find header row (contains 'Invoice No' or 'Taxable Value')
            header_idx = None
            col_map = {}
            for i, row in enumerate(rows):
                rt = _row_text(row)
                if ('invoice' in rt or 'inv' in rt) and ('taxable' in rt or 'value' in rt):
                    header_idx = i
                    # Map columns
                    for j, cell in enumerate(row):
                        cl = str(cell).strip().lower() if cell is not None else ''
                        if 'taxable' in cl and 'value' in cl:
                            col_map['taxable'] = j
                        elif 'igst' in cl and 'rate' in cl:
                            col_map['igst_rate'] = j
                        elif 'igst' in cl and ('amount' in cl or 'amt' in cl or cl.strip() in ('igst', 'igst amount', 'igst amount (₹)', 'igst (₹)')):
                            col_map['igst_amount'] = j
                        elif 'igst' in cl:
                            # Generic IGST column — could be rate or amount
                            if 'rate' not in cl and 'igst_amount' not in col_map:
                                col_map['igst_amount'] = j
                        elif 'export type' in cl or 'type' in cl:
                            col_map['export_type'] = j
                        elif 'shipping' in cl and 'bill' in cl:
                            col_map['shipping_bill'] = j
                        elif 'fob' in cl:
                            col_map['fob'] = j
                    break

            if header_idx is not None:
                export_turnover_wpay = 0.0
                export_turnover_wopay = 0.0
                igst_on_exports = 0.0
                total_export_turnover = 0.0
                invoice_count = 0
                sb_count = 0

                for i in range(header_idx + 1, len(rows)):
                    row = rows[i]
                    rt = _row_text(row)

                    # Skip total/summary rows
                    if 'total' in rt and any(_safe_float(row[j]) > 0 for j in range(len(row))):
                        continue
                    if rt.strip() == '' or all(v is None or (isinstance(v, float) and np.isnan(v)) for v in row):
                        continue

                    # Get taxable value
                    taxable = _safe_float(row[col_map['taxable']]) if 'taxable' in col_map else 0
                    if taxable == 0:
                        continue  # Skip non-data rows

                    # Get export type
                    exp_type = ''
                    if 'export_type' in col_map:
                        exp_type = str(row[col_map['export_type']] or '').strip().upper()

                    # Get IGST amount
                    igst_amt = 0.0
                    if 'igst_amount' in col_map:
                        igst_amt = _safe_float(row[col_map['igst_amount']])
                    elif 'igst_rate' in col_map:
                        rate = _safe_float(row[col_map['igst_rate']])
                        igst_amt = round(taxable * rate / 100, 2)

                    # Classify
                    if exp_type == 'WPAY' or igst_amt > 0:
                        export_turnover_wpay += taxable
                        igst_on_exports += igst_amt
                    elif exp_type == 'WOPAY' or igst_amt == 0:
                        export_turnover_wopay += taxable

                    total_export_turnover += taxable
                    invoice_count += 1

                    # Count shipping bills
                    if 'shipping_bill' in col_map:
                        sb_val = row[col_map['shipping_bill']]
                        if sb_val is not None and str(sb_val).strip():
                            sb_count += 1

                if total_export_turnover > 0:
                    if refund_type in ('export_goods_lut', 'deemed_export'):
                        values['turnover_zero_rated_goods'] = round(total_export_turnover, 2)
                    elif refund_type == 'export_service_lut':
                        values['turnover_zero_rated_services'] = round(total_export_turnover, 2)
                    elif refund_type == 'export_igst':
                        values['igst_paid_on_exports'] = round(igst_on_exports, 2)
                        values['shipping_bills_total'] = sb_count if sb_count > 0 else invoice_count
                        values['shipping_bills_matched'] = values['shipping_bills_total']

                    notes.append(
                        f"GSTR-1 Table 6A: {invoice_count} export invoices, "
                        f"₹{total_export_turnover:,.2f} total "
                        f"(WPAY ₹{export_turnover_wpay:,.2f}, WOPAY ₹{export_turnover_wopay:,.2f}), "
                        f"IGST ₹{igst_on_exports:,.2f}"
                    )
            else:
                notes.append("⚠️ Could not find header row in GSTR-1 export sheet")

        # ── Find and parse B2B (Domestic) ──
        b2b_sheet = None
        for s in sheets:
            if 'b2b' in s.lower():
                b2b_sheet = s
                break

        if b2b_sheet:
            df = _read_excel_raw(file_bytes, sheet_name=b2b_sheet)
            rows = df.values.tolist()

            # Find header row
            header_idx = None
            taxable_col = None
            for i, row in enumerate(rows):
                rt = _row_text(row)
                if ('taxable' in rt or 'invoice' in rt) and any(kw in rt for kw in ['gstin', 'buyer', 'value', 'amount']):
                    header_idx = i
                    for j, cell in enumerate(row):
                        cl = str(cell).strip().lower() if cell is not None else ''
                        if 'taxable' in cl:
                            taxable_col = j
                            break
                    break

            if header_idx is not None and taxable_col is not None:
                domestic_turnover = 0.0
                for i in range(header_idx + 1, len(rows)):
                    row = rows[i]
                    rt = _row_text(row)
                    if 'total' in rt:
                        # Use the total row value directly
                        total_val = _safe_float(row[taxable_col])
                        if total_val > 0:
                            domestic_turnover = total_val
                        break
                    taxable = _safe_float(row[taxable_col])
                    if taxable > 0:
                        domestic_turnover += taxable

                if domestic_turnover > 0:
                    # Only set if not already set from GSTR-3B
                    if 'total_turnover' not in values:
                        values['total_turnover'] = round(domestic_turnover, 2)
                    notes.append(f"GSTR-1 B2B: Domestic turnover ₹{domestic_turnover:,.2f}")

    except Exception as e:
        logger.error(f"GSTR-1 parsing error: {e}", exc_info=True)
        notes.append(f"❌ GSTR-1 parsing error: {str(e)}")

    return values, notes


# ═══════════════════════════════════════════════════════════════
# PURCHASE REGISTER PARSER — ITC eligibility aware
# ═══════════════════════════════════════════════════════════════

def _parse_purchase_register(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """
    Parse Purchase Register with ITC eligibility flags.

    Extracts:
      - Total eligible ITC (IGST + CGST + SGST for rows with ITC Eligible = Yes)
      - Capital goods ITC (rows marked as Capital Goods type)
      - Blocked/ineligible ITC (rows with ITC Eligible = No)
    """
    values = {}
    notes = []

    try:
        df = _read_excel_raw(file_bytes)
        rows = df.values.tolist()

        # Find header row
        header_idx = None
        col_map = {}
        for i, row in enumerate(rows):
            rt = _row_text(row)
            if (('igst' in rt or 'cgst' in rt or 'sgst' in rt)
                    and ('taxable' in rt or 'invoice' in rt or 'supplier' in rt)):
                header_idx = i
                for j, cell in enumerate(row):
                    cl = str(cell).strip().lower() if cell is not None else ''
                    if 'taxable' in cl:
                        col_map['taxable'] = j
                    elif 'igst' in cl and 'sgst' not in cl:
                        col_map['igst'] = j
                    elif 'cgst' in cl:
                        col_map['cgst'] = j
                    elif cl.startswith('sgst') or 'sgst' in cl or 'utgst' in cl:
                        col_map['sgst'] = j
                    elif 'itc eligible' in cl or 'itc' in cl and 'eligible' in cl:
                        col_map['itc_eligible'] = j
                    elif 'type' in cl and 'voucher' not in cl:
                        col_map['type'] = j
                    elif 'total tax' in cl:
                        col_map['total_tax'] = j
                    elif 'remarks' in cl or 'remark' in cl:
                        col_map['remarks'] = j
                break

        if header_idx is None:
            notes.append("⚠️ Could not find header row in Purchase Register")
            return values, notes

        logger.info(f"  PR header at row {header_idx}, columns: {col_map}")

        # ── First check for ITC Summary section ──
        summary_itc = _find_itc_summary(rows, header_idx)
        if summary_itc:
            values.update(summary_itc['values'])
            notes.extend(summary_itc['notes'])
            return values, notes

        # ── Parse line items ──
        eligible_igst = 0.0
        eligible_cgst = 0.0
        eligible_sgst = 0.0
        capital_goods_igst = 0.0
        capital_goods_cgst = 0.0
        capital_goods_sgst = 0.0
        ineligible_total = 0.0
        import_igst = 0.0
        total_entries = 0
        eligible_entries = 0

        for i in range(header_idx + 1, len(rows)):
            row = rows[i]
            rt = _row_text(row)

            # Stop at blank rows or summary sections
            if rt.strip() == '' or 'itc summary' in rt or 'auto-calculated' in rt or 'total eligible' in rt:
                break
            if 'total' in rt and 'purchase' not in rt:
                continue

            # Get tax amounts
            igst = abs(_safe_float(row[col_map['igst']])) if 'igst' in col_map else 0
            cgst = abs(_safe_float(row[col_map['cgst']])) if 'cgst' in col_map else 0
            sgst = abs(_safe_float(row[col_map['sgst']])) if 'sgst' in col_map else 0
            row_tax = igst + cgst + sgst

            if row_tax == 0:
                continue  # Skip zero-tax rows

            total_entries += 1

            # Check ITC eligibility
            is_eligible = True
            if 'itc_eligible' in col_map:
                elig_val = str(row[col_map['itc_eligible']] or '').strip().lower()
                is_eligible = elig_val in ('yes', 'y', 'eligible', 'true', '1')

            # Check if capital goods
            is_capital = False
            if 'type' in col_map:
                type_val = str(row[col_map['type']] or '').strip().lower()
                is_capital = 'capital' in type_val

            is_import = False
            if 'type' in col_map:
                type_val = str(row[col_map['type']] or '').strip().lower()
                is_import = 'import' in type_val or 'boe' in type_val

            if not is_eligible:
                ineligible_total += row_tax
                continue

            eligible_entries += 1

            if is_capital:
                capital_goods_igst += igst
                capital_goods_cgst += cgst
                capital_goods_sgst += sgst
            else:
                eligible_igst += igst
                eligible_cgst += cgst
                eligible_sgst += sgst

            if is_import:
                import_igst += igst

        # Calculate totals
        total_eligible_itc = eligible_igst + eligible_cgst + eligible_sgst
        total_capital_itc = capital_goods_igst + capital_goods_cgst + capital_goods_sgst

        # Include capital goods in total eligible ITC (it will be excluded by the calculator)
        grand_total_itc = total_eligible_itc + total_capital_itc

        if grand_total_itc > 0:
            values['itc_availed'] = round(grand_total_itc, 2)

        if total_capital_itc > 0:
            values['itc_capital_goods'] = round(total_capital_itc, 2)

        if ineligible_total > 0:
            # Only set blocked credit if not already set from GSTR-3B
            if 'blocked_credit' not in values:
                values['blocked_credit'] = round(ineligible_total, 2)

        notes.append(
            f"Purchase Register: {eligible_entries}/{total_entries} eligible entries, "
            f"Eligible ITC ₹{grand_total_itc:,.2f} "
            f"(IGST ₹{eligible_igst:,.2f}, CGST ₹{eligible_cgst:,.2f}, SGST ₹{eligible_sgst:,.2f}), "
            f"Capital Goods ITC ₹{total_capital_itc:,.2f}, "
            f"Ineligible ₹{ineligible_total:,.2f}"
        )

    except Exception as e:
        logger.error(f"Purchase Register parsing error: {e}", exc_info=True)
        notes.append(f"❌ Purchase Register parsing error: {str(e)}")

    return values, notes


def _find_itc_summary(rows: list, header_idx: int) -> Optional[Dict]:
    """
    Look for an ITC Summary section in the Purchase Register.
    Many CAs add a summary at the bottom with total eligible ITC.
    """
    for i in range(header_idx + 1, len(rows)):
        rt = _row_text(rows[i])
        if 'total eligible itc' in rt or ('total' in rt and 'eligible' in rt and 'itc' in rt):
            # Found summary total row — extract values
            values = {}
            notes = []
            row = rows[i]

            # Scan for numeric values in the row
            numerics = [_safe_float(v) for v in row]
            non_zero = [v for v in numerics if v > 0]

            if non_zero:
                # Typically: Description, Taxable, IGST, CGST, SGST, Total
                # Find the columns by looking at the header above this summary
                for j in range(max(0, i - 5), i):
                    srt = _row_text(rows[j])
                    if 'description' in srt or 'igst' in srt or 'cgst' in srt:
                        # This is the summary header
                        sum_cols = {}
                        for k, cell in enumerate(rows[j]):
                            cl = str(cell or '').strip().lower()
                            if 'igst' in cl and 'sgst' not in cl:
                                sum_cols['igst'] = k
                            elif 'cgst' in cl:
                                sum_cols['cgst'] = k
                            elif cl.startswith('sgst') or 'sgst' in cl:
                                sum_cols['sgst'] = k
                            elif 'total' in cl and 'tax' in cl:
                                sum_cols['total_tax'] = k
                            elif 'taxable' in cl:
                                sum_cols['taxable'] = k

                        igst = _safe_float(row[sum_cols['igst']]) if 'igst' in sum_cols else 0
                        cgst = _safe_float(row[sum_cols['cgst']]) if 'cgst' in sum_cols else 0
                        sgst = _safe_float(row[sum_cols['sgst']]) if 'sgst' in sum_cols else 0
                        total = igst + cgst + sgst

                        if total > 0:
                            values['itc_availed'] = round(total, 2)
                            notes.append(f"Purchase Register ITC Summary: Total Eligible ₹{total:,.2f} (IGST ₹{igst:,.2f}, CGST ₹{cgst:,.2f}, SGST ₹{sgst:,.2f})")

                        # Now look for capital goods in nearby rows
                        for m in range(max(0, i - 6), i):
                            mrt = _row_text(rows[m])
                            if 'capital' in mrt and 'goods' in mrt:
                                m_igst = _safe_float(rows[m][sum_cols.get('igst', 0)])
                                m_cgst = _safe_float(rows[m][sum_cols.get('cgst', 0)])
                                m_sgst = _safe_float(rows[m][sum_cols.get('sgst', 0)])
                                cap_total = m_igst + m_cgst + m_sgst
                                if cap_total > 0:
                                    values['itc_capital_goods'] = round(cap_total, 2)
                                    notes.append(f"Purchase Register: Capital Goods ITC ₹{cap_total:,.2f}")

                        return {"values": values, "notes": notes}

    return None


# ═══════════════════════════════════════════════════════════════
# SHIPPING BILLS PARSER
# ═══════════════════════════════════════════════════════════════

def _parse_shipping_bills(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """
    Parse Shipping Bills Excel.

    Extracts:
      - Number of shipping bills
      - Total FOB value in INR (for validation, NOT as primary turnover)
      - Cross-check status vs GSTR-1

    IMPORTANT: Shipping bills do NOT override turnover_zero_rated_goods.
    The primary turnover comes from GSTR-1/3B. Shipping bills provide:
      1. shipping_bills_total / shipping_bills_matched (for Rule 96)
      2. FOB cross-check validation
    """
    values = {}
    notes = []

    try:
        df = _read_excel_raw(file_bytes)
        rows = df.values.tolist()

        # Find header row
        header_idx = None
        col_map = {}
        for i, row in enumerate(rows):
            rt = _row_text(row)
            if ('shipping bill' in rt or 'sb no' in rt or 'sb date' in rt) and ('fob' in rt or 'value' in rt):
                header_idx = i
                for j, cell in enumerate(row):
                    cl = str(cell).strip().lower() if cell is not None else ''
                    # Prefer the INR FOB column (with ₹ or (₹) or (inr) marker)
                    if 'fob' in cl and ('₹' in cl or 'inr' in cl or '(₹)' in cl):
                        col_map['fob_inr'] = j
                    elif 'fob' in cl and ('usd' in cl or '$' in cl or 'dollar' in cl):
                        col_map['fob_usd'] = j
                    elif 'fob' in cl and 'fob_inr' not in col_map:
                        # Generic FOB column — only use if INR not found
                        col_map['fob_generic'] = j
                    elif 'shipping bill' in cl or 'sb no' in cl:
                        col_map['sb_no'] = j
                    elif 'status' in cl:
                        col_map['status'] = j
                break

        if header_idx is None:
            notes.append("⚠️ Could not find header row in Shipping Bills")
            return values, notes

        # Use INR column, falling back to generic (but NOT USD)
        fob_col = col_map.get('fob_inr', col_map.get('fob_generic'))

        # Parse shipping bill rows
        total_fob_inr = 0.0
        sb_count = 0

        for i in range(header_idx + 1, len(rows)):
            row = rows[i]
            rt = _row_text(row)

            # Stop at blank or cross-check section
            if rt.strip() == '' or 'cross-check' in rt or 'cross check' in rt:
                break

            # Use total row if found
            if 'total' in rt:
                if fob_col is not None:
                    total_val = _safe_float(row[fob_col])
                    if total_val > 0:
                        total_fob_inr = total_val
                continue

            if fob_col is not None:
                fob = _safe_float(row[fob_col])
                if fob > 0:
                    total_fob_inr += fob
                    sb_count += 1

        # Set shipping bill count (for Rule 96)
        if sb_count > 0:
            values['shipping_bills_total'] = sb_count
            values['shipping_bills_matched'] = sb_count  # Assume all matched initially
            notes.append(f"Shipping Bills: {sb_count} bills, Total FOB ₹{total_fob_inr:,.2f}")

        # ── Parse cross-check section ──
        for i in range(header_idx + 1, len(rows)):
            rt = _row_text(rows[i])
            if 'difference' in rt or 'mismatch' in rt or 'matched' in rt:
                if 'matched' in rt.lower() and 'mismatch' not in rt.lower():
                    notes.append("Shipping Bills ↔ GSTR-1: MATCHED ✓")
                elif 'mismatch' in rt.lower():
                    notes.append("⚠️ Shipping Bills ↔ GSTR-1: MISMATCH detected")
                    # If mismatch, some bills may be withheld
                    values['shipping_bills_matched'] = max(0, sb_count - 1)
                break

    except Exception as e:
        logger.error(f"Shipping Bills parsing error: {e}", exc_info=True)
        notes.append(f"❌ Shipping Bills parsing error: {str(e)}")

    return values, notes


# ═══════════════════════════════════════════════════════════════
# SALES REGISTER PARSER (for Inverted Duty)
# ═══════════════════════════════════════════════════════════════

def _parse_sales_register(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """Parse Sales Register for output tax (used in Inverted Duty calculation)."""
    values = {}
    notes = []

    if refund_type != 'inverted_duty':
        notes.append("Sales Register: Not needed for this refund type")
        return values, notes

    try:
        from app.services.gst.reconciliation import load_reconciliation_file, identify_columns

        df = load_reconciliation_file(file_bytes, "sales_register.xlsx")
        cols = identify_columns(df)

        total_tax = 0
        for tax_col in ['igst', 'cgst', 'sgst']:
            if tax_col in cols and cols[tax_col] in df.columns:
                val = pd.to_numeric(df[cols[tax_col]], errors='coerce').fillna(0).abs().sum()
                total_tax += float(val)

        if total_tax > 0:
            values['tax_payable_inverted'] = round(total_tax, 2)

        if 'taxable' in cols and cols['taxable'] in df.columns:
            turnover = pd.to_numeric(df[cols['taxable']], errors='coerce').fillna(0).abs().sum()
            values['turnover_inverted'] = round(float(turnover), 2)

        notes.append(f"Sales Register: Tax payable ₹{total_tax:,.2f}")

    except Exception as e:
        notes.append(f"❌ Sales Register parsing error: {str(e)}")

    return values, notes


# ═══════════════════════════════════════════════════════════════
# CASH LEDGER PARSER (for Excess Cash)
# ═══════════════════════════════════════════════════════════════

def _parse_cash_ledger(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """Parse Electronic Cash Ledger for balance."""
    values = {}
    notes = []

    try:
        df = _read_excel_raw(file_bytes)
        rows = df.values.tolist()

        for i, row in enumerate(rows):
            rt = _row_text(row)
            if 'balance' in rt or 'closing' in rt:
                # Find numeric value in this row
                for v in row:
                    f = _safe_float(v)
                    if f > 0:
                        values['cash_ledger_balance'] = round(f, 2)
                        notes.append(f"Cash Ledger: Balance ₹{f:,.2f}")
                        break
                if 'cash_ledger_balance' in values:
                    break

    except Exception as e:
        notes.append(f"❌ Cash Ledger parsing error: {str(e)}")

    return values, notes


# ═══════════════════════════════════════════════════════════════
# GENERIC FALLBACK PARSER
# ═══════════════════════════════════════════════════════════════

def _parse_generic(file_bytes: bytes, ext: str, refund_type: str) -> Tuple[Dict, List]:
    """Fallback: try to extract tax columns from any tabular file."""
    values = {}
    notes = []

    try:
        from app.services.gst.reconciliation import load_reconciliation_file, identify_columns

        df = load_reconciliation_file(file_bytes, "file.xlsx" if ext != '.csv' else "file.csv")
        cols = identify_columns(df)

        total_itc = 0
        for tax_col in ['igst', 'cgst', 'sgst']:
            if tax_col in cols and cols[tax_col] in df.columns:
                val = pd.to_numeric(df[cols[tax_col]], errors='coerce').fillna(0).abs().sum()
                total_itc += float(val)

        if total_itc > 0:
            values['itc_availed'] = round(total_itc, 2)

        if 'taxable' in cols and cols['taxable'] in df.columns:
            turnover = pd.to_numeric(df[cols['taxable']], errors='coerce').fillna(0).abs().sum()
            values['total_turnover'] = round(float(turnover), 2)

        notes.append(f"Generic extraction: {len(df)} rows, ITC ₹{total_itc:,.2f}")

    except Exception as e:
        notes.append(f"❌ Generic extraction error: {str(e)}")

    return values, notes


# ═══════════════════════════════════════════════════════════════
# JSON EXTRACTION (GSTR-1 / GSTR-3B from GST Portal download)
# ═══════════════════════════════════════════════════════════════

def _extract_from_json(data: Any, filename: str, refund_type: str) -> Dict[str, Any]:
    """Extract values from GSTR-1 or GSTR-3B JSON format (portal downloads)."""
    values = {}
    file_type = "unknown_json"
    notes = []

    if isinstance(data, dict):
        # GSTR-3B JSON
        if 'ret_period' in data or 'sup_details' in data or 'itc_elg' in data:
            file_type = "gstr3b_json"
            values, notes = _parse_gstr3b_json(data, refund_type)
        # GSTR-1 JSON
        elif 'exp' in data or 'b2b' in data:
            file_type = "gstr1_json"
            values, notes = _parse_gstr1_json(data, refund_type)

    return {"file_type": file_type, "values": values, "notes": notes}


def _parse_gstr3b_json(data: dict, refund_type: str) -> Tuple[Dict, List]:
    """Parse GSTR-3B JSON (portal download format)."""
    values = {}
    notes = []

    # Table 4: ITC
    itc_data = data.get("itc_elg", {})
    if itc_data:
        itc_details = itc_data.get("itc_avl", [])
        total_itc = 0
        for item in itc_details:
            iamt = float(item.get("iamt", 0) or 0)
            camt = float(item.get("camt", 0) or 0)
            samt = float(item.get("samt", 0) or 0)
            total_itc += iamt + camt + samt

        if total_itc > 0:
            values["itc_availed"] = round(total_itc, 2)
            notes.append(f"GSTR-3B JSON: Total ITC ₹{total_itc:,.2f}")

    # Table 3.1: Supplies
    sup = data.get("sup_details", {})
    if sup:
        zero_rated = float(sup.get("osup_zero", {}).get("txval", 0) or 0)
        domestic = float(sup.get("osup_det", {}).get("txval", 0) or 0)
        exempt = float(sup.get("osup_nil_exmp", {}).get("txval", 0) or 0)

        if domestic > 0:
            values["total_turnover"] = round(domestic, 2)
        if exempt > 0:
            values["exempt_turnover"] = round(exempt, 2)
        if zero_rated > 0:
            if refund_type in ("export_goods_lut", "deemed_export"):
                values["turnover_zero_rated_goods"] = round(zero_rated, 2)
            elif refund_type == "export_service_lut":
                values["turnover_zero_rated_services"] = round(zero_rated, 2)

    return values, notes


def _parse_gstr1_json(data: dict, refund_type: str) -> Tuple[Dict, List]:
    """Parse GSTR-1 JSON (portal download format)."""
    values = {}
    notes = []

    exp_data = data.get("exp", [])
    total_export = 0
    total_igst = 0
    inv_count = 0

    for entry in exp_data:
        for inv in entry.get("inv", []):
            for itm in inv.get("itms", []):
                txval = float(itm.get("txval", 0) or 0)
                iamt = float(itm.get("iamt", 0) or 0)
                total_export += txval
                total_igst += iamt
                inv_count += 1

    if total_export > 0:
        if refund_type in ("export_goods_lut", "deemed_export"):
            values["turnover_zero_rated_goods"] = round(total_export, 2)
        elif refund_type == "export_service_lut":
            values["turnover_zero_rated_services"] = round(total_export, 2)
        elif refund_type == "export_igst":
            values["igst_paid_on_exports"] = round(total_igst, 2)
            values["shipping_bills_total"] = inv_count
        notes.append(f"GSTR-1 JSON: {inv_count} exports, ₹{total_export:,.2f}")

    return values, notes
