"""
═══════════════════════════════════════════════════════════════════════════
GST Reconciliation Engine — Firm-Agnostic
═══════════════════════════════════════════════════════════════════════════

Designed to work with ANY CA firm's data, regardless of:
  • Sheet names   (B2B, B2B_Data, B2B Data, Purchases, Sheet1...)
  • Column names  (Invoice No, Inv No., Invoice Number, Bill No, ...)
  • Header position (row 0, 1, 3, 5... — auto-detected)
  • Total rows    (footer sums, grand totals — auto-stripped)
  • Blocked ITC   (ITC Eligible = No rows — separated out)
  • Invoice number formats (HCL/2024/7712, HCL-2024-7712, HCL20247712)

Matching Strategy for GSTR-2B vs Purchase Register:
  Since Tally stores internal voucher numbers and 2B stores supplier invoice
  numbers, direct invoice-number matching is NOT possible.

  Priority 1: GSTIN + CGST + SGST + IGST (if GSTIN is present in PR)
  Priority 2: Normalized Supplier Name + CGST + SGST + IGST (fallback)

  Tally PR is filtered to only process Purchase and Debit Note voucher types.
"""

import pandas as pd
import numpy as np
import io
import os
import re
import logging
from typing import List, Dict, Any, Tuple, Optional
from app.models.job import JobType

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONSTANTS — Fuzzy matching dictionaries
# ═══════════════════════════════════════════════════════════════

# Sheet names that likely contain invoice data (substring, case-insensitive)
SHEET_POSITIVE = [
    'b2b', 'purchase', 'sales', 'data', 'invoice', 'register',
    'pr', 'sr', 'detail', 'transaction', 'voucher', 'b2ba', 'cdnr',
    'sheet1', 'report',
    # Busy-specific
    'gst register', 'purchase register', 'sale register',
    'input tax', 'output tax',
]

# Sheet names that are definitely NOT invoice data
SHEET_NEGATIVE = [
    'summary', 'overview', 'help', 'readme', 'instruction',
    'cover', 'index', 'metadata', 'info', 'statistic', 'dashboard',
    'template', 'master', 'setting', 'config', 'about',
    'read me', 'read_me',
]

# Column synonyms → standard name mapping
# Each standard name has a list of (keyword_patterns, priority)
# Higher priority wins when multiple columns match the same standard name
COLUMN_SYNONYMS = {
    'gstin': {
        'patterns': [
            ('gstin of supplier', 10),
            ('gstin of buyer', 10),
            ('supplier gstin', 10),
            ('party gstin', 10),
            ('gstin/uin', 10),
            ('gstin', 9),
            ('gst no', 8),
            ('gst_no', 8),
            ('gst number', 8),
            ('uin', 5),
        ],
    },
    'inv_num': {
        'patterns': [
            # HIGH priority — explicit "invoice" keyword
            ('supplier invoice no', 20),
            ('supplier invoice number', 20),
            ('invoice number', 15),
            ('invoice no.', 15),
            ('invoice no', 15),
            ('invoice num', 15),
            ('inv number', 12),
            ('inv no.', 12),
            ('inv no', 12),
            ('document number', 10),
            ('document no', 10),
            ('doc no', 8),
            # Busy software patterns
            ('vch no.', 6),
            ('vch no', 6),
            ('vch number', 6),
            # LOW priority — fallback only (voucher/bill/ref)
            ('voucher number', 3),
            ('voucher no.', 3),
            ('voucher no', 3),
            ('bill number', 3),
            ('bill no.', 3),
            ('bill no', 3),
            ('ref no', 2),
            ('reference no', 2),
        ],
    },
    'date': {
        'patterns': [
            ('invoice date', 10),
            ('inv date', 10),
            ('document date', 8),
            ('bill date', 7),
            ('voucher date', 6),
            ('date', 3),
        ],
    },
    'taxable': {
        'patterns': [
            ('taxable value', 10),
            ('taxable amount', 10),
            ('taxable amt', 10),
            ('taxable val', 10),
            ('assessable value', 8),
            # Busy uses "Assessable Value" or "Value" columns
            ('value of goods', 7),
            ('value of supply', 7),
            ('goods value', 7),
            ('net amount', 5),
            ('base amount', 5),
        ],
    },
    'igst': {
        'patterns': [
            ('integrated tax', 10),
            ('igst amount', 10),
            ('igst amt', 10),
            ('igst', 8),
        ],
    },
    'cgst': {
        'patterns': [
            ('central tax', 10),
            ('cgst amount', 10),
            ('cgst amt', 10),
            ('cgst', 8),
        ],
    },
    'sgst': {
        'patterns': [
            ('state/ut tax', 12),
            ('state tax', 10),
            ('sgst/utgst', 10),
            ('sgst amount', 10),
            ('sgst amt', 10),
            ('sgst', 8),
            ('utgst', 7),
            # Busy format
            ('state gst', 7),
        ],
    },
    'cess': {
        'patterns': [
            ('cess amount', 10),
            ('cess amt', 10),
            ('cess', 8),
        ],
    },
    'total_tax': {
        'patterns': [
            ('total tax', 10),
            ('tax amount', 8),
            ('total gst', 8),
        ],
    },
    'total_value': {
        'patterns': [
            ('total invoice value', 10),
            ('invoice value', 9),
            ('total value', 8),
            ('gross amount', 7),
            ('gross value', 7),
        ],
    },
    'irn': {
        'patterns': [
            ('irn', 8),
        ],
    },
    'itc_eligible': {
        'patterns': [
            ('itc eligibility', 10),
            ('itc eligible', 10),
            ('eligibility', 8),
            ('itc availability', 8),
            ('itc availed', 7),
            ('block', 5),
        ],
    },
    'supplier_name': {
        'patterns': [
            ('trade/legal name', 12),
            ('trade name', 10),
            ('legal name', 10),
            ('supplier name', 10),
            ('party name', 9),
            ('vendor name', 9),
            ('name of supplier', 9),
            ('buyer/supplier', 9),
            ('buyer / supplier', 9),
            ('buyer name', 8),
            ('seller name', 8),
            ('particulars', 8),
            ('particulers', 8),
            ('ledger name', 7),
            ('ledger', 6),
            ('party', 5),
        ],
    },
    'voucher_type': {
        'patterns': [
            ('voucher type', 10),
            ('transaction type', 8),
            # Busy software patterns
            ('vch type', 10),
            ('voucher kind', 7),
            ('type', 2),
        ],
    },
    # Busy-specific: "Account Name" / "Account Head"
    'account_name': {
        'patterns': [
            ('account name', 8),
            ('account head', 8),
            ('a/c name', 7),
            ('a/c head', 7),
        ],
    },
}

# Values that indicate a total/summary row (case-insensitive)
TOTAL_ROW_INDICATORS = [
    'total', 'grand total', 'sub total', 'sub-total', 'subtotal',
    'sum', 'net total', 'overall', 'aggregate', 'totals',
    'closing bal', 'opening bal', 'closing balance', 'opening balance',
]

# Values that indicate ITC is NOT eligible / blocked
ITC_BLOCKED_VALUES = [
    'no', 'n', 'blocked', 'not eligible', 'ineligible',
    'not available', 'not availed', 'section 17(5)',
]

# Stopwords to remove from supplier name tokens before comparison
# These are common legal/filler words that don't help distinguish suppliers
NAME_STOPWORDS = {
    'PVT', 'LTD', 'CO', 'AND', 'THE', 'OF', '&',
    'A', 'AN', 'FOR', 'IN', 'AT', 'BY', 'TO', 'WITH',
}

# Minimum token similarity score to consider a supplier name match
# Set at 0.60 because Tally appends city names (e.g., "-DELHI") which adds
# an extra token and reduces scores. Tax amount matching provides the second
# filter to prevent false positives.
NAME_MATCH_THRESHOLD = 0.60

# Voucher types to always include from Tally/Busy exports
TALLY_VOUCHER_TYPES_ALWAYS = [
    'purchase', 'debit note',
    # Busy software uses these values
    'purchase voucher', 'purchase invoice',
    'pur. voucher', 'pur voucher',
    'debit memo', 'dr. note', 'dr note',
]

# Voucher types to include ONLY if they have GST amounts
TALLY_VOUCHER_TYPES_CONDITIONAL = [
    'journal', 'jounal',  # common misspelling in some exports
    'journal voucher',
]


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def reconcile_gst(input_bytes_list: List[bytes], filenames: List[str] = None, job_type: str = "GST_RECON") -> bytes:
    """
    Entry point for all reconciliation jobs.
    input_bytes_list[0] = Source 1 (Portal data: GSTR-2B, IMS, GSTR-1, E-Invoice)
    input_bytes_list[1] = Source 2 (Books data: Purchase Register, Sales Register)
    """
    fn1 = filenames[0] if filenames else "file1.xlsx"
    fn2 = filenames[1] if filenames else "file2.xlsx"
    
    # Load
    df1 = load_reconciliation_file(input_bytes_list[0], fn1)
    df2 = load_reconciliation_file(input_bytes_list[1], fn2)
    
    # Transform Busy format if detected (adds IGST/CGST/SGST columns)
    df1 = transform_busy_format(df1, fn1)
    df2 = transform_busy_format(df2, fn2)
    
    logger.info(f"━━━ Source 1 ({fn1}): {len(df1)} rows, cols: {list(df1.columns)[:10]}...")
    logger.info(f"━━━ Source 2 ({fn2}): {len(df2)} rows, cols: {list(df2.columns)[:10]}...")
    
    # Identify columns
    cols1 = identify_columns(df1)
    cols2 = identify_columns(df2)
    
    # Standardize
    df1_std = rename_to_standard(df1, cols1)
    df2_std = rename_to_standard(df2, cols2)
    
    # Clean — drop total/summary rows
    df1_std = drop_total_rows(df1_std, "Source 1")
    df2_std = drop_total_rows(df2_std, "Source 2")
    
    # Filter by voucher type (for Tally exports)
    df2_std = filter_voucher_types(df2_std, "Source 2")
    
    # Separate blocked ITC rows (if applicable)
    df1_std, df1_blocked = separate_blocked_itc(df1_std, "Source 1")
    df2_std, df2_blocked = separate_blocked_itc(df2_std, "Source 2")
    
    logger.info(f"━━━ After cleaning → Source 1: {len(df1_std)} rows, Source 2: {len(df2_std)} rows")
    if len(df1_blocked): logger.info(f"    Blocked ITC (Source 1): {len(df1_blocked)} rows")
    if len(df2_blocked): logger.info(f"    Blocked ITC (Source 2): {len(df2_blocked)} rows")
    
    # Match
    result_df = match_data(df1_std, df2_std, job_type)
    
    # Build Excel output
    src1_label, src2_label = get_source_labels(job_type)
    output = generate_excel_report(result_df, df1_std, df2_std, df1_blocked, df2_blocked, src1_label, src2_label)
    return output


def get_source_labels(job_type: str) -> Tuple[str, str]:
    """Return (source1_label, source2_label) based on job type."""
    labels = {
        'gstr2b_vs_pr': ('GSTR-2B', 'Purchase Register'),
        'ims_vs_pr': ('IMS', 'Purchase Register'),
        'einv_vs_sr': ('E-Invoice', 'Sales Register'),
        'gstr1_vs_einv': ('GSTR-1', 'E-Invoice'),
        JobType.GSTR2B_VS_PR: ('GSTR-2B', 'Purchase Register'),
        JobType.IMS_VS_PR: ('IMS', 'Purchase Register'),
        JobType.EINV_VS_SR: ('E-Invoice', 'Sales Register'),
        JobType.GSTR1_VS_EINV: ('GSTR-1', 'E-Invoice'),
    }
    return labels.get(job_type, ('Source 1', 'Source 2'))


# ═══════════════════════════════════════════════════════════════
# SHEET SELECTION — Score-based, scans headers
# ═══════════════════════════════════════════════════════════════

def pick_best_sheet(xls: pd.ExcelFile) -> str:
    """Score each sheet and pick the one most likely to contain invoice data."""
    names = xls.sheet_names
    if len(names) == 1:
        return names[0]
    
    best_name, best_score = names[0], -100
    
    for name in names:
        score = 0
        nl = name.lower().replace(' ', '').replace('-', '').replace('_', '')
        
        # Explicit B2B sheet gets highest priority (critical for GSTR-2B files)
        if nl == 'b2b':
            score += 50
        
        # Positive keyword matches (substring)
        for kw in SHEET_POSITIVE:
            if kw.replace('_', '') in nl:
                score += 10
        
        # Negative keyword matches
        for kw in SHEET_NEGATIVE:
            if kw in nl:
                score -= 50
        
        # "ITC" sheets are NOT the main data sheets
        if 'itc' in nl:
            score -= 30
        
        # Bonus: scan first 10 rows of the sheet for invoice-like content
        try:
            df_peek = pd.read_excel(xls, sheet_name=name, header=None, nrows=10)
            header_text = ' '.join(str(v).lower() for row in df_peek.values for v in row if pd.notna(v))
            invoice_keywords = ['gstin', 'invoice', 'taxable', 'igst', 'cgst', 'sgst', 'supplier']
            hits = sum(1 for k in invoice_keywords if k in header_text)
            score += hits * 5
        except Exception:
            pass
        
        if score > best_score:
            best_score = score
            best_name = name
    
    logger.info(f"  Sheet selected: '{best_name}' (score: {best_score}) from {names}")
    return best_name


# ═══════════════════════════════════════════════════════════════
# BUSY FORMAT DETECTION & TRANSFORMATION
# ═══════════════════════════════════════════════════════════════

# Busy GST Type patterns:
#   I/GST-28%      → Interstate @ 28% → IGST = Total * 28/128
#   L/GST-28%      → Local @ 28%      → CGST = Total * 14/128, SGST = same
#   I/GST-18%      → Interstate @ 18% → IGST = Total * 18/118
#   L/GST-18%      → Local @ 18%      → CGST = Total * 9/118, SGST = same
#   I/GST-MultiRate → Mixed rates (can't split precisely — use total as-is)
#   L/GST-MultiRate → Mixed rates local

BUSY_GST_PATTERN = re.compile(r'^([IL])/GST-(\d+%|MultiRate)', re.IGNORECASE)


def detect_busy_format(df: pd.DataFrame) -> bool:
    """
    Detect if DataFrame is in Busy accounting software format.
    
    Busy format characteristics:
    1. Has a 'Type' column with values like 'I/GST-28%', 'L/GST-28%'
    2. Has 'Total Amount' column but NO separate IGST/CGST/SGST columns
    3. Has 'Account' column (supplier) instead of 'Party Name'
    4. Has 'Vch/Bill No' column
    """
    cols_lower = {str(c).lower().strip() for c in df.columns}
    
    # Must have Type column
    has_type = any('type' in c for c in cols_lower)
    if not has_type:
        return False
    
    # Must NOT already have IGST/CGST/SGST columns (already Tally/standard format)
    has_tax_cols = any(c in cols_lower for c in ['igst', 'cgst', 'sgst', 'integrated tax', 'central tax', 'state tax'])
    if has_tax_cols:
        return False
    
    # Must have Total Amount or similar
    has_amount = any(c in cols_lower for c in ['total amount', 'amount', 'total amt', 'gross amount'])
    if not has_amount:
        return False
    
    # Check if Type column contains Busy GST patterns
    type_col = None
    for c in df.columns:
        if str(c).lower().strip() == 'type':
            type_col = c
            break
    
    if type_col is None:
        return False
    
    type_vals = df[type_col].dropna().astype(str).str.strip().head(20)
    gst_matches = type_vals.str.match(BUSY_GST_PATTERN, na=False).sum()
    
    return gst_matches >= 2  # At least 2 rows with GST type patterns


def transform_busy_format(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Transform Busy accounting software Purchase Register into standard format.
    
    Busy PR has: Date, Vch/Bill No, Account, Type, Total Amount, Material Centre
    We need:     Date, inv_num, supplier_name, voucher_type, taxable, igst, cgst, sgst
    
    Tax calculation from Total Amount:
      For rate R% on Total Amount T:
        Taxable Value = T / (1 + R/100) = T * 100 / (100 + R)
        Tax = T - Taxable = T * R / (100 + R)
        
      I/GST (Interstate): IGST = Tax, CGST = 0, SGST = 0
      L/GST (Local):       IGST = 0, CGST = Tax/2, SGST = Tax/2
    """
    if not detect_busy_format(df):
        return df
    
    logger.info(f"  🔄 Busy format detected in '{filename}' — transforming to standard format")
    
    # Find actual column names (case-insensitive matching)
    col_map = {}
    for c in df.columns:
        cl = str(c).lower().strip()
        if cl == 'type': col_map['type'] = c
        elif cl in ['total amount', 'amount', 'total amt', 'gross amount']: col_map['amount'] = c
        elif cl in ['account', 'party', 'party name', 'account name']: col_map['account'] = c
        elif cl in ['vch/bill no', 'vch no', 'vch no.', 'bill no', 'bill no.', 'voucher no']: col_map['bill'] = c
        elif cl == 'date': col_map['date'] = c
        elif cl in ['material centre', 'godown']: col_map['mc'] = c
    
    # Build new DataFrame with standard columns
    new_rows = []
    multi_rate_count = 0
    
    for _, row in df.iterrows():
        gst_type = str(row.get(col_map.get('type', 'Type'), '')).strip()
        total_amt = float(row.get(col_map.get('amount', 'Total Amount'), 0) or 0)
        
        # Skip total/empty rows
        if gst_type.lower() in ('totals', 'total', 'grand total', '', 'nan', 'none'):
            continue
        if total_amt == 0:
            continue
        
        # Parse GST type
        m = BUSY_GST_PATTERN.match(gst_type)
        if not m:
            # Non-GST row (exempt, nil-rated, etc.) — include with zero tax
            new_rows.append({
                'Date': row.get(col_map.get('date', 'Date')),
                'Invoice No': row.get(col_map.get('bill', 'Vch/Bill No'), ''),
                'Party Name': str(row.get(col_map.get('account', 'Account'), '')).strip(),
                'Voucher Type': 'Purchase',
                'Taxable Value': total_amt,
                'IGST': 0, 'CGST': 0, 'SGST': 0, 'Cess': 0,
                'Total Amount': total_amt,
                'GST Type': gst_type,
            })
            continue
        
        scope = m.group(1).upper()  # 'I' or 'L'
        rate_str = m.group(2)       # '28%' or 'MultiRate'
        
        if rate_str.lower() == 'multirate':
            # MultiRate — can't determine exact split. Use 18% as default estimate
            rate = 18.0
            multi_rate_count += 1
        else:
            rate = float(rate_str.replace('%', ''))
        
        # Reverse-calculate tax from inclusive amount
        taxable = round(total_amt * 100 / (100 + rate), 2)
        total_tax = round(total_amt - taxable, 2)
        
        igst = 0.0
        cgst = 0.0
        sgst = 0.0
        
        if scope == 'I':
            # Interstate — all IGST
            igst = total_tax
        else:
            # Local — split equally between CGST and SGST
            cgst = round(total_tax / 2, 2)
            sgst = round(total_tax / 2, 2)
        
        new_rows.append({
            'Date': row.get(col_map.get('date', 'Date')),
            'Invoice No': str(row.get(col_map.get('bill', 'Vch/Bill No'), '')).strip(),
            'Party Name': str(row.get(col_map.get('account', 'Account'), '')).strip(),
            'Voucher Type': 'Purchase',
            'Taxable Value': taxable,
            'IGST': igst,
            'CGST': cgst,
            'SGST': sgst,
            'Cess': 0,
            'Total Amount': total_amt,
            'GST Type': gst_type,
        })
    
    if not new_rows:
        logger.warning(f"  ⚠️ Busy transform produced 0 rows from {len(df)} input rows")
        return df
    
    result = pd.DataFrame(new_rows)
    
    total_igst = result['IGST'].sum()
    total_cgst = result['CGST'].sum()
    total_sgst = result['SGST'].sum()
    total_taxable = result['Taxable Value'].sum()
    
    logger.info(f"  ✅ Busy → Standard: {len(result)} rows, "
               f"Taxable=₹{total_taxable:,.0f}, "
               f"IGST=₹{total_igst:,.0f}, CGST=₹{total_cgst:,.0f}, SGST=₹{total_sgst:,.0f}")
    if multi_rate_count:
        logger.warning(f"  ⚠️ {multi_rate_count} MultiRate entries — tax split estimated at 18%")
    
    return result


# ═══════════════════════════════════════════════════════════════
# FILE LOADING — Auto-detect header row
# ═══════════════════════════════════════════════════════════════

def load_reconciliation_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Loads Excel/CSV, finds the best sheet and header row automatically."""
    ext = os.path.splitext(filename)[1].lower()
    
    if ext in ['.xlsx', '.xls']:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        sheet_name = pick_best_sheet(xls)
        logger.info(f"📄 '{filename}' → sheet '{sheet_name}' (all: {xls.sheet_names})")
        
        # Auto-detect header row by scanning up to 30 rows
        df_scan = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
        header_info = find_header_row(df_scan)
        
        if isinstance(header_info, tuple):
            # Double header — merge two rows into one set of column names
            row_a, row_b = header_info
            logger.info(f"  Header detected: DOUBLE HEADER at rows {row_a} & {row_b} (merged)")
            
            raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            
            # Build merged column names: prefer row_b (child) value if not NaN, else row_a (parent)
            merged_cols = []
            for col_idx in range(len(raw.columns)):
                a = raw.iloc[row_a, col_idx] if row_a < len(raw) else None
                b = raw.iloc[row_b, col_idx] if row_b < len(raw) else None
                
                if pd.notna(b) and str(b).strip() not in ('', 'nan'):
                    merged_cols.append(str(b).strip())
                elif pd.notna(a) and str(a).strip() not in ('', 'nan'):
                    merged_cols.append(str(a).strip())
                else:
                    merged_cols.append(f'_unknown_{col_idx}')
            
            df = raw.iloc[row_b + 1:].copy()
            df.columns = merged_cols
            logger.info(f"  Merged columns: {merged_cols[:10]}{'...' if len(merged_cols) > 10 else ''}")
        else:
            # Single header row
            header_row = header_info
            logger.info(f"  Header detected at row {header_row}")
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
        
        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]
        
        # Drop completely empty rows
        df = df.dropna(how='all').reset_index(drop=True)
        
        return df
    else:
        # CSV — try common encodings
        for enc in ['utf-8', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
                df.columns = [str(c).strip() for c in df.columns]
                return df.dropna(how='all').reset_index(drop=True)
            except Exception:
                continue
        raise ValueError(f"Could not parse CSV file: {filename}")


def _score_header_row(row, header_keywords: list) -> float:
    """Score a single row for how 'header-like' it is."""
    score = 0.0
    for val in row.values:
        if pd.isna(val):
            continue
        s = str(val).lower().strip()
        # Score: how many header keywords appear in this cell?
        for kw in header_keywords:
            if kw in s:
                score += 1
        # Bonus for string cells that look like column headers (not numbers, not dates)
        if isinstance(val, str) and len(val) > 2 and not val.replace('.', '').replace(',', '').isdigit():
            score += 0.5
    return score


def find_header_row(df_scan: pd.DataFrame):
    """
    Finds the header row by scoring each row for "column-name-like" content.
    
    Returns:
      int — single header row index
      tuple(int, int) — double header (row_a, row_b) when two consecutive rows
                         are both header-like (e.g., GSTR-2B merged cells)
    """
    header_keywords = [
        'gstin', 'invoice', 'date', 'taxable', 'igst', 'cgst', 'sgst', 'cess',
        'supplier', 'customer', 'party', 'voucher', 'bill', 'amount', 'value',
        'number', 'name', 'trade', 'total', 'tax', 'place', 'type', 'no.',
        'buyer', 'particulars', 'particulers', 'integrated', 'central', 'state',
    ]
    
    best_row, best_score = 0, 0.0
    row_scores = {}
    
    for idx, row in df_scan.iterrows():
        score = _score_header_row(row, header_keywords)
        row_scores[idx] = score
        if score > best_score:
            best_score = score
            best_row = idx
    
    # Check if the row immediately after best_row is ALSO header-like
    # This detects double-header formats (e.g., GSTR-2B with merged cells)
    # Threshold > 5 is safe: parent headers score ~9, data rows score ~2
    next_row = best_row + 1
    if next_row in row_scores and row_scores[next_row] > 5:
        logger.info(f"  🔍 Double header detected: row {best_row} (score={best_score:.0f}) + "
                    f"row {next_row} (score={row_scores[next_row]:.0f})")
        return (best_row, next_row)
    
    # Also check: if the row BEFORE best_row is header-like, maybe best_row is the child
    prev_row = best_row - 1
    if prev_row >= 0 and prev_row in row_scores and row_scores[prev_row] > 5:
        logger.info(f"  🔍 Double header detected: row {prev_row} (score={row_scores[prev_row]:.0f}) + "
                    f"row {best_row} (score={best_score:.0f})")
        return (prev_row, best_row)
    
    return best_row


# ═══════════════════════════════════════════════════════════════
# COLUMN IDENTIFICATION — Priority-scored fuzzy matching
# ═══════════════════════════════════════════════════════════════

def identify_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Maps DataFrame columns to standard names using fuzzy matching with priorities.
    
    Each standard column has multiple synonym patterns with a priority score.
    When a DataFrame column matches multiple standard names, the highest-priority match wins.
    When multiple DataFrame columns match the same standard name, the highest-priority one wins.
    """
    # For each standard name, find the best matching DataFrame column
    matches: Dict[str, Tuple[str, int]] = {}  # std_name -> (df_col, priority)
    
    for real_col in df.columns:
        col_lower = str(real_col).lower().strip()
        # Special: skip columns that are clearly row indices
        if col_lower in ['sl no', 'sl no.', 'sr no', 'sr no.', 's.no', 's no', 'sno', '#', 'unnamed: 0']:
            continue
        
        for std_name, config in COLUMN_SYNONYMS.items():
            for pattern, priority in config['patterns']:
                # Check if pattern matches (substring or exact)
                if pattern in col_lower or col_lower == pattern:
                    # For 'sgst': make sure we don't match 'igst' columns
                    if std_name == 'sgst' and 'igst' in col_lower:
                        continue
                    
                    # Keep the highest-priority match per standard name
                    current = matches.get(std_name)
                    if current is None or priority > current[1]:
                        matches[std_name] = (real_col, priority)
                    break  # This pattern matched; move to next std_name
    
    col_map = {std: df_col for std, (df_col, _) in matches.items()}
    
    logger.info(f"  Column mapping: {col_map}")
    
    # Warn if critical columns are missing
    for critical in ['inv_num', 'gstin']:
        if critical not in col_map:
            logger.warning(f"  ⚠️ Could not find '{critical}' column. Available: {list(df.columns)}")
    
    return col_map


# ═══════════════════════════════════════════════════════════════
# INVOICE NUMBER NORMALIZATION
# ═══════════════════════════════════════════════════════════════

def normalize_inv_num(s: str) -> str:
    """
    Normalize an invoice number for matching.
    Strips whitespace, uppercases, removes separators (/ - _ space)
    so 'HCL/2024/7712' and 'HCL20247712' will match.
    """
    s = str(s).strip().upper()
    s = re.sub(r'[\s/\-_\.]+', '', s)
    return s


# ═══════════════════════════════════════════════════════════════
# SUPPLIER NAME NORMALIZATION
# ═══════════════════════════════════════════════════════════════

def tokenize_supplier_name(name: str) -> set:
    """
    Tokenize a supplier name for similarity matching.
    
    Steps:
      1. Uppercase, remove punctuation, collapse spaces
      2. Normalize legal suffixes (PRIVATE LIMITED → PVT LTD, etc.)
      3. Split into word tokens
      4. Remove stopwords (PVT, LTD, CO, AND, THE, OF, &)
      5. Return set of meaningful tokens
    
    Example:
      'SHIVALIK PACKAGING INDUSTRIES-DELHI'  → {'SHIVALIK', 'PACKAGING', 'INDUSTRIES', 'DELHI'}
      'SHIVALIK PACKAGING INDUSTRIES'        → {'SHIVALIK', 'PACKAGING', 'INDUSTRIES'}
      'A2Z PACKAGING INDUSTRIES PVT. LTD.'   → {'A2Z', 'PACKAGING', 'INDUSTRIES'}
    """
    s = str(name).strip().upper()
    
    # Empty/null check
    if s in ('', 'NAN', 'NONE', 'NA', 'NULL', 'NIL', '-'):
        return set()
    
    # Normalize legal suffixes BEFORE tokenizing
    s = re.sub(r'\bPRIVATE\s+LIMITED\b', 'PVT LTD', s)
    s = re.sub(r'\bPVT\.?\s*LTD\.?\b', 'PVT LTD', s)
    s = re.sub(r'\bLIMITED\b', 'LTD', s)
    s = re.sub(r'\bLTD\.', 'LTD', s)
    s = re.sub(r'\bCOMPANY\b', 'CO', s)
    s = re.sub(r'\bCO\.', 'CO', s)
    s = re.sub(r'\bENTERPRISES\b', 'ENTERPRISE', s)
    s = re.sub(r'\bINDUSTRY\b', 'INDUSTRIES', s)
    s = re.sub(r'\bINTERNATIONAL\b', 'INTL', s)
    s = re.sub(r'\bMANUFACTURING\b', 'MFG', s)
    s = re.sub(r'\bSOLUTIONS\b', 'SOLN', s)
    s = re.sub(r'\bSERVICES\b', 'SVC', s)
    
    # Merge dot-separated initials BEFORE removing punctuation
    # A.P. → AP, S.K.F. → SKF, V.V.P. → VVP
    s = re.sub(r'\b([A-Z])\.([A-Z])\.([A-Z])\.?', r'\1\2\3', s)
    s = re.sub(r'\b([A-Z])\.([A-Z])\.?', r'\1\2', s)
    
    # Remove all punctuation → replace with space
    s = re.sub(r'[^A-Z0-9\s]', ' ', s)
    
    # Split into tokens
    tokens = s.split()
    
    # Merge consecutive single-letter tokens into one
    # e.g., ['A', 'P', 'MOTORS'] → ['AP', 'MOTORS']
    merged = []
    buf = ''
    for t in tokens:
        if len(t) == 1 and t.isalpha():
            buf += t
        else:
            if buf:
                merged.append(buf)
                buf = ''
            merged.append(t)
    if buf:
        merged.append(buf)
    tokens = merged
    
    # Remove stopwords
    meaningful = {t for t in tokens if t not in NAME_STOPWORDS and len(t) > 0}
    
    return meaningful


def supplier_name_similarity(name1: str, name2: str) -> float:
    """
    Compute token-based similarity between two supplier names.
    
    Formula: len(common_tokens) / max(len(tokens1), len(tokens2))
    
    Examples:
      'SHIVALIK PACKAGING INDUSTRIES-DELHI' vs 'SHIVALIK PACKAGING INDUSTRIES'
        tokens1 = {'SHIVALIK', 'PACKAGING', 'INDUSTRIES', 'DELHI'}
        tokens2 = {'SHIVALIK', 'PACKAGING', 'INDUSTRIES'}
        common = 3, max_len = 4 → score = 0.75
      
      'A2Z PACKAGING PVT LTD' vs 'A2Z PACKAGING'
        tokens1 = {'A2Z', 'PACKAGING'}  (PVT, LTD are stopwords)
        tokens2 = {'A2Z', 'PACKAGING'}
        common = 2, max_len = 2 → score = 1.00
    """
    tokens1 = tokenize_supplier_name(name1)
    tokens2 = tokenize_supplier_name(name2)
    
    if not tokens1 or not tokens2:
        return 0.0
    
    common = tokens1 & tokens2
    max_len = max(len(tokens1), len(tokens2))
    
    return len(common) / max_len if max_len > 0 else 0.0


# ═══════════════════════════════════════════════════════════════
# DATA NORMALIZATION
# ═══════════════════════════════════════════════════════════════

def clean_dr_cr(val):
    """
    Clean Tally Columnar Ledger format amounts.
    Tally exports amounts as '18,504.58 Dr' or '1,200.00 Cr'.
    Dr = Debit (positive), Cr = Credit (negative for expenses context).
    Also handles comma-separated numbers like '18,504.58'.
    """
    s = str(val).strip()
    if s in ('', 'nan', 'None', 'NaN', 'NA', '-'):
        return 0.0
    if s.endswith(' Dr'):
        try:
            return float(s.replace(' Dr', '').replace(',', ''))
        except ValueError:
            return 0.0
    if s.endswith(' Cr'):
        try:
            return -float(s.replace(' Cr', '').replace(',', ''))
        except ValueError:
            return 0.0
    try:
        return float(s.replace(',', ''))
    except (ValueError, TypeError):
        return 0.0


def is_supplier_invoice_format(series: pd.Series, display_series: pd.Series = None) -> bool:
    """
    Check if invoice number values look like supplier invoice numbers
    (e.g., VP/034/25-26, TFI/25-26/01557, SPI/25-26/8, VVP2504104, SSC/3/25-26)
    rather than internal Tally voucher numbers (11, 12, 18, 302).
    
    Uses display_series (pre-normalized) if available, falling back to series.
    
    Supplier invoices:
      Pattern 1: letters mixed with / or - (e.g., 'BWD/25/MOS/02', '2025/26-0096')
      Pattern 2: letters + digits mixed together, length > 5 (e.g., 'VVP2504104', 'SSC32526')
    
    Internal vouchers: pure short numbers (1, 2, 11, 302)
    
    Returns True if >30% of sampled values match either pattern.
    """
    # Use display (pre-normalized) values if available
    check = display_series if display_series is not None else series
    sample = check.dropna().astype(str).str.strip()
    sample = sample[~sample.isin(['', 'NAN', 'NONE', 'NA'])].head(20)
    if len(sample) == 0:
        return False
    
    # Pattern 1: Contains letters AND separators (/ or -)
    pattern1 = sample.str.contains(
        r'[A-Za-z].*[/\-]|[/\-].*[A-Za-z]', regex=True
    ).mean()
    
    # Pattern 2: Alphanumeric mix with length > 5 (VVP2504104, SSC32526, PPA0025202526)
    # NOT just a pure number and NOT just pure letters
    pattern2 = sample.apply(
        lambda x: len(x) > 5 and bool(re.search(r'[A-Za-z]', x)) and bool(re.search(r'\d', x))
    ).mean()
    
    # Either pattern match triggers supplier format
    result = pattern1 > 0.3 or pattern2 > 0.3
    logger.info(f"  🔍 Invoice format check: pattern1={pattern1*100:.0f}%, pattern2={pattern2*100:.0f}% → "
                f"{'supplier format' if result else 'internal voucher format'}")
    return result


def rename_to_standard(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
    """Renames columns to standard names, cleans and normalizes data."""
    if not col_map:
        raise ValueError(
            "Could not identify any standard columns in the file. "
            f"Columns found: {list(df.columns)}\n"
            "Expected columns like: GSTIN, Invoice Number, Taxable Value, IGST, CGST, SGST"
        )
    
    reverse_map = {v: k for k, v in col_map.items()}
    valid_cols = [v for v in col_map.values() if v in df.columns]
    std_df = df[valid_cols].rename(columns=reverse_map).copy()
    
    # Force numeric for value columns
    # First clean Dr/Cr suffixes (Tally Columnar Ledger format),
    # then convert to numeric, round to 2dp
    num_cols = ['taxable', 'igst', 'cgst', 'sgst', 'cess', 'total_tax', 'total_value']
    for col in num_cols:
        if col in std_df.columns:
            # Check if any values contain 'Dr' or 'Cr' (Tally format)
            sample = std_df[col].astype(str).str.strip()
            has_dr_cr = sample.str.contains(r'\s+(Dr|Cr)$', regex=True, na=False).any()
            if has_dr_cr:
                logger.info(f"  🔧 Detected Dr/Cr format in '{col}' column — cleaning...")
                std_df[col] = std_df[col].apply(clean_dr_cr)
            else:
                std_df[col] = pd.to_numeric(std_df[col], errors='coerce').fillna(0)
            std_df[col] = std_df[col].round(2)
    
    # Clean GSTIN
    if 'gstin' in std_df.columns:
        std_df['gstin'] = (
            std_df['gstin'].astype(str).str.strip().str.upper()
            .replace({'NAN': '', 'NONE': '', 'NA': '', 'NULL': '', 'NIL': '', '-': ''})
        )
    
    # Clean invoice number — keep display version, normalize for matching
    if 'inv_num' in std_df.columns:
        std_df['inv_num_display'] = std_df['inv_num'].astype(str).str.strip()
        std_df['inv_num'] = std_df['inv_num'].apply(normalize_inv_num)
    
    return std_df


# ═══════════════════════════════════════════════════════════════
# VOUCHER TYPE FILTERING — For Tally exports
# ═══════════════════════════════════════════════════════════════

def filter_voucher_types(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Filters Tally export data to relevant voucher types.
    
    Include:
      - Purchase, Debit Note → always included
      - Journal → only if CGST > 0 OR SGST > 0 OR IGST > 0
        (RCM entries, import purchases, reverse charge are booked as Journal)
    
    Exclude:
      - Payment, Receipt, Sales, Credit Note, etc.
    
    If no voucher_type column exists, returns data as-is.
    """
    if 'voucher_type' not in df.columns:
        return df
    
    before = len(df)
    vt = df['voucher_type'].astype(str).str.strip().str.lower()
    
    # Always include Purchase and Debit Note
    mask_always = vt.isin(TALLY_VOUCHER_TYPES_ALWAYS)
    
    # Conditionally include Journal rows that have GST amounts
    mask_journal = vt.isin(TALLY_VOUCHER_TYPES_CONDITIONAL)
    mask_has_gst = pd.Series(False, index=df.index)
    for col in ['cgst', 'sgst', 'igst']:
        if col in df.columns:
            mask_has_gst = mask_has_gst | (pd.to_numeric(df[col], errors='coerce').fillna(0) > 0)
    mask_conditional = mask_journal & mask_has_gst
    
    # Combine
    mask = mask_always | mask_conditional
    result = df[mask].reset_index(drop=True)
    
    dropped = before - len(result)
    journal_kept = mask_conditional.sum()
    
    # Log unique voucher types present
    unique_vts = vt.unique().tolist()
    logger.info(f"  🔧 {source_name}: Voucher types found: {unique_vts}")
    
    if dropped > 0 or journal_kept > 0:
        logger.info(f"  🔧 {source_name}: Voucher filter → kept {len(result)} rows "
                     f"(Purchase/Debit Note: {mask_always.sum()}, "
                     f"Journal with GST: {journal_kept}), "
                     f"dropped {dropped} rows (Payment, Receipt, etc.)")
    
    if len(result) == 0 and before > 0:
        logger.warning(f"  ⚠️ {source_name}: ALL {before} rows were filtered out! "
                       f"Voucher types present: {unique_vts}. "
                       f"Expected: {TALLY_VOUCHER_TYPES_ALWAYS + TALLY_VOUCHER_TYPES_CONDITIONAL}. "
                       f"Returning original data unfiltered.")
        return df
    
    return result


# ═══════════════════════════════════════════════════════════════
# TOTAL / SUMMARY ROW REMOVAL
# ═══════════════════════════════════════════════════════════════

def drop_total_rows(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Removes footer/total/summary rows that contaminate matching.
    Detects by:
      1. inv_num is NaN, blank, or literally "NAN" (BUT only if <70% of rows have blank inv_num)
      2. Any text column contains "Total", "Grand Total", "Closing Bal", etc.
      3. gstin is blank AND inv_num is blank → likely a sum row (only if inv_num isn't mostly blank)
    """
    before = len(df)
    mask_keep = pd.Series(True, index=df.index)
    
    # Rule 1: inv_num is NaN/blank/NAN — but only if the column is meaningful
    if 'inv_num' in df.columns:
        inv = df['inv_num'].astype(str).str.strip().str.upper()
        mask_bad_inv = inv.isin(['', 'NAN', 'NONE', 'NULL', 'NA', 'NIL', '-', 'TOTAL', 'GRANDTOTAL'])
        blank_pct = mask_bad_inv.mean()
        
        if blank_pct < 0.70:
            # Most rows have inv_num → blank ones are likely total/summary rows
            mask_keep &= ~mask_bad_inv
            logger.info(f"  🗑️ {source_name}: Rule 1 (blank inv_num) → {mask_bad_inv.sum()} rows flagged ({blank_pct*100:.0f}% blank)")
        else:
            # Most rows lack inv_num → column is non-informative (e.g., Tally Columnar format)
            logger.info(f"  ℹ️ {source_name}: inv_num is {blank_pct*100:.0f}% blank — skipping Rule 1 (column appears non-informative)")
    
    # Rule 2: Any string column contains total-like keywords
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        vals = df[col].astype(str).str.strip().str.lower()
        for indicator in TOTAL_ROW_INDICATORS:
            mask_keep &= ~(vals == indicator)
    
    # Rule 3: gstin is blank AND inv_num is blank → almost certainly a total row
    # But only apply if inv_num is meaningful (not mostly blank)
    if 'gstin' in df.columns and 'inv_num' in df.columns:
        inv = df['inv_num'].astype(str).str.strip().str.upper()
        inv_blank_pct = inv.isin(['', 'NAN', 'NONE', 'NULL', 'NA', 'NIL', '-']).mean()
        if inv_blank_pct < 0.70:
            gstin_blank = df['gstin'].astype(str).str.strip().str.upper().isin(['', 'NAN', 'NONE', 'NA'])
            inv_blank = inv.isin(['', 'NAN', 'NONE', 'NA'])
            rule3_drop = gstin_blank & inv_blank
            mask_keep &= ~rule3_drop
            if rule3_drop.sum() > 0:
                logger.info(f"  🗑️ {source_name}: Rule 3 (blank GSTIN + inv_num) → {rule3_drop.sum()} rows flagged")
    
    result = df[mask_keep].reset_index(drop=True)
    dropped = before - len(result)
    if dropped > 0:
        logger.info(f"  🗑️ {source_name}: Dropped {dropped} total/summary row(s) total (from {before} → {len(result)})")
    else:
        logger.info(f"  ✅ {source_name}: No total/summary rows detected ({before} rows kept)")
    
    return result


# ═══════════════════════════════════════════════════════════════
# BLOCKED ITC SEPARATION
# ═══════════════════════════════════════════════════════════════

def separate_blocked_itc(df: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    If an 'itc_eligible' column exists, separates rows where ITC is blocked.
    Returns (eligible_df, blocked_df).
    """
    if 'itc_eligible' not in df.columns:
        return df, pd.DataFrame()
    
    itc_vals = df['itc_eligible'].astype(str).str.strip().str.lower()
    
    blocked_mask = itc_vals.isin(ITC_BLOCKED_VALUES)
    
    eligible = df[~blocked_mask].reset_index(drop=True)
    blocked = df[blocked_mask].reset_index(drop=True)
    
    if len(blocked) > 0:
        blocked_tax = 0
        for col in ['igst', 'cgst', 'sgst', 'cess']:
            if col in blocked.columns:
                blocked_tax += blocked[col].sum()
        logger.info(f"  🚫 {source_name}: Separated {len(blocked)} blocked ITC row(s) "
                     f"(₹{blocked_tax:,.0f} total tax excluded from comparison)")
    
    return eligible, blocked


# ═══════════════════════════════════════════════════════════════
# MATCHING ENGINE
# ═══════════════════════════════════════════════════════════════

def match_data(df1: pd.DataFrame, df2: pd.DataFrame, job_type: str) -> pd.DataFrame:
    """
    Matches invoices from two sources.
    
    For GSTR-2B vs PR (Tally):
      Since Tally stores internal voucher numbers and 2B stores supplier invoice 
      numbers, direct invoice-number matching is NOT possible.
      
      Priority 1: GSTIN + CGST + SGST + IGST (if both have GSTIN)
      Priority 2: Normalized Supplier Name + CGST + SGST + IGST (fallback)
    
    For other job types: Traditional GSTIN + Invoice Number matching.
    """
    src1_label, src2_label = get_source_labels(job_type)
    
    # ── Determine if this is a Tally PR job ──
    is_tally_pr_job = job_type in ['gstr2b_vs_pr', 'ims_vs_pr',
                                    JobType.GSTR2B_VS_PR, JobType.IMS_VS_PR]
    
    logger.info(f"  📋 df1 cols: {list(df1.columns)}")
    logger.info(f"  📋 df2 cols: {list(df2.columns)}")
    
    # Track whether both files originally have taxable column
    both_have_taxable = 'taxable' in df1.columns and 'taxable' in df2.columns
    
    # ── Synthesize missing tax columns ──
    # If one file has total_tax but not individual components, create them as 0
    # If one file has individual components but not total_tax, compute it
    for df, label in [(df1, src1_label), (df2, src2_label)]:
        has_individual = any(c in df.columns for c in ['cgst', 'sgst', 'igst'])
        has_total = 'total_tax' in df.columns
        
        if has_total and not has_individual:
            # Has total_tax but no individual components — add zeros
            # (We'll match on total_tax instead)
            for c in ['cgst', 'sgst', 'igst']:
                if c not in df.columns:
                    df[c] = 0.0
            logger.info(f"  🔧 {label}: Has total_tax but no CGST/SGST/IGST — added zero columns")
        
        if has_individual and not has_total:
            # Has individual components but no total_tax — compute it
            df['total_tax'] = sum(
                df[c].fillna(0) for c in ['cgst', 'sgst', 'igst'] if c in df.columns
            )
            logger.info(f"  🔧 {label}: Computed total_tax from CGST+SGST+IGST")
        
        # Ensure all tax columns exist with 0 defaults
        for c in ['cgst', 'sgst', 'igst']:
            if c not in df.columns:
                df[c] = 0.0
    
    # Re-check after synthesis
    # Check if PR (df2) has GSTIN
    pr_has_gstin = 'gstin' in df2.columns and df2['gstin'].astype(str).str.strip().replace('', pd.NA).dropna().nunique() > 0
    
    # Check if both have supplier names
    both_have_names = 'supplier_name' in df1.columns and 'supplier_name' in df2.columns
    
    # Check available tax/amount columns (now includes synthesized ones)
    has_tax_cols = any(c in df1.columns and c in df2.columns for c in ['cgst', 'sgst', 'igst', 'total_tax', 'taxable'])
    
    # ── Decide which file has total_tax only (for tax matching strategy) ──
    # If one side lacks individual components, we match on total_tax instead
    df1_has_individual = any(df1[c].abs().sum() > 0 for c in ['cgst', 'sgst', 'igst'] if c in df1.columns)
    df2_has_individual = any(df2[c].abs().sum() > 0 for c in ['cgst', 'sgst', 'igst'] if c in df2.columns)
    use_total_tax = not df1_has_individual or not df2_has_individual
    
    if use_total_tax:
        logger.info(f"  🔧 One source lacks individual tax components — will match on total_tax amount")
    
    if is_tally_pr_job:
        # Check if PR has supplier invoice numbers (not internal voucher numbers)
        # Use inv_num_display (pre-normalized) for format check if available
        pr_display = df2.get('inv_num_display', df2.get('inv_num'))
        pr_has_supplier_invnums = (
            'inv_num' in df2.columns and
            'inv_num' in df1.columns and
            is_supplier_invoice_format(df2['inv_num'], display_series=pr_display)
        )
        
        if pr_has_supplier_invnums:
            # ── PR has supplier invoice numbers → use invoice-based matching ──
            if pr_has_gstin:
                logger.info("  💡 PR has supplier invoice numbers + GSTIN → matching on GSTIN + Invoice Number")
                keys = ['gstin', 'inv_num']
            elif both_have_names:
                logger.info("  💡 PR has supplier invoice numbers, no GSTIN → matching on Invoice Number (+ name verification)")
                keys = ['inv_num']
            else:
                keys = ['inv_num']
                logger.info("  💡 PR has supplier invoice numbers → matching on Invoice Number only")
            
            # Use traditional merge for invoice-based matching
            merged = pd.merge(df1, df2, on=keys, how='outer', suffixes=('_src1', '_src2'), indicator=True)
            
            num_cols = ['taxable', 'igst', 'cgst', 'sgst', 'cess', 'total_tax', 'total_value']
            for col in num_cols:
                c1, c2 = f"{col}_src1", f"{col}_src2"
                if c1 in merged.columns and c2 in merged.columns:
                    merged[f'Diff_{col.capitalize()}'] = merged[c1].fillna(0) - merged[c2].fillna(0)
            
            def status(row):
                if row['_merge'] == 'both':
                    if both_have_taxable:
                        t1 = row.get('taxable_src1', 0) or 0
                        t2 = row.get('taxable_src2', 0) or 0
                        diff = abs(t1 - t2)
                    else:
                        # Use tax amounts as mismatch signal
                        diff = sum(abs((row.get(f'{c}_src1', 0) or 0) - (row.get(f'{c}_src2', 0) or 0))
                                   for c in ['cgst', 'sgst', 'igst'])
                    if diff < 1.0:
                        return 'MATCHED'
                    else:
                        return f'MISMATCH (₹{diff:,.0f} diff)'
                elif row['_merge'] == 'left_only':
                    return f'MISSING IN {src2_label.upper()}'
                else:
                    return f'MISSING IN {src1_label.upper()}'
            
            merged['Status'] = merged.apply(status, axis=1)
            merged['Match_Confidence'] = merged['Status'].apply(
                lambda s: 'EXACT' if s == 'MATCHED' else ''
            )
        
        elif pr_has_gstin:
            # ── TALLY MATCHING: GSTIN + Tax Amounts ──
            logger.info("  💡 PR has GSTIN, internal voucher numbers → matching on GSTIN + Tax Amounts")
            merged = smart_match_by_tax(
                df1, df2, src1_label, src2_label,
                match_key='gstin',
                use_total_tax=use_total_tax,
                both_have_taxable=both_have_taxable,
            )
        elif both_have_names:
            # ── TALLY MATCHING: Supplier Name + Tax Amounts ──
            logger.info("  💡 PR lacks GSTIN, internal voucher numbers → matching on Supplier Name + Tax Amounts")
            merged = smart_match_by_tax(
                df1, df2, src1_label, src2_label,
                match_key='supplier_name',
                use_total_tax=use_total_tax,
                both_have_taxable=both_have_taxable,
            )
        elif 'supplier_name' in df2.columns or 'supplier_name' in df1.columns:
            # ── FALLBACK: Only one file has supplier_name ──
            # Use pure tax amount matching (no name/gstin key)
            logger.info("  💡 Only one file has supplier_name, no GSTIN in PR → matching on Tax Amounts only")
            merged = smart_match_by_tax(
                df1, df2, src1_label, src2_label,
                match_key='tax_only',
                use_total_tax=use_total_tax,
                both_have_taxable=both_have_taxable,
            )
        else:
            logger.warning("  ⚠️ Cannot match: no usable key columns")
            raise ValueError(
                "Cannot match: no common matching columns found.\n"
                f"  GSTR-2B cols: {list(df1.columns)}\n"
                f"  PR cols: {list(df2.columns)}"
            )
    else:
        # ── TRADITIONAL MATCHING: GSTIN + Invoice Number ──
        keys = []
        
        if job_type in [JobType.EINV_VS_SR, JobType.GSTR1_VS_EINV, 'einv_vs_sr', 'gstr1_vs_einv']:
            if 'irn' in df1.columns and 'irn' in df2.columns:
                keys.append('irn')
        
        if not keys:
            if 'gstin' in df1.columns and 'gstin' in df2.columns:
                keys.append('gstin')
            if 'inv_num' in df1.columns and 'inv_num' in df2.columns:
                keys.append('inv_num')
        
        if not keys:
            if 'inv_num' in df1.columns and 'inv_num' in df2.columns:
                keys.append('inv_num')
            else:
                raise ValueError(
                    f"Cannot match: no common key columns found.\n"
                    f"  File 1 cols: {list(df1.columns)}\n"
                    f"  File 2 cols: {list(df2.columns)}"
                )
        
        logger.info(f"  🔗 Matching on keys: {keys}")
        merged = pd.merge(df1, df2, on=keys, how='outer', suffixes=('_src1', '_src2'), indicator=True)
        
        # Calculate differences
        num_cols = ['taxable', 'igst', 'cgst', 'sgst', 'cess', 'total_tax', 'total_value']
        for col in num_cols:
            c1, c2 = f"{col}_src1", f"{col}_src2"
            if c1 in merged.columns and c2 in merged.columns:
                merged[f'Diff_{col.capitalize()}'] = merged[c1].fillna(0) - merged[c2].fillna(0)
        
        # Status
        def status(row):
            if row['_merge'] == 'both':
                if both_have_taxable:
                    t1 = row.get('taxable_src1', 0) or 0
                    t2 = row.get('taxable_src2', 0) or 0
                    diff = abs(t1 - t2)
                else:
                    diff = sum(abs((row.get(f'{c}_src1', 0) or 0) - (row.get(f'{c}_src2', 0) or 0))
                               for c in ['cgst', 'sgst', 'igst'])
                if diff < 1.0:
                    return 'MATCHED'
                else:
                    return f'MISMATCH (₹{diff:,.0f} diff)'
            elif row['_merge'] == 'left_only':
                return f'MISSING IN {src2_label.upper()}'
            else:
                return f'MISSING IN {src1_label.upper()}'
        
        merged['Status'] = merged.apply(status, axis=1)
    
    # Reorder: Status first, drop internal columns
    drop_cols = {'_merge', '_match_key', '_norm_name'}
    cols = ['Status'] + [c for c in merged.columns if c != 'Status' and c not in drop_cols]
    merged = merged[[c for c in cols if c in merged.columns]]
    
    # Stats
    matched = (merged.Status == 'MATCHED').sum()
    mismatch = merged.Status.str.contains('MISMATCH', na=False).sum()
    missing_s2 = merged.Status.str.contains(f'MISSING IN {src2_label.upper()}', na=False).sum()
    missing_s1 = merged.Status.str.contains(f'MISSING IN {src1_label.upper()}', na=False).sum()
    logger.info(f"  📊 Results: {len(merged)} total | ✅ {matched} matched | "
                f"⚠️ {mismatch} mismatch | 🔴 {missing_s2} only in {src1_label} | "
                f"🟡 {missing_s1} only in {src2_label}")
    
    return merged


def smart_match_by_tax(
    df1: pd.DataFrame, df2: pd.DataFrame,
    src1_label: str, src2_label: str,
    match_key: str = 'gstin',
    tax_tolerance: float = 1.0,
    use_total_tax: bool = False,
    both_have_taxable: bool = True,
) -> pd.DataFrame:
    """
    Match invoices by (GSTIN or Supplier Name) + Tax Amounts.
    
    match_key: 'gstin' or 'supplier_name'
    use_total_tax: if True, compare total_tax instead of individual CGST+SGST+IGST
    
    For GSTIN matching: exact match on GSTIN string.
    For supplier_name matching: token similarity ≥ threshold.
    
    Adds Match_Confidence column:
      - 'EXACT' if name similarity = 1.0 (or GSTIN match)
      - 'FUZZY (XX%)' if similarity between threshold–0.99
      - empty for unmatched rows
    """
    tax_mode = 'total_tax' if use_total_tax else 'CGST+SGST+IGST'
    logger.info(f"  🧠 Smart matching: {match_key} + {tax_mode} (tolerance: ₹{tax_tolerance})")
    
    # Ensure tax columns exist with zeros, rounded to 2dp
    for df in [df1, df2]:
        for col in ['cgst', 'sgst', 'igst']:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
        if 'total_tax' in df.columns:
            df['total_tax'] = pd.to_numeric(df['total_tax'], errors='coerce').fillna(0).round(2)
    
    # Helper: compare tax amounts between two rows
    def tax_match(row1, row2):
        """Returns (is_match, total_diff) for tax comparison."""
        if use_total_tax:
            # Compare total_tax only
            t1 = round(float(row1.get('total_tax', 0) or 0), 2)
            t2 = round(float(row2.get('total_tax', 0) or 0), 2)
            diff = abs(t1 - t2)
            return diff <= tax_tolerance, diff
        else:
            # Compare individual CGST, SGST, IGST
            cgst1 = round(float(row1.get('cgst', 0) or 0), 2)
            sgst1 = round(float(row1.get('sgst', 0) or 0), 2)
            igst1 = round(float(row1.get('igst', 0) or 0), 2)
            cgst2 = round(float(row2.get('cgst', 0) or 0), 2)
            sgst2 = round(float(row2.get('sgst', 0) or 0), 2)
            igst2 = round(float(row2.get('igst', 0) or 0), 2)
            
            ok = (abs(cgst1 - cgst2) <= tax_tolerance and
                  abs(sgst1 - sgst2) <= tax_tolerance and
                  abs(igst1 - igst2) <= tax_tolerance)
            total_diff = abs(cgst1-cgst2) + abs(sgst1-sgst2) + abs(igst1-igst2)
            return ok, total_diff
    
    # Pre-compute tokens for name-based matching
    if match_key == 'supplier_name':
        df1_tokens = {}
        df2_tokens = {}
        for i1, row1 in df1.iterrows():
            name = str(row1.get('supplier_name', ''))
            df1_tokens[i1] = tokenize_supplier_name(name)
        for i2, row2 in df2.iterrows():
            name = str(row2.get('supplier_name', ''))
            df2_tokens[i2] = tokenize_supplier_name(name)
        
        # Log sample tokenizations
        logged = 0
        for i, tokens in list(df1_tokens.items())[:3]:
            if tokens:
                orig = df1.loc[i, 'supplier_name']
                logger.info(f"    {src1_label}: '{orig}' → tokens: {tokens}")
                logged += 1
        for i, tokens in list(df2_tokens.items())[:3]:
            if tokens:
                orig = df2.loc[i, 'supplier_name']
                logger.info(f"    {src2_label}: '{orig}' → tokens: {tokens}")
    
    # Build results
    results = []
    df2_used = set()  # Track which df2 rows are already matched (1:1)
    
    if match_key == 'gstin':
        # ── GSTIN-based matching: index by GSTIN for O(n) lookup ──
        df2_index = {}
        for i2, row2 in df2.iterrows():
            gval = str(row2.get('gstin', '')).strip().upper()
            if gval and gval not in ('NAN', 'NONE', 'NA', '', '-'):
                if gval not in df2_index:
                    df2_index[gval] = []
                df2_index[gval].append(i2)
        
        for i1, row1 in df1.iterrows():
            key1 = str(row1.get('gstin', '')).strip().upper()
            if key1 in ('NAN', 'NONE', 'NA', '', '-'):
                key1 = ''
            
            matched_idx = None
            matched_diff = float('inf')
            
            if key1 and key1 in df2_index:
                for i2 in df2_index[key1]:
                    if i2 in df2_used:
                        continue
                    row2 = df2.loc[i2]
                    is_match, total_diff = tax_match(row1, row2)
                    if is_match and total_diff < matched_diff:
                        matched_diff = total_diff
                        matched_idx = i2
            
            if matched_idx is not None:
                df2_used.add(matched_idx)
                row_data = _build_matched_row(row1, df2.loc[matched_idx], df1, df2, 'EXACT')
                results.append(row_data)
            else:
                results.append(_build_unmatched_row(row1, df1, 'src1', f'MISSING IN {src2_label.upper()}'))
    
    elif match_key == 'supplier_name':
        # ── Supplier Name similarity matching ──
        # For each df1 row, find best df2 match by token similarity + tax amounts
        for i1, row1 in df1.iterrows():
            tokens1 = df1_tokens.get(i1, set())
            
            best_idx = None
            best_score = 0.0
            best_tax_diff = float('inf')
            
            if tokens1:
                for i2, row2 in df2.iterrows():
                    if i2 in df2_used:
                        continue
                    
                    tokens2 = df2_tokens.get(i2, set())
                    if not tokens2:
                        continue
                    
                    # Compute name similarity
                    common = tokens1 & tokens2
                    max_len = max(len(tokens1), len(tokens2))
                    score = len(common) / max_len if max_len > 0 else 0.0
                    
                    if score < NAME_MATCH_THRESHOLD:
                        continue
                    
                    # Check tax amounts
                    is_match, total_diff = tax_match(row1, row2)
                    if is_match:
                        # Prefer higher similarity, then lower tax diff
                        if (score > best_score) or (score == best_score and total_diff < best_tax_diff):
                            best_score = score
                            best_tax_diff = total_diff
                            best_idx = i2
            
            if best_idx is not None:
                df2_used.add(best_idx)
                confidence = 'EXACT' if best_score >= 1.0 else f'FUZZY ({best_score*100:.0f}%)'
                row_data = _build_matched_row(row1, df2.loc[best_idx], df1, df2, confidence)
                results.append(row_data)
            else:
                results.append(_build_unmatched_row(row1, df1, 'src1', f'MISSING IN {src2_label.upper()}'))
    
    elif match_key == 'tax_only':
        # ── Tax-amount-only matching (last resort fallback) ──
        # No shared key column — match purely by tax amounts, 1:1
        logger.info("  ⚠️ Tax-only matching: no GSTIN or name overlap. Matching by tax amounts alone.")
        for i1, row1 in df1.iterrows():
            best_idx = None
            best_diff = float('inf')
            
            for i2, row2 in df2.iterrows():
                if i2 in df2_used:
                    continue
                is_match, total_diff = tax_match(row1, row2)
                if is_match and total_diff < best_diff:
                    best_diff = total_diff
                    best_idx = i2
            
            if best_idx is not None:
                df2_used.add(best_idx)
                row_data = _build_matched_row(row1, df2.loc[best_idx], df1, df2, 'TAX ONLY')
                results.append(row_data)
            else:
                results.append(_build_unmatched_row(row1, df1, 'src1', f'MISSING IN {src2_label.upper()}'))
    
    # Unmatched df2 rows → Missing in GSTR-2B
    for i2, row2 in df2.iterrows():
        if i2 not in df2_used:
            results.append(_build_unmatched_row(row2, df2, 'src2', f'MISSING IN {src1_label.upper()}'))
    
    merged = pd.DataFrame(results)
    
    # Check for taxable value mismatches in matched rows
    # Only flag on taxable if BOTH files actually provided a taxable column
    if both_have_taxable and 'Diff_Taxable' in merged.columns:
        mask_matched = merged['Status'] == 'MATCHED'
        mask_diff = merged['Diff_Taxable'].abs() > tax_tolerance
        merged.loc[mask_matched & mask_diff, 'Status'] = merged.loc[
            mask_matched & mask_diff, 'Diff_Taxable'
        ].apply(lambda d: f'MISMATCH (₹{abs(d):,.0f} diff)')
    
    # Also check tax amount mismatches (primary signal when taxable unavailable)
    for tax_col in (['Diff_Total_tax'] if use_total_tax else ['Diff_Cgst', 'Diff_Sgst', 'Diff_Igst']):
        if tax_col in merged.columns:
            mask_matched = merged['Status'] == 'MATCHED'
            mask_diff = merged[tax_col].abs() > tax_tolerance
            if mask_diff.any():
                merged.loc[mask_matched & mask_diff, 'Status'] = merged.loc[
                    mask_matched & mask_diff, tax_col
                ].apply(lambda d: f'MISMATCH (₹{abs(d):,.0f} tax diff)')
    
    matched_count = (merged['Status'] == 'MATCHED').sum()
    fuzzy_count = merged['Match_Confidence'].str.contains('FUZZY', na=False).sum() if 'Match_Confidence' in merged.columns else 0
    logger.info(f"  🧠 Smart match complete: {len(results)} rows, "
                f"{matched_count} matched ({matched_count - fuzzy_count} exact, {fuzzy_count} fuzzy), "
                f"{len(results) - matched_count} unmatched")
    
    return merged


def _build_matched_row(row1, row2, df1, df2, confidence: str) -> dict:
    """Build a result dict for a matched row pair."""
    row_data = {'Status': 'MATCHED', 'Match_Confidence': confidence}
    for col in df1.columns:
        if col.startswith('_'):
            continue
        row_data[f'{col}_src1'] = row1.get(col)
    for col in df2.columns:
        if col.startswith('_'):
            continue
        row_data[f'{col}_src2'] = row2.get(col) if hasattr(row2, 'get') else row2[col]
    
    # Calculate diffs
    for comp in ['taxable', 'cgst', 'sgst', 'igst', 'cess', 'total_value']:
        v1 = round(float(row1.get(comp, 0) or 0), 2)
        v2_val = row2.get(comp, 0) if hasattr(row2, 'get') else row2.get(comp, 0)
        v2 = round(float(v2_val or 0), 2)
        row_data[f'Diff_{comp.capitalize()}'] = round(v1 - v2, 2)
    
    return row_data


def _build_unmatched_row(row, df, suffix: str, status: str) -> dict:
    """Build a result dict for an unmatched row."""
    row_data = {'Status': status, 'Match_Confidence': ''}
    for col in df.columns:
        if col.startswith('_'):
            continue
        row_data[f'{col}_{suffix}'] = row.get(col)
    return row_data


# ═══════════════════════════════════════════════════════════════
# EXCEL REPORT GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_excel_report(
    result_df: pd.DataFrame,
    df1: pd.DataFrame, df2: pd.DataFrame,
    df1_blocked: pd.DataFrame, df2_blocked: pd.DataFrame,
    src1_label: str, src2_label: str
) -> bytes:
    """Generates a formatted Excel report with multiple sheets."""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, index=False, sheet_name="Reconciliation")
        df1.to_excel(writer, index=False, sheet_name=src1_label[:31])
        df2.to_excel(writer, index=False, sheet_name=src2_label[:31])
        
        if len(df1_blocked) > 0:
            df1_blocked.to_excel(writer, index=False, sheet_name="Blocked ITC")
        
        workbook = writer.book
        ws = writer.sheets['Reconciliation']
        
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1})
        matched_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        mismatch_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700'})
        missing_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        
        for i, col in enumerate(result_df.columns):
            ws.write(0, i, col, header_fmt)
            ws.set_column(i, i, max(len(str(col)), 15))
        
        if 'Status' in result_df.columns:
            si = result_df.columns.get_loc('Status')
            for ri, s in enumerate(result_df['Status']):
                fmt = None
                if s == 'MATCHED': fmt = matched_fmt
                elif 'MISMATCH' in str(s): fmt = mismatch_fmt
                elif 'MISSING' in str(s): fmt = missing_fmt
                if fmt: ws.write(ri + 1, si, s, fmt)
    
    return output.getvalue()
