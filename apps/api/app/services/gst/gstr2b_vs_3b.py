"""
GSTR-2B vs GSTR-3B — ITC Reconciliation (Summary-level)

Compares:
  - ITC available   (from GSTR-2B — sum of IGST, CGST, SGST, Cess)
  - ITC claimed     (from GSTR-3B — Table 4A)
  - Variance = 3B − 2B  (positive = over-claimed, negative = under-claimed)

Supports: .xlsx, .csv, .json, .pdf
"""

import pandas as pd
import numpy as np
import io, os, json, logging, re
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

COMPONENTS = ['igst', 'cgst', 'sgst', 'cess']


# ══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

def reconcile_gstr2b_vs_3b(file_bytes_list: List[bytes], filenames: List[str] = None) -> Dict[str, Any]:
    """Main entry: parse both files, compute ITC variance, return result."""
    fn1 = (filenames or ["gstr2b.xlsx"])[0]
    fn2 = (filenames or ["", "gstr3b.xlsx"])[1]

    ext1 = fn1.lower().rsplit('.', 1)[-1]
    ext2 = fn2.lower().rsplit('.', 1)[-1]

    logger.warning(f"╔═ GSTR2B vs 3B ITC Reconciliation ════════════════════")
    logger.warning(f"║ File 1 (2B):  '{fn1}' → ext='{ext1}'")
    logger.warning(f"║ File 2 (3B):  '{fn2}' → ext='{ext2}'")
    logger.warning(f"║ Bytes:  2B={len(file_bytes_list[0]):,}  3B={len(file_bytes_list[1]):,}")

    # Route GSTR-2B to correct parser
    if ext1 == 'pdf':
        logger.warning("║ 2B parser → PDF")
        g2b = parse_gstr2b_pdf(file_bytes_list[0])
    elif ext1 == 'json':
        logger.warning("║ 2B parser → JSON")
        g2b = parse_gstr2b_json(file_bytes_list[0])
    else:
        logger.warning("║ 2B parser → Excel")
        g2b = parse_gstr2b_excel(file_bytes_list[0], fn1)

    # Route GSTR-3B to correct parser
    if ext2 == 'pdf':
        logger.warning("║ 3B parser → PDF")
        g3b = parse_gstr3b_pdf(file_bytes_list[1])
    elif ext2 == 'json':
        logger.warning("║ 3B parser → JSON")
        g3b = parse_gstr3b_json(file_bytes_list[1])
    else:
        logger.warning("║ 3B parser → Excel")
        g3b = parse_gstr3b_excel(file_bytes_list[1], fn2)

    logger.warning(f"║ 2B ITC available: {g2b['totals']}")
    logger.warning(f"║ 3B ITC claimed:   {g3b['totals']}")
    logger.warning(f"╚══════════════════════════════════════════════════════")

    # Variance = claimed - available (positive = over-claimed)
    variance = {c: round(g3b['totals'][c] - g2b['totals'][c], 2) for c in COMPONENTS}
    total_2b = sum(g2b['totals'][c] for c in COMPONENTS)
    total_3b = sum(g3b['totals'][c] for c in COMPONENTS)
    total_var = round(total_3b - total_2b, 2)
    pct = round(abs(total_var / total_2b * 100), 1) if total_2b else 0

    logger.info(f"2B ITC={total_2b:,.0f}  3B ITC={total_3b:,.0f}  Var={total_var:,.0f} ({pct}%)")

    risk = _risk(total_var, pct)
    actions = _actions(total_var, variance)
    report = _excel_report(g2b, g3b, variance, risk)

    return {
        'gstr2b_totals':   g2b['totals'],
        'gstr3b_totals':   g3b['totals'],
        'gstr2b_invoices': g2b.get('invoice_count', 0),
        'variance':        variance,
        'total_itc_2b':    round(total_2b, 2),
        'total_itc_3b':    round(total_3b, 2),
        'total_variance':  total_var,
        'variance_pct':    pct,
        'risk':            risk,
        'actions':         actions,
        'report_bytes':    report,
    }


# ══════════════════════════════════════════════════════════════
#  GSTR-2B  PARSERS  (ITC Available)
# ══════════════════════════════════════════════════════════════

def parse_gstr2b_excel(fbytes: bytes, fname: str) -> Dict:
    """Parse GSTR-2B from .xlsx / .csv — sum all ITC tax components."""
    try:
        xls = pd.ExcelFile(io.BytesIO(fbytes))
    except Exception:
        # Try CSV
        df = pd.read_csv(io.BytesIO(fbytes))
        return _sum_2b_dataframe(df)

    logger.info(f"GSTR-2B sheets: {xls.sheet_names}")
    grand = {c: 0.0 for c in COMPONENTS}
    total_rows = 0

    # Try B2B sheet first, then any sheet
    for sn in xls.sheet_names:
        sl = sn.lower()
        if any(skip in sl for skip in ['summary', 'help', 'readme', 'overview']):
            continue

        try:
            df = pd.read_excel(xls, sheet_name=sn)
            df.columns = [str(c).strip() for c in df.columns]
        except Exception:
            continue

        tcols = _find_tax_cols(df)
        if not tcols:
            continue

        # Drop total/summary rows
        for col in df.select_dtypes('object').columns:
            df = df[~df[col].astype(str).str.strip().str.lower().isin(
                ['total', 'grand total', 'sub total'])]

        for c in COMPONENTS:
            if c in tcols and tcols[c] in df.columns:
                grand[c] += round(float(pd.to_numeric(df[tcols[c]], errors='coerce').fillna(0).sum()), 2)

        total_rows += len(df)
        logger.info(f"  ✅ '{sn}': {len(df)} rows, ITC={sum(grand[c] for c in COMPONENTS):,.0f}")

    grand = {c: round(v, 2) for c, v in grand.items()}
    return {'totals': grand, 'invoice_count': total_rows}


def _sum_2b_dataframe(df: pd.DataFrame) -> Dict:
    """Sum ITC from a single DataFrame (CSV or single sheet)."""
    df.columns = [str(c).strip() for c in df.columns]
    tcols = _find_tax_cols(df)
    grand = {c: 0.0 for c in COMPONENTS}

    for col in df.select_dtypes('object').columns:
        df = df[~df[col].astype(str).str.strip().str.lower().isin(
            ['total', 'grand total', 'sub total'])]

    for c in COMPONENTS:
        if c in tcols and tcols[c] in df.columns:
            grand[c] = round(float(pd.to_numeric(df[tcols[c]], errors='coerce').fillna(0).sum()), 2)

    return {'totals': grand, 'invoice_count': len(df)}


def parse_gstr2b_json(fbytes: bytes) -> Dict:
    """Parse GSTR-2B JSON (GSTN API format)."""
    data = json.loads(fbytes.decode('utf-8'))
    grand = {c: 0.0 for c in COMPONENTS}
    count = 0

    # Walk through all ITC-eligible sections
    for section_key in ['b2b', 'b2ba', 'cdnr', 'cdnra', 'isd', 'isda', 'impg', 'impgsez']:
        entries = _find_key_deep(data, section_key)
        if entries and isinstance(entries, list):
            for entry in entries:
                _walk_json_itc(entry, grand)
                count += 1

    return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'invoice_count': count}


def parse_gstr2b_pdf(fbytes: bytes) -> Dict:
    """Parse GSTR-2B PDF — extract ITC summary from the summary table."""
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber required. Run: pip install pdfplumber")

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    logger.info(f"GSTR-2B PDF text length: {len(full_text)}")

    # Try to find total ITC from 2B summary
    grand = {c: 0.0 for c in COMPONENTS}
    NUM = r'([\d,]+\.?\d*)'

    # Look for total row with IGST, CGST, SGST, Cess values
    patterns = [
        # "Total ITC Available" or "Total" row
        rf'(?i)(?:total\s+itc|total\s+input|grand\s+total|total)\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}',
        # Fallback: any line with 4+ numbers after "total"
        rf'(?im)^.*total.*?{NUM}\s+{NUM}\s+{NUM}\s+{NUM}',
    ]

    for pat in patterns:
        m = re.search(pat, full_text)
        if m:
            vals = [float(g.replace(',', '')) for g in m.groups()]
            # Map: could be taxable+igst+cgst+sgst+cess or igst+cgst+sgst+cess
            if len(vals) >= 5:
                grand = {'igst': vals[1], 'cgst': vals[2], 'sgst': vals[3], 'cess': vals[4]}
            elif len(vals) >= 4:
                grand = {'igst': vals[0], 'cgst': vals[1], 'sgst': vals[2], 'cess': vals[3]}
            logger.info(f"  ✅ 2B ITC from PDF: {grand}")
            return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'invoice_count': 0}

    # Try pdfplumber table extraction
    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [str(c).strip() if c else '' for c in row]
                    row_text = ' '.join(cells).lower()
                    if 'total' not in row_text:
                        continue
                    nums = []
                    for cell in cells:
                        clean = cell.replace(',', '').replace(' ', '')
                        try:
                            nums.append(float(clean))
                        except ValueError:
                            pass
                    if len(nums) >= 4:
                        grand = {'igst': nums[-4], 'cgst': nums[-3], 'sgst': nums[-2], 'cess': nums[-1]}
                        logger.info(f"  ✅ 2B ITC from PDF table: {grand}")
                        return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'invoice_count': 0}

    logger.error("  ❌ Could not extract ITC from GSTR-2B PDF")
    return {'totals': grand, 'invoice_count': 0}


# ══════════════════════════════════════════════════════════════
#  GSTR-3B  PARSERS  (ITC Claimed — Table 4A)
# ══════════════════════════════════════════════════════════════

def parse_gstr3b_excel(fbytes: bytes, fname: str) -> Dict:
    """Parse GSTR-3B from .xlsx — find Table 4A (ITC claimed)."""
    xls = pd.ExcelFile(io.BytesIO(fbytes))
    logger.warning(f"GSTR-3B sheets: {xls.sheet_names}")

    for sn in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sn, header=None)

        for idx, row in df.iterrows():
            text_parts = []
            for v in row.values:
                if isinstance(v, str) and v.strip():
                    text_parts.append(v.strip())
            text = ' '.join(text_parts).lower()
            if not text or len(text) < 3:
                continue

            # Look for Table 4A rows
            is_4a = False
            if '4(a)' in text or '4a' in text or 'all other itc' in text:
                is_4a = True
            elif 'eligible itc' in text and ('import' in text or 'inward' in text or 'input' in text):
                is_4a = True

            if not is_4a:
                continue

            nums = [float(v) for v in row.values if _is_number(v)]
            if len(nums) < 3:
                # Try next row for numbers
                if idx + 1 < len(df):
                    next_row = df.iloc[idx + 1]
                    nums = [float(v) for v in next_row.values if _is_number(v)]
                if len(nums) < 3:
                    continue

            totals = {
                'igst': nums[0] if len(nums) > 0 else 0,
                'cgst': nums[1] if len(nums) > 1 else 0,
                'sgst': nums[2] if len(nums) > 2 else 0,
                'cess': nums[3] if len(nums) > 3 else 0,
            }
            totals = {c: round(v, 2) for c, v in totals.items()}
            logger.warning(f"  ✅ Table 4A found in '{sn}' row {idx}: {totals}")
            return {'totals': totals, 'row_label': 'Table 4A'}

    # Fallback: try summing all ITC rows
    logger.warning("  Table 4A not found — trying generic ITC sum")
    return _fallback_3b_itc(xls)


def _fallback_3b_itc(xls) -> Dict:
    """Fallback: scan for any ITC-related totals in 3B."""
    for sn in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sn, header=None)
        for idx, row in df.iterrows():
            text = ' '.join(str(v).lower() for v in row.values if isinstance(v, str))
            if 'total itc' in text or 'net itc' in text or 'eligible itc' in text:
                nums = [float(v) for v in row.values if _is_number(v)]
                if len(nums) >= 3:
                    totals = {
                        'igst': nums[0], 'cgst': nums[1],
                        'sgst': nums[2], 'cess': nums[3] if len(nums) > 3 else 0
                    }
                    logger.warning(f"  ✅ Fallback ITC from '{sn}': {totals}")
                    return {'totals': {c: round(v, 2) for c, v in totals.items()}, 'row_label': 'ITC Total'}

    return {'totals': {c: 0.0 for c in COMPONENTS}, 'row_label': 'Not found'}


def parse_gstr3b_json(fbytes: bytes) -> Dict:
    """Parse GSTR-3B JSON — extract Table 4A ITC."""
    data = json.loads(fbytes.decode('utf-8'))

    # Standard GSTN JSON: itc_elg → itc_avl → [{ ty: "OTH", iamt, camt, samt, csamt }]
    itc_elg = _find_key_deep(data, 'itc_elg')
    if itc_elg and isinstance(itc_elg, dict):
        itc_avl = itc_elg.get('itc_avl', [])
        grand = {c: 0.0 for c in COMPONENTS}
        for item in itc_avl:
            grand['igst'] += float(item.get('iamt', 0) or 0)
            grand['cgst'] += float(item.get('camt', 0) or 0)
            grand['sgst'] += float(item.get('samt', 0) or 0)
            grand['cess'] += float(item.get('csamt', 0) or 0)
        return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'row_label': 'Table 4A'}

    # Simpler format
    grand = {c: 0.0 for c in COMPONENTS}
    for key_map in [('igst', 'iamt'), ('cgst', 'camt'), ('sgst', 'samt'), ('cess', 'csamt')]:
        comp, jkey = key_map
        val = _find_key_deep(data, jkey)
        if val is not None:
            grand[comp] = float(val)

    return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'row_label': 'Table 4A'}


def parse_gstr3b_pdf(fbytes: bytes) -> Dict:
    """Extract Table 4A (ITC claimed) from GSTR-3B PDF."""
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber required. Run: pip install pdfplumber")

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    logger.warning(f"GSTR-3B PDF text length: {len(full_text)}")

    NUM = r'([\d,]+\.?\d*)'

    def to_f(s: str) -> float:
        s = s.strip().replace(',', '')
        if s == '-' or s == '':
            return 0.0
        return float(s)

    # Strategy 1: Find "All other ITC" row (most common Table 4A format)
    patterns_4a = [
        r'(?i)all\s+other\s+itc',
        r'(?i)\(2\)\s*all\s+other',
        r'(?i)4\s*[\(\[]?\s*a\s*[\)\]]?\s*.*?itc',
        r'(?i)eligible\s+itc',
    ]

    for pat in patterns_4a:
        m = re.search(pat, full_text)
        if m:
            after = full_text[m.end():]
            amt_pattern = r'([\d,]+\.?\d+|-)'
            amounts = re.findall(amt_pattern, after[:300])
            if len(amounts) >= 3:
                vals = [to_f(a) for a in amounts[:4]]
                totals = {
                    'igst': vals[0] if len(vals) > 0 else 0,
                    'cgst': vals[1] if len(vals) > 1 else 0,
                    'sgst': vals[2] if len(vals) > 2 else 0,
                    'cess': vals[3] if len(vals) > 3 else 0,
                }
                logger.warning(f"  ✅ Table 4A from PDF: {totals}")
                return {'totals': {c: round(v, 2) for c, v in totals.items()}, 'row_label': 'Table 4A'}

    # Strategy 2: pdfplumber table extraction
    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [str(c).strip() if c else '' for c in row]
                    row_text = ' '.join(cells).lower()
                    if 'all other' not in row_text and '4(a)' not in row_text and '4a' not in row_text:
                        continue
                    nums = []
                    for cell in cells:
                        clean = cell.replace(',', '').replace(' ', '')
                        try:
                            nums.append(float(clean))
                        except ValueError:
                            pass
                    if len(nums) >= 3:
                        totals = {
                            'igst': nums[0], 'cgst': nums[1],
                            'sgst': nums[2], 'cess': nums[3] if len(nums) > 3 else 0
                        }
                        logger.warning(f"  ✅ Table 4A from PDF table: {totals}")
                        return {'totals': {c: round(v, 2) for c, v in totals.items()}, 'row_label': 'Table 4A'}

    logger.error("  ❌ Table 4A not found in GSTR-3B PDF")
    return {'totals': {c: 0.0 for c in COMPONENTS}, 'row_label': 'Not found'}


# ══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ══════════════════════════════════════════════════════════════

def _find_tax_cols(df: pd.DataFrame) -> Dict[str, str]:
    """Find tax component columns in a DataFrame."""
    mapping = {
        'igst': ['igst', 'integrated tax', 'iamt', 'integrated'],
        'cgst': ['cgst', 'central tax', 'camt', 'central'],
        'sgst': ['sgst', 'state tax', 'samt', 'sgst/utgst', 'utgst', 'state/ut'],
        'cess': ['cess', 'csamt'],
    }
    result = {}
    for col in df.columns:
        cl = str(col).lower().strip()
        for comp, keywords in mapping.items():
            if comp not in result and any(kw in cl for kw in keywords):
                if comp == 'sgst' and 'igst' in cl:
                    continue
                result[comp] = col
                break
    return result


def _is_number(v) -> bool:
    if pd.isna(v):
        return False
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _find_key_deep(obj, key):
    """Recursively find a key in nested dicts/lists."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find_key_deep(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_key_deep(item, key)
            if r is not None:
                return r
    return None


def _walk_json_itc(obj, totals):
    """Walk JSON and sum ITC components."""
    if isinstance(obj, list):
        for item in obj:
            _walk_json_itc(item, totals)
    elif isinstance(obj, dict):
        for comp, keys in [('igst', ['iamt']), ('cgst', ['camt']),
                           ('sgst', ['samt']), ('cess', ['csamt'])]:
            for k in keys:
                if k in obj:
                    totals[comp] += float(obj[k] or 0)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_json_itc(v, totals)


# ══════════════════════════════════════════════════════════════
#  RISK & ACTIONS
# ══════════════════════════════════════════════════════════════

def _risk(var: float, pct: float) -> Dict:
    """Assess ITC variance risk."""
    if abs(var) <= 1:
        return {'level': 'perfect', 'icon': '✅',
                'heading': 'Perfect Match!',
                'description': 'ITC claimed in GSTR-3B matches GSTR-2B available ITC. No action needed.'}

    if var > 0:  # Over-claimed
        if var > 2500000 or pct > 20:
            return {'level': 'high', 'icon': '🔴',
                    'heading': f'HIGH RISK — Excess ITC of ₹{abs(var):,.0f}',
                    'description': ('Significantly more ITC claimed than available. '
                                    'Likely to trigger notice u/s 73/74. '
                                    'Immediate reversal with interest @ 18% recommended.')}
        if var > 500000 or pct > 10:
            return {'level': 'medium', 'icon': '🟡',
                    'heading': f'MEDIUM RISK — Over-claim of ₹{abs(var):,.0f}',
                    'description': ('Moderate excess ITC. Reverse in next GSTR-3B '
                                    'Table 4B(2) to avoid interest and scrutiny.')}
        return {'level': 'low', 'icon': '🟢',
                'heading': f'LOW RISK — Minor over-claim of ₹{abs(var):,.0f}',
                'description': 'Small variance. Correct in next return period.'}
    else:  # Under-claimed
        return {'level': 'neutral', 'icon': '💸',
                'heading': f'Under-claimed ITC of ₹{abs(var):,.0f}',
                'description': (f'You haven\'t claimed ₹{abs(var):,.0f} of available ITC. '
                                'Claim in next GSTR-3B Table 4A or GSTR-9 annual return.')}


def _actions(var: float, comp_var: Dict) -> List[Dict]:
    """Generate recommended actions."""
    acts = []
    a = abs(var)

    if var > 1:
        acts.append({'icon': '⚠️', 'color': 'red',
                     'title': f'Reverse excess ITC of ₹{a:,.0f}',
                     'description': 'Reduce ITC in next GSTR-3B Table 4B(2) to avoid interest u/s 50.'})
        acts.append({'icon': '💰', 'color': 'yellow',
                     'title': 'Calculate interest liability',
                     'description': 'Interest @ 18% p.a. applicable on excess ITC from filing date.'})
    elif var < -1:
        acts.append({'icon': '📋', 'color': 'blue',
                     'title': f'Claim missed ITC of ₹{a:,.0f}',
                     'description': 'Add under-claimed amount in next GSTR-3B Table 4A. Deadline: September return of next FY.'})
    else:
        acts.append({'icon': '✅', 'color': 'green',
                     'title': 'No action required',
                     'description': 'ITC perfectly reconciled.'})

    acts.append({'icon': '📝', 'color': 'purple',
                 'title': 'Prepare GSTR-9 Table 8',
                 'description': 'This ITC reconciliation feeds into Table 8 of annual return.'})

    return acts


# ══════════════════════════════════════════════════════════════
#  EXCEL REPORT
# ══════════════════════════════════════════════════════════════

def _excel_report(g2b, g3b, variance, risk) -> bytes:
    """Generate a downloadable Excel reconciliation report."""
    buf = io.BytesIO()
    labels = {'igst': 'IGST', 'cgst': 'CGST', 'sgst': 'SGST/UTGST', 'cess': 'Cess'}

    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        # Comparison sheet
        rows = [
            {'Component':        labels[c],
             'GSTR-2B (Available)': g2b['totals'][c],
             'GSTR-3B (Claimed)':   g3b['totals'][c],
             'Variance (3B−2B)':    variance[c],
             'Status': ('Match'        if abs(variance[c]) <= 1 else
                        'Over-claimed' if variance[c] > 0 else 'Under-claimed')}
            for c in COMPONENTS
        ]

        # Add total row
        total_2b = sum(g2b['totals'][c] for c in COMPONENTS)
        total_3b = sum(g3b['totals'][c] for c in COMPONENTS)
        total_var = sum(variance[c] for c in COMPONENTS)
        rows.append({
            'Component': 'TOTAL',
            'GSTR-2B (Available)': round(total_2b, 2),
            'GSTR-3B (Claimed)': round(total_3b, 2),
            'Variance (3B−2B)': round(total_var, 2),
            'Status': risk['heading'],
        })

        pd.DataFrame(rows).to_excel(w, index=False, sheet_name='ITC Comparison')

        # Risk & Actions sheet
        action_rows = [{'Action': a['title'], 'Details': a['description']} for a in risk.get('actions', [])]
        if not action_rows:
            action_rows = [{'Action': risk['heading'], 'Details': risk['description']}]
        pd.DataFrame(action_rows).to_excel(w, index=False, sheet_name='Risk & Actions')

        for ws in w.sheets.values():
            for i in range(10):
                ws.set_column(i, i, 25)

    return buf.getvalue()
