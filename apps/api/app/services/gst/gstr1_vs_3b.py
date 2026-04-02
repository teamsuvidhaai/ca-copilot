"""
GSTR-1 vs GSTR-3B — Tax Liability Reconciliation

Simple summary-level comparison:
  1. Sum outward supply sheets from GSTR-1 (B2B, B2CL, B2CS, EXP, AT)
  2. Find Table 3.1(a) row in GSTR-3B
  3. Variance = G1 − 3B per component

Supports: .xlsx, .csv, .json, .pdf
"""

import pandas as pd
import numpy as np
import io, os, json, logging, re
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

COMPONENTS = ['taxable', 'igst', 'cgst', 'sgst', 'cess']

# Sheets to skip in GSTR-1 (not outward supply data)
# IMPORTANT: Table 12 (HSN Summary) and Table 15/16 (Totals) contain the SAME
# data as the section sheets (B2B, B2CL, etc.) — including them double/triple counts.
SKIP_SHEETS = ['cdnr', 'cdnur', 'cdn', 'hsn', 'doc', 'nil', 'exempt', 'summary',
               'help', 'readme', 'atadj', 'overview',
               # GST Portal table numbers that are summaries:
               'table 12', 'table12', 'table 15', 'table15', 'table 16', 'table16',
               'hsn wise', 'hsnwise', 'hsn summary', 'hsnsummary',
               'liability', 'total liability', 'tax liability',
               ]


# ══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

def reconcile_gstr1_vs_3b(file_bytes_list: List[bytes], filenames: List[str] = None) -> Dict[str, Any]:
    """Main entry: parse both files, compute variance, return result."""
    fn1 = (filenames or ["gstr1.xlsx"])[0]
    fn2 = (filenames or ["", "gstr3b.xlsx"])[1]

    ext1 = fn1.lower().rsplit('.', 1)[-1]
    ext2 = fn2.lower().rsplit('.', 1)[-1]

    logger.warning(f"╔═ GSTR1 vs 3B ═══════════════════════════════════════")
    logger.warning(f"║ File 1 (G1):  '{fn1}' → ext='{ext1}'")
    logger.warning(f"║ File 2 (3B):  '{fn2}' → ext='{ext2}'")
    logger.warning(f"║ Bytes:  G1={len(file_bytes_list[0]):,}  3B={len(file_bytes_list[1]):,}")

    # Route GSTR-1 to correct parser
    if ext1 == 'pdf':
        logger.warning("║ G1 parser → PDF")
        g1 = parse_gstr1_pdf(file_bytes_list[0])
    elif ext1 == 'json':
        logger.warning("║ G1 parser → JSON")
        g1 = _g1_json(file_bytes_list[0])
    else:
        logger.warning("║ G1 parser → Excel")
        g1 = parse_gstr1(file_bytes_list[0], fn1)

    # Route GSTR-3B to correct parser
    if ext2 == 'pdf':
        logger.warning("║ 3B parser → PDF")
        g3b = parse_gstr3b_pdf(file_bytes_list[1])
    elif ext2 == 'json':
        logger.warning("║ 3B parser → JSON")
        g3b = _3b_json(file_bytes_list[1])
    else:
        logger.warning("║ 3B parser → Excel")
        g3b = parse_gstr3b(file_bytes_list[1], fn2)
    
    logger.warning(f"║ G1 totals:  {g1['totals']}")
    logger.warning(f"║ 3B totals:  {g3b['totals']}")
    logger.warning(f"╚════════════════════════════════════════════════════")

    variance = {c: round(g1['totals'][c] - g3b['totals'][c], 2) for c in COMPONENTS}
    tax_g1  = sum(g1['totals'][c]  for c in ['igst', 'cgst', 'sgst', 'cess'])
    tax_3b  = sum(g3b['totals'][c] for c in ['igst', 'cgst', 'sgst', 'cess'])
    total_var = round(tax_g1 - tax_3b, 2)

    # Variance % = |variance| / min(G1, 3B) × 100
    denom = min(tax_g1, tax_3b)
    if denom > 0:
        pct = round(abs(total_var) / denom * 100, 1)
        pct_str = f"{pct}%"
    elif abs(total_var) > 0:
        pct = float('inf')
        pct_str = "∞%"
    else:
        pct = 0.0
        pct_str = "0%"

    logger.info(f"G1 tax={tax_g1:,.0f}  3B tax={tax_3b:,.0f}  Var={total_var:,.0f} ({pct_str})")

    risk    = _risk(total_var, tax_g1, tax_3b)
    actions = _actions(total_var, variance)

    # Per-component status
    HEAD_LABELS = {'igst': 'IGST', 'cgst': 'CGST', 'sgst': 'SGST/UTGST', 'cess': 'Cess'}
    components = []
    for c in ['igst', 'cgst', 'sgst', 'cess']:
        v = variance[c]
        if abs(v) <= 1:
            status = '✅ Matched'
        elif v > 0:
            status = '🔴 Under-declared'
        else:
            status = '🟡 Over-declared'
        components.append({
            'head': HEAD_LABELS[c],
            'gstr1': round(g1['totals'][c], 2),
            'gstr3b': round(g3b['totals'][c], 2),
            'variance': round(v, 2),
            'status': status,
        })

    report  = _excel_report(g1, g3b, variance, risk)

    return {
        'gstr1_sections':  g1['sections'],
        'gstr3b_row':      g3b.get('row_label', 'Table 3.1(a)'),
        'gstr1_totals':    g1['totals'],
        'gstr3b_totals':   g3b['totals'],
        'variance':        variance,
        'total_tax_g1':    round(tax_g1, 2),
        'total_tax_3b':    round(tax_3b, 2),
        'total_variance':  total_var,
        'variance_pct':    pct if pct != float('inf') else 9999.9,
        'variance_pct_str': pct_str,
        'risk':            risk,
        'actions':         actions,
        'components':      components,
        'report_bytes':    report,
    }


# ══════════════════════════════════════════════════════════════
#  GSTR-1  PARSERS
# ══════════════════════════════════════════════════════════════

# ── Excel / CSV ────────────────────────────────────────────────

def parse_gstr1(fbytes: bytes, fname: str) -> Dict:
    """Parse GSTR-1 from .xlsx / .xls / .csv.
    
    Strategy to avoid double/triple counting:
      1. Skip known summary sheets (HSN, Table 12, Table 15, etc.)
      2. Only sum B2B/B2CL/B2CS/EXP/AT section-level sheets
      3. Dedup check: if total from sheets > 2x largest sheet, warn and use only largest
    """
    xls = pd.ExcelFile(io.BytesIO(fbytes))
    logger.info(f"GSTR-1 sheets: {xls.sheet_names}")

    sections, grand = [], {c: 0.0 for c in COMPONENTS}

    for sn in xls.sheet_names:
        if _should_skip(sn):
            logger.info(f"  skip '{sn}'")
            continue

        df = _read_sheet(xls, sn)
        if df is None or df.empty:
            continue

        tcols = _find_tax_cols(df)
        if not tcols:
            continue

        # drop total rows
        for col in df.select_dtypes('object').columns:
            df = df[~df[col].astype(str).str.strip().str.lower().isin(
                ['total', 'grand total', 'sub total'])]

        totals = {
            c: round(float(pd.to_numeric(df[tcols[c]], errors='coerce').fillna(0).sum()), 2)
               if c in tcols and tcols[c] in df.columns else 0.0
            for c in COMPONENTS
        }

        sections.append({'section': sn, 'rows': len(df), **totals})
        for c in COMPONENTS:
            grand[c] += totals[c]

        logger.info(f"  ✅ '{sn}': {len(df)} rows, "
                    f"tax={totals['igst']+totals['cgst']+totals['sgst']:,.0f}")

    # ── Deduplication check ──
    # If the total across all sheets is suspiciously high compared to the
    # largest single section, it likely means summary/HSN sheets leaked through.
    if len(sections) > 1:
        section_taxes = []
        for s in sections:
            s_tax = s.get('igst', 0) + s.get('cgst', 0) + s.get('sgst', 0)
            section_taxes.append((s['section'], s_tax, s))
        
        max_section_tax = max(t for _, t, _ in section_taxes) if section_taxes else 0
        total_tax = sum(t for _, t, _ in section_taxes)
        
        if max_section_tax > 0 and total_tax > max_section_tax * 1.8:
            # Likely double/triple counting — use only the section-level sheets
            # that match known GSTR-1 supply type patterns
            supply_patterns = ['b2b', 'b2cl', 'b2cs', 'exp', 'at', 'b2ba']
            supply_sections = [
                s for _, _, s in section_taxes
                if any(p in s['section'].lower().replace(' ', '').replace('-', '')
                       for p in supply_patterns)
            ]
            
            if supply_sections:
                logger.warning(
                    f"  ⚠️ DEDUP: Total tax {total_tax:,.0f} is {total_tax/max_section_tax:.1f}x "
                    f"the largest section ({max_section_tax:,.0f}). "
                    f"Using only supply-type sheets: {[s['section'] for s in supply_sections]}"
                )
                sections = supply_sections
                grand = {c: 0.0 for c in COMPONENTS}
                for s in sections:
                    for c in COMPONENTS:
                        grand[c] += s.get(c, 0.0)
            else:
                # No recognizable supply sections — use single largest
                largest = max(section_taxes, key=lambda x: x[1])
                logger.warning(
                    f"  ⚠️ DEDUP: No supply-type sheets found. Using largest: '{largest[0]}'"
                )
                sections = [largest[2]]
                grand = {c: largest[2].get(c, 0.0) for c in COMPONENTS}

    grand = {c: round(v, 2) for c, v in grand.items()}
    return {'sections': sections, 'totals': grand}


# ── JSON ───────────────────────────────────────────────────────

def _g1_json(fbytes: bytes) -> Dict:
    data = json.loads(fbytes.decode('utf-8'))
    grand = {c: 0.0 for c in COMPONENTS}
    sections = []

    for key in ['b2b', 'b2cl', 'b2cs', 'exp', 'at']:
        entries = data.get(key, [])
        if not entries:
            continue
        tot = {c: 0.0 for c in COMPONENTS}
        _walk_json_tax(entries, tot)
        if any(tot[c] for c in tot):
            sections.append({'section': key.upper(), 'rows': 0,
                             **{c: round(v, 2) for c, v in tot.items()}})
            for c in COMPONENTS:
                grand[c] += tot[c]

    return {'sections': sections, 'totals': {c: round(v, 2) for c, v in grand.items()}}


# ── PDF ────────────────────────────────────────────────────────

def parse_gstr1_pdf(fbytes: bytes) -> Dict:
    """
    Extract outward supply totals from GSTR-1 PDF (GSTN portal format).

    The GSTN summary PDF has a table with columns:
      Nature of Supplies | No. of Recipients | No. of Invoices |
      Total Taxable Value | Integrated Tax | Central Tax | State/UT Tax | Cess

    We try section-wise extraction first; fall back to grand-total row.
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber is required for PDF parsing. Run: pip install pdfplumber")

    grand    = {c: 0.0 for c in COMPONENTS}
    sections = []

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    logger.info(f"GSTR-1 PDF extracted text length: {len(full_text)}")

    def to_f(s: str) -> float:
        return float(s.replace(',', '').strip())

    # ── Approach 1: section-wise regex ─────────────────────────
    # Each section row:  <Name>  <int>  <int>  <amount> <amount> <amount> <amount> <amount>
    # The 5 amounts = taxable, igst, cgst, sgst, cess
    NUM = r'([\d,]+\.\d{2})'
    section_keywords = [
        ('B2B',   r'(?:b2b|registered\s+taxable\s+outward)'),
        ('B2CL',  r'(?:b2cl|unregistered.*large)'),
        ('B2CS',  r'(?:b2cs|unregistered.*small)'),
        ('EXP',   r'(?:export|exports)'),
        ('AT',    r'(?:\bat\b|advance\s+received)'),
        ('ATADJ', r'(?:atadj|advance\s+adjusted)'),
    ]

    for name, kw in section_keywords:
        pat = (rf'(?i){kw}[^\n]*?\n?'
               rf'(?:[\d,]+\s+)?(?:[\d,]+\s+)?'   # optional count columns
               rf'{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}')
        m = re.search(pat, full_text, re.IGNORECASE | re.DOTALL)
        if m:
            vals = [to_f(g) for g in m.groups()]
            row  = {'section': name, 'rows': 0,
                    'taxable': vals[0], 'igst': vals[1],
                    'cgst':    vals[2], 'sgst': vals[3], 'cess': vals[4]}
            sections.append(row)
            for c in COMPONENTS:
                grand[c] += row[c]
            logger.info(f"  ✅ PDF section '{name}': {row}")

    # ── Approach 2: grand total row ─────────────────────────────
    if not sections:
        logger.warning("  Section-wise PDF parse failed — trying grand total row")
        grand = _parse_gstr1_pdf_grand_total(full_text)

    # ── Approach 3: table extraction via pdfplumber ─────────────
    if not sections and all(v == 0.0 for v in grand.values()):
        logger.warning("  Grand-total regex failed — trying pdfplumber table extraction")
        grand, sections = _parse_gstr1_pdf_tables(fbytes)

    return {'sections': sections, 'totals': {c: round(v, 2) for c, v in grand.items()}}


def _parse_gstr1_pdf_grand_total(text: str) -> Dict:
    """Extract grand total row from GSTR-1 PDF summary text."""
    grand = {c: 0.0 for c in COMPONENTS}
    NUM   = r'([\d,]+\.\d{2})'

    patterns = [
        # "Grand Total / Total  taxable igst cgst sgst cess"
        rf'(?i)(?:grand\s+total|total\s+tax(?:able)?)\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}',
        # fallback: just "Total" at start of line
        rf'(?im)^Total\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}',
    ]

    for pat in patterns:
        m = re.search(pat, text)
        if m:
            vals = [float(g.replace(',', '')) for g in m.groups()]
            for i, c in enumerate(COMPONENTS):
                grand[c] = vals[i] if i < len(vals) else 0.0
            logger.info(f"  ✅ Grand total from PDF: {grand}")
            return grand

    logger.error("  ❌ Could not find grand total in GSTR-1 PDF")
    return grand


def _parse_gstr1_pdf_tables(fbytes: bytes) -> tuple:
    """Use pdfplumber table extraction as last-resort for GSTR-1 PDF.
    
    IMPORTANT: The GSTN GSTR-1 PDF is a summary document where actual supply
    data appears in rows labeled "Total" (e.g., "Total 5 Invoice 9,06,195...").
    We cannot blindly skip "Total" — instead we skip amendment/differential rows
    and use deduplication to prevent triple-counting.
    """
    try:
        import pdfplumber
    except ImportError:
        return {c: 0.0 for c in COMPONENTS}, []

    grand    = {c: 0.0 for c in COMPONENTS}
    sections = []
    seen_amounts = set()  # Dedup: track (taxable, igst, cgst, sgst, cess) tuples

    # Labels that indicate non-supply rows — SKIP these
    SKIP_LABELS = [
        'nature of supplies', 'description', 'total taxable', 'integrated tax',
        'net differential', 'amended amount', 'net amended',
        'total liability',   # final summary row = duplicate of supply totals
        'grand total',
        'liable to collect', 'liable to pay',
    ]

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [str(c).strip() if c else '' for c in row]
                    # Look for rows with 5+ numeric values
                    nums = []
                    for cell in cells:
                        clean = cell.replace(',', '').replace(' ', '')
                        try:
                            nums.append(float(clean))
                        except ValueError:
                            pass
                    if len(nums) >= 5:
                        label = next((c for c in cells if c and not _is_number_str(c)), 'Unknown')
                        label_lower = label.lower().strip()
                        
                        # Skip header, amendment, and summary rows
                        if any(x in label_lower for x in SKIP_LABELS):
                            continue
                        
                        # Skip if all tax values are zero
                        tax_tuple = (nums[-5], nums[-4], nums[-3], nums[-2], nums[-1])
                        if all(v == 0.0 for v in tax_tuple):
                            continue
                        
                        # Skip duplicate amounts (same taxable+tax values = same data)
                        if tax_tuple in seen_amounts:
                            continue
                        seen_amounts.add(tax_tuple)
                        
                        row_data = {'section': label[:20], 'rows': 0,
                                    'taxable': nums[-5], 'igst': nums[-4],
                                    'cgst':    nums[-3], 'sgst': nums[-2], 'cess': nums[-1]}
                        sections.append(row_data)
                        for c in COMPONENTS:
                            grand[c] += row_data[c]

    return grand, sections


# ══════════════════════════════════════════════════════════════
#  GSTR-3B  PARSERS
# ══════════════════════════════════════════════════════════════

# ── Excel ──────────────────────────────────────────────────────

def parse_gstr3b(fbytes: bytes, fname: str) -> Dict:
    """Parse GSTR-3B from .xlsx / .xls — reads ALL rows of Table 3.1.
    
    Correct comparison:
      GSTR-1 total outward supplies should be compared with:
      3.1(a) + 3.1(b) + 3.1(c) + 3.1(e)
      
      3.1(d) = inward supplies liable to RCM (NOT outward, exclude).
    """
    xls = pd.ExcelFile(io.BytesIO(fbytes))
    logger.warning(f"GSTR-3B sheets: {xls.sheet_names}")

    grand = {c: 0.0 for c in COMPONENTS}
    rows_found = []

    # Row identifiers: (label, keywords_that_must_match, keywords_to_exclude)
    # Include (a), (b), (c), (e) for outward supply comparison
    # Exclude (d) = inward supplies liable to reverse charge (not outward)
    ROW_DEFS = [
        ('3.1(a)', ['other than zero'], []),
        ('3.1(a)', ['3.1(a)'], []),
        ('3.1(a)', ['3.1 (a)'], []),
        ('3.1(b)', ['zero rated'], ['other than zero']),
        ('3.1(b)', ['3.1(b)'], []),
        ('3.1(c)', ['nil rated'], []),
        ('3.1(c)', ['exempted'], ['other than']),
        ('3.1(c)', ['3.1(c)'], []),
        ('3.1(c)', ['3.1 (c)'], []),
        ('3.1(e)', ['non-gst'], []),
        ('3.1(e)', ['non gst'], []),
        ('3.1(e)', ['3.1(e)'], []),
        ('3.1(e)', ['3.1 (e)'], []),
    ]

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

            # Check which 3.1 row this matches
            matched_label = None
            for label, must_have, must_not in ROW_DEFS:
                if label in [r[0] for r in rows_found]:
                    continue  # already found this row
                if all(kw in text for kw in must_have) and not any(kw in text for kw in must_not):
                    matched_label = label
                    break

            if not matched_label:
                continue

            nums = [float(v) for v in row.values if _is_number(v)]
            if len(nums) < 3:
                if idx + 1 < len(df):
                    next_row = df.iloc[idx + 1]
                    nums = [float(v) for v in next_row.values if _is_number(v)]
                if len(nums) < 3:
                    continue

            totals = {
                'taxable': nums[0] if len(nums) > 0 else 0,
                'igst':    nums[1] if len(nums) > 1 else 0,
                'cgst':    nums[2] if len(nums) > 2 else 0,
                'sgst':    nums[3] if len(nums) > 3 else 0,
                'cess':    nums[4] if len(nums) > 4 else 0,
            }
            totals = {c: round(v, 2) for c, v in totals.items()}
            rows_found.append((matched_label, totals))
            logger.warning(f"  ✅ {matched_label} found in '{sn}' row {idx}: {totals}")

            # Sum into grand total — (a)+(b)+(c)+(e) for outward supplies
            for c in COMPONENTS:
                grand[c] += totals[c]

        if rows_found:
            break

    if rows_found:
        grand = {c: round(v, 2) for c, v in grand.items()}
        labels_str = '+'.join(r[0] for r in rows_found)
        logger.warning(f"  TOTAL 3B: {grand}")
        return {'totals': grand, 'row_label': f'Table 3.1 ({labels_str})'}

    logger.error("  ❌ Table 3.1 not found in GSTR-3B!")
    return {'totals': {c: 0.0 for c in COMPONENTS}, 'row_label': 'Not found'}


# ── JSON ───────────────────────────────────────────────────────

def _3b_json(fbytes: bytes) -> Dict:
    """Parse GSTR-3B JSON — sum 3.1(a)+3.1(b)+3.1(c)+3.1(e) for outward supply comparison."""
    data = json.loads(fbytes.decode('utf-8'))
    sup = data.get('sup_details', data)
    
    grand = {c: 0.0 for c in COMPONENTS}
    labels_found = []
    
    # 3.1(a) — Outward taxable (other than zero/nil/exempt)
    osup_det = sup.get('osup_det', {})
    if osup_det:
        for c, k in [('taxable','txval'),('igst','iamt'),('cgst','camt'),('sgst','samt'),('cess','csamt')]:
            grand[c] += float(osup_det.get(k, 0) or 0)
        labels_found.append('3.1(a)')
    
    # 3.1(b) — Zero-rated outward supplies
    osup_zero = sup.get('osup_zero', {})
    if osup_zero:
        for c, k in [('taxable','txval'),('igst','iamt'),('cgst','camt'),('sgst','samt'),('cess','csamt')]:
            grand[c] += float(osup_zero.get(k, 0) or 0)
        labels_found.append('3.1(b)')
    
    # 3.1(c) — Nil rated, exempted outward supplies
    osup_nil = sup.get('osup_nil_exmp', {})
    if osup_nil:
        for c, k in [('taxable','txval'),('igst','iamt'),('cgst','camt'),('sgst','samt'),('cess','csamt')]:
            grand[c] += float(osup_nil.get(k, 0) or 0)
        labels_found.append('3.1(c)')
    
    # 3.1(e) — Non-GST outward supplies
    osup_nongst = sup.get('osup_nongst', {})
    if osup_nongst:
        for c, k in [('taxable','txval'),('igst','iamt'),('cgst','camt'),('sgst','samt'),('cess','csamt')]:
            grand[c] += float(osup_nongst.get(k, 0) or 0)
        labels_found.append('3.1(e)')
    
    row_label = '+'.join(labels_found) if labels_found else '3.1(a)'
    return {'totals': {c: round(v, 2) for c, v in grand.items()}, 'row_label': row_label}


# ── PDF ────────────────────────────────────────────────────────

def parse_gstr3b_pdf(fbytes: bytes) -> Dict:
    """
    Extract Table 3.1(a) — Outward taxable supplies (other than zero rated, nil rated
    and exempted) — from GSTR-3B PDF downloaded from GSTN portal.

    The PDF row typically reads:
      (a) Outward taxable supplies (other than zero rated, nil rated and exempted)
          <taxable>  <igst>  <cgst>  <sgst/utgst>  <cess>
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber is required for PDF parsing. Run: pip install pdfplumber")

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    logger.warning(f"GSTR-3B PDF text length: {len(full_text)}")
    logger.warning(f"GSTR-3B PDF text (first 3000 chars):\n{full_text[:3000]}")

    NUM = r'([\d,]+\.\d{2})'

    def to_f(s: str) -> float:
        s = s.strip().replace(',', '')
        if s == '-' or s == '':
            return 0.0
        return float(s)

    # ── Strategy: Parse ALL rows (a)-(e) of Table 3.1 ──
    # The GSTR-1 includes B2B, B2CL, B2CS, EXP, AT — which span rows (a), (b), (d)
    # Row format in PDF text:
    #   (a) Outward taxable supplies (other than zero rated, nil rated and 0.00 0.00 0.00 0.00 0.00
    #   (b) Outward taxable supplies (zero rated) 14212660.00 3979544.80 - - 0.00
    #   (c) Other outward supplies (nil rated, exempted) 0.00 - - - -
    #   (d) Inward supplies (liable to reverse charge) 0.00 0.00 0.00 0.00 0.00
    #   (e) Non-GST outward supplies 0.00 - - - -
    
    # Rows to SUM for outward liability comparison with GSTR-1:
    # (a) = B2B + B2CS (other than zero/nil)
    # (b) = EXP (zero rated)
    # (c) = nil rated, exempted
    # (e) = non-GST outward supplies
    # EXCLUDE (d) = Inward supplies liable to RCM (NOT outward supplies)
    
    grand = {c: 0.0 for c in COMPONENTS}
    rows_found = []
    
    # Parse each row (a)-(e) individually
    row_patterns = [
        ('3.1(a)', r'\(a\)\s*Outward\s+taxable\s+supplies\s*\(other\s+than\s+zero'),
        ('3.1(b)', r'\(b\)\s*Outward\s+taxable\s+supplies\s*\(zero\s+rated\)'),
        ('3.1(c)', r'\(c\s*\)\s*Other\s+outward\s+supplies'),
        ('3.1(d)', r'\(d\)\s*Inward\s+supplies\s*\(liable\s+to\s+reverse'),
        ('3.1(e)', r'\(e\)\s*Non-GST'),
    ]
    
    for label, pattern in row_patterns:
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            # Get text after the match to find numbers
            after = full_text[m.end():]
            # Find up to 5 amounts (some may be "-")
            # Match numbers like "14212660.00" or "0.00", stopping at next section row
            amt_pattern = r'([\d,]+\.\d{2}|-)'
            amounts = re.findall(amt_pattern, after[:300])
            if len(amounts) >= 4:
                vals = [to_f(a) for a in amounts[:5]]
                row_data = {
                    'taxable': vals[0] if len(vals) > 0 else 0.0,
                    'igst':    vals[1] if len(vals) > 1 else 0.0,
                    'cgst':    vals[2] if len(vals) > 2 else 0.0,
                    'sgst':    vals[3] if len(vals) > 3 else 0.0,
                    'cess':    vals[4] if len(vals) > 4 else 0.0,
                }
                rows_found.append((label, row_data))
                logger.warning(f"  ✅ {label}: {row_data}")
                
                # Sum (a)+(b)+(c)+(e) for total outward supply liability
                # Skip (d) = inward supplies (RCM) — NOT outward
                if label in ('3.1(a)', '3.1(b)', '3.1(c)', '3.1(e)'):
                    for c in COMPONENTS:
                        grand[c] += row_data[c]
    
    if rows_found:
        summed = [l for l, _ in rows_found if l != '3.1(d)']
        logger.warning(f"  TOTAL from 3.1: {grand}")
        return {'totals': {c: round(v, 2) for c, v in grand.items()},
                'row_label': 'Table 3.1 (' + '+'.join(summed) + ')'}

    # ── Fallback: try the old patterns for just 3.1(a) ──
    logger.warning("  Row-by-row parse failed — trying regex patterns")
    for line in full_text.split('\n'):
        ll = line.lower()
        if 'outward' in ll or '3.1' in ll or 'zero' in ll:
            nums = re.findall(r'[\d,]+\.\d{2}', line)
            if len(nums) >= 4:
                vals = [float(n.replace(',', '')) for n in nums]
                totals = {
                    'taxable': vals[0] if len(vals) > 0 else 0.0,
                    'igst':    vals[1] if len(vals) > 1 else 0.0,
                    'cgst':    vals[2] if len(vals) > 2 else 0.0,
                    'sgst':    vals[3] if len(vals) > 3 else 0.0,
                    'cess':    vals[4] if len(vals) > 4 else 0.0,
                }
                logger.info(f"  ✅ 3.1(a) [line-scan] from PDF: {totals}")
                return {'totals': {c: round(v, 2) for c, v in totals.items()},
                        'row_label': '3.1(a) — line scan'}

    # ── Pattern 5: pdfplumber table extraction ──────────────────────
    logger.warning("  Line scan failed — trying pdfplumber table extraction for GSTR-3B")
    result = _parse_gstr3b_pdf_tables(fbytes)
    if result:
        return result

    # ── Log first 2000 chars for debugging ──────────────────────────
    logger.error(f"  ❌ Table 3.1(a) not found in GSTR-3B PDF. Text preview:\n{full_text[:2000]}")
    return {'totals': {c: 0.0 for c in COMPONENTS}, 'row_label': 'Not found'}


def _parse_gstr3b_pdf_tables(fbytes: bytes) -> Optional[Dict]:
    """Use pdfplumber table extraction for GSTR-3B PDF as last resort."""
    try:
        import pdfplumber
    except ImportError:
        return None

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [str(c).strip() if c else '' for c in row]
                    row_text = ' '.join(cells).lower()
                    if 'other than zero' not in row_text and '3.1' not in row_text:
                        continue
                    nums = []
                    for cell in cells:
                        clean = cell.replace(',', '').replace(' ', '')
                        try:
                            nums.append(float(clean))
                        except ValueError:
                            pass
                    if len(nums) >= 4:
                        totals = {
                            'taxable': nums[0],
                            'igst':    nums[1],
                            'cgst':    nums[2],
                            'sgst':    nums[3] if len(nums) > 3 else 0.0,
                            'cess':    nums[4] if len(nums) > 4 else 0.0,
                        }
                        logger.info(f"  ✅ 3.1(a) [table] from PDF: {totals}")
                        return {'totals': {c: round(v, 2) for c, v in totals.items()},
                                'row_label': '3.1(a) — table extraction'}
    return None


# ══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ══════════════════════════════════════════════════════════════

def _should_skip(sheet_name: str) -> bool:
    """Check if a sheet should be skipped (summary/HSN/document sheets)."""
    name = sheet_name.lower().replace(' ', '').replace('-', '').replace('_', '')
    # Also check unnormalized name for patterns like 'Table 12'
    name_lower = sheet_name.lower().strip()
    
    if any(kw.replace(' ', '') in name for kw in SKIP_SHEETS):
        return True
    
    # Skip GST Portal table number patterns: "4A", "12", "15", etc.
    # Keep only supply-type tables: b2b, b2cl, b2cs, exp, at
    if re.match(r'^table\s*\d', name_lower):
        # Only keep tables that are supply data (Table 4A = B2B, etc.)
        supply_tables = ['4a', '4b', '5', '6a', '6b', '6c', '7', '9a', '9b', '9c']
        table_num = re.sub(r'^table\s*', '', name_lower).strip()
        if table_num not in supply_tables:
            return True
    
    return False


def _read_sheet(xls, sn) -> Optional[pd.DataFrame]:
    try:
        scan = pd.read_excel(xls, sheet_name=sn, header=None, nrows=15)
        hdr  = _find_header(scan)
        df   = pd.read_excel(xls, sheet_name=sn, header=hdr)
        df.columns = [str(c).strip() for c in df.columns]
        return df.dropna(how='all').reset_index(drop=True)
    except Exception:
        return None


def _find_header(scan: pd.DataFrame) -> int:
    kw = ['igst', 'cgst', 'sgst', 'cess', 'taxable value', 'integrated tax',
          'central tax', 'state tax', 'gstin', 'invoice no', 'invoice number']
    best, best_s = 0, 0
    for i, row in scan.iterrows():
        s = sum(1 for v in row.values
                if pd.notna(v) and any(k in str(v).lower() for k in kw))
        if s >= best_s:
            best_s, best = s, i
    return best


def _find_tax_cols(df: pd.DataFrame) -> Dict[str, str]:
    mapping = {
        'taxable': ['taxable value', 'taxable amount', 'taxable_value'],
        'igst':    ['igst', 'integrated tax', 'iamt'],
        'cgst':    ['cgst', 'central tax', 'camt'],
        'sgst':    ['sgst', 'state tax', 'samt', 'sgst/utgst', 'utgst'],
        'cess':    ['cess', 'csamt'],
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


def _is_number_str(s: str) -> bool:
    try:
        float(s.replace(',', ''))
        return True
    except (ValueError, TypeError):
        return False


def _walk_json_tax(obj, totals):
    if isinstance(obj, list):
        for item in obj:
            _walk_json_tax(item, totals)
    elif isinstance(obj, dict):
        for comp, keys in [('taxable', ['txval']), ('igst', ['iamt']),
                           ('cgst',  ['camt']),  ('sgst', ['samt']), ('cess', ['csamt'])]:
            for k in keys:
                if k in obj:
                    totals[comp] += float(obj[k] or 0)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_json_tax(v, totals)


# ══════════════════════════════════════════════════════════════
#  RISK  &  ACTIONS
# ══════════════════════════════════════════════════════════════

def _risk(var: float, tax_g1: float = 0, tax_3b: float = 0) -> Dict:
    a = abs(var)
    if a == 0:
        return {'level': 'NO_RISK', 'icon': '✅',
                'heading': 'NO RISK — Fully Matched',
                'description': 'GSTR-1 and GSTR-3B match perfectly. No action needed.'}
    if a <= 1000:
        return {'level': 'LOW_RISK', 'icon': '🟢',
                'heading': f'LOW RISK — Rounding difference of ₹{a:,.0f}',
                'description': f'Variance of ₹{a:,.0f} is within rounding tolerance. No action required.'}
    if var > 0:  # GSTR-1 > GSTR-3B → under-declared in 3B
        return {'level': 'HIGH_RISK', 'icon': '🔴',
                'heading': f'HIGH RISK — Under-declared by ₹{a:,.0f}',
                'description': (f'GSTR-1 shows ₹{a:,.0f} more tax than GSTR-3B. '
                                f'ASMT-10 risk. Pay via DRC-03 with 18% interest from due date.')}
    else:  # GSTR-1 < GSTR-3B → over-declared in 3B
        return {'level': 'MEDIUM_RISK', 'icon': '🟡',
                'heading': f'MEDIUM RISK — Over-declared by ₹{a:,.0f}',
                'description': (f'Excess tax of ₹{a:,.0f} paid in GSTR-3B. '
                                f'File amendment or adjust in next period.')}


def _actions(var: float, comp_var: Dict) -> List[Dict]:
    acts = []
    a = abs(var)
    if var > 1:
        acts.append({'icon': '⚠️', 'color': 'red',
                     'title': f'Pay ₹{a:,.0f} via DRC-03',
                     'description': 'File DRC-03 with 18% interest from due date.'})
        acts.append({'icon': '📋', 'color': 'yellow',
                     'title': 'Verify missing invoices in 3B',
                     'description': 'Check if sales invoices were missed in 3B computation.'})
    elif var < -1:
        acts.append({'icon': '💰', 'color': 'blue',
                     'title': f'Claim excess ₹{a:,.0f}',
                     'description': 'Reduce next month 3B liability or adjust in GSTR-9.'})
    else:
        acts.append({'icon': '✅', 'color': 'green',
                     'title': 'No action needed', 'description': 'Perfectly matched.'})
    acts.append({'icon': '📝', 'color': 'purple',
                 'title': 'Prepare GSTR-9 Table 9',
                 'description': 'This reconciliation feeds into Table 9 of annual return.'})
    return acts


# ══════════════════════════════════════════════════════════════
#  EXCEL  REPORT
# ══════════════════════════════════════════════════════════════

def _excel_report(g1, g3b, variance, risk) -> bytes:
    buf    = io.BytesIO()
    labels = {'taxable': 'Taxable Value', 'igst': 'IGST',
              'cgst': 'CGST', 'sgst': 'SGST/UTGST', 'cess': 'Cess'}
    
    row_label = g3b.get('row_label', 'GSTR-3B')

    with pd.ExcelWriter(buf, engine='xlsxwriter') as w:
        rows = [
            {'Component':             labels[c],
             'GSTR-1':                g1['totals'][c],
             f'GSTR-3B ({row_label})': g3b['totals'][c],
             'Variance (G1−3B)':      variance[c],
             'Status': ('Match'          if abs(variance[c]) <= 1 else
                        'Under-declared' if variance[c] > 0 else 'Over-declared')}
            for c in COMPONENTS
        ]
        pd.DataFrame(rows).to_excel(w, index=False, sheet_name='Comparison')

        if g1.get('sections'):
            pd.DataFrame(g1['sections']).to_excel(w, index=False, sheet_name='GSTR-1 Breakup')

        for ws in w.sheets.values():
            for i in range(10):
                ws.set_column(i, i, 20)

    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
#  DEBUG  HELPER  (call this to inspect a PDF's raw text)
# ══════════════════════════════════════════════════════════════

def debug_pdf_text(fbytes: bytes, chars: int = 4000) -> str:
    """
    Print the raw text extracted from a PDF.
    Use this when PDF parsing returns zeros to tune the regex.

    Usage:
        with open("GSTR3B_file.pdf", "rb") as f:
            print(debug_pdf_text(f.read()))
    """
    try:
        import pdfplumber
    except ImportError:
        return "pdfplumber not installed. Run: pip install pdfplumber"

    with pdfplumber.open(io.BytesIO(fbytes)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    return text[:chars]