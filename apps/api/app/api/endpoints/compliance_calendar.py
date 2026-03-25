"""
Compliance Calendar — Statutory deadlines for Indian CAs.

Provides a comprehensive, up-to-date calendar of all GST, Income Tax,
TDS/TCS, ROC/MCA, and ESI/PF compliance deadlines relevant to
Chartered Accountants in India.  Deadlines are generated dynamically
based on the requested month/year so the data is always current.
"""

from datetime import date, timedelta
from typing import List, Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter()


# ──────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────
class Deadline(BaseModel):
    id: str
    title: str
    description: str
    due_date: str          # YYYY-MM-DD
    category: str          # gst | income-tax | tds | roc | audit | esi-pf
    form_or_section: str   # e.g., GSTR-1, 143(1), Form 16
    applicable_to: str     # e.g., "All taxpayers", "Companies", "Individuals"
    penalty_info: str      # Penalty for non-compliance
    priority: str          # critical | high | medium | low


# ──────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────
def _last_day(year: int, month: int) -> date:
    """Return the last day of a given month."""
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def _prev_month(year: int, month: int):
    """Return (year, month) for the previous month."""
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _month_name(m: int) -> str:
    return [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ][m]


def _quarter_label(month: int) -> str:
    """Get the quarter being reported for a given due-date month."""
    mapping = {
        7: "Q1 (Apr-Jun)", 10: "Q2 (Jul-Sep)",
        1: "Q3 (Oct-Dec)", 5: "Q4 (Jan-Mar)"
    }
    return mapping.get(month, "")


def _fy_for_month(year: int, month: int) -> str:
    """FY label for a given month, e.g. 2025-26."""
    if month >= 4:
        return f"{year}-{str(year + 1)[-2:]}"
    return f"{year - 1}-{str(year)[-2:]}"


def _ay_for_fy(fy_start: int) -> str:
    """Assessment year for a FY starting year."""
    return f"{fy_start + 1}-{str(fy_start + 2)[-2:]}"


# ──────────────────────────────────────────────────
# Deadline generators per category
# ──────────────────────────────────────────────────
def _gst_deadlines(year: int, month: int) -> List[dict]:
    """GST deadlines that fall in a given month."""
    deadlines = []
    py, pm = _prev_month(year, month)
    prev_mon = _month_name(pm)
    fy = _fy_for_month(year, month)

    # GSTR-3B — 20th of every month (for previous month)
    deadlines.append({
        "id": f"gst-3b-{year}{month:02d}",
        "title": f"GSTR-3B — {prev_mon} {py}",
        "description": f"Monthly summary return with self-assessed tax payment for {prev_mon} {py}. Late filing attracts interest @ 18% p.a. on tax payable.",
        "due_date": date(year, month, 20).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-3B",
        "applicable_to": "All regular taxpayers (turnover > ₹5 Cr or opted for monthly)",
        "penalty_info": "Late fee ₹50/day (₹20/day for nil return) + 18% interest on tax",
        "priority": "critical",
    })

    # GSTR-1 — 11th of every month
    deadlines.append({
        "id": f"gst-1-{year}{month:02d}",
        "title": f"GSTR-1 — {prev_mon} {py}",
        "description": f"Outward supply details for {prev_mon} {py}. Invoice-wise upload of B2B sales.",
        "due_date": date(year, month, 11).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-1",
        "applicable_to": "All regular taxpayers (turnover > ₹5 Cr or opted for monthly)",
        "penalty_info": "Late fee ₹50/day (₹20/day for nil return), max ₹10,000",
        "priority": "critical",
    })

    # GSTR-1 Quarterly (IFF) — 13th (for QRMP taxpayers) — Jan, Apr, Jul, Oct
    if month in [1, 2, 4, 5, 7, 8, 10, 11]:
        deadlines.append({
            "id": f"gst-iff-{year}{month:02d}",
            "title": f"IFF (Invoice Furnishing Facility) — {prev_mon} {py}",
            "description": f"Optional upload of B2B invoices for {prev_mon} {py} by QRMP taxpayers so recipients can claim ITC.",
            "due_date": date(year, month, 13).isoformat(),
            "category": "gst",
            "form_or_section": "IFF",
            "applicable_to": "QRMP scheme taxpayers (turnover ≤ ₹5 Cr)",
            "penalty_info": "No direct penalty but recipient ITC delayed",
            "priority": "medium",
        })

    # CMP-08 — 18th of month following quarter (Apr, Jul, Oct, Jan)
    if month in [4, 7, 10, 1]:
        q = _quarter_label(month)
        deadlines.append({
            "id": f"gst-cmp08-{year}{month:02d}",
            "title": f"CMP-08 — Composition Scheme {q}",
            "description": f"Quarterly statement-cum-challan for composition dealers for {q}.",
            "due_date": date(year, month, 18).isoformat(),
            "category": "gst",
            "form_or_section": "CMP-08",
            "applicable_to": "Composition scheme taxpayers (turnover ≤ ₹1.5 Cr)",
            "penalty_info": "Late fee ₹50/day + interest @ 18%",
            "priority": "high",
        })

    # GSTR-5 / 5A (Non-resident) — 20th
    deadlines.append({
        "id": f"gst-5-{year}{month:02d}",
        "title": f"GSTR-5 — Non-Resident Taxable Person — {prev_mon} {py}",
        "description": f"Return for non-resident taxable persons for {prev_mon} {py}.",
        "due_date": date(year, month, 20).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-5",
        "applicable_to": "Non-resident taxable persons",
        "penalty_info": "Late fee ₹50/day + 18% interest",
        "priority": "low",
    })

    # GSTR-6 (Input Service Distributor) — 13th
    deadlines.append({
        "id": f"gst-6-{year}{month:02d}",
        "title": f"GSTR-6 — ISD Return — {prev_mon} {py}",
        "description": f"Input Service Distributor return for {prev_mon} {py}.",
        "due_date": date(year, month, 13).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-6",
        "applicable_to": "Input Service Distributors",
        "penalty_info": "Late fee ₹50/day",
        "priority": "low",
    })

    # GSTR-7 (TDS under GST) — 10th
    deadlines.append({
        "id": f"gst-7-{year}{month:02d}",
        "title": f"GSTR-7 — GST TDS Return — {prev_mon} {py}",
        "description": f"Return for tax deducted at source under GST for {prev_mon} {py}.",
        "due_date": date(year, month, 10).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-7",
        "applicable_to": "Government departments & specified entities deducting TDS under GST",
        "penalty_info": "Late fee ₹200/day (₹100 CGST + ₹100 SGST)",
        "priority": "medium",
    })

    # GSTR-8 (E-commerce TCS) — 10th
    deadlines.append({
        "id": f"gst-8-{year}{month:02d}",
        "title": f"GSTR-8 — E-Commerce TCS — {prev_mon} {py}",
        "description": f"Return by e-commerce operators collecting TCS for {prev_mon} {py}.",
        "due_date": date(year, month, 10).isoformat(),
        "category": "gst",
        "form_or_section": "GSTR-8",
        "applicable_to": "E-commerce operators (Amazon, Flipkart, etc.)",
        "penalty_info": "Late fee ₹200/day",
        "priority": "medium",
    })

    # Annual return GSTR-9 — December 31
    if month == 12:
        prev_fy_start = year - 1
        deadlines.append({
            "id": f"gst-9-{year}",
            "title": f"GSTR-9 — Annual Return — FY {prev_fy_start}-{str(year)[-2:]}",
            "description": f"Annual GST return for FY {prev_fy_start}-{str(year)[-2:]}. Consolidates all monthly/quarterly returns.",
            "due_date": date(year, 12, 31).isoformat(),
            "category": "gst",
            "form_or_section": "GSTR-9 / 9C",
            "applicable_to": "All regular taxpayers (turnover > ₹2 Cr for 9C audit)",
            "penalty_info": "Late fee ₹200/day, max 0.50% of turnover",
            "priority": "critical",
        })

    return deadlines


def _income_tax_deadlines(year: int, month: int) -> List[dict]:
    """Income Tax deadlines for a given month."""
    deadlines = []
    fy = _fy_for_month(year, month)

    # Advance Tax installments — June 15, Sep 15, Dec 15, Mar 15
    advance_tax_dates = {
        6: ("First Installment — 15% of estimated tax", 15),
        9: ("Second Installment — 45% of estimated tax", 15),
        12: ("Third Installment — 75% of estimated tax", 15),
        3: ("Fourth Installment — 100% of estimated tax", 15),
    }
    if month in advance_tax_dates:
        desc_part, day = advance_tax_dates[month]
        deadlines.append({
            "id": f"it-advtax-{year}{month:02d}",
            "title": f"Advance Tax — {desc_part}",
            "description": f"Advance tax payment for FY {fy}. {desc_part}. Applies if estimated tax liability exceeds ₹10,000.",
            "due_date": date(year, month, day).isoformat(),
            "category": "income-tax",
            "form_or_section": "Section 208-211",
            "applicable_to": "All taxpayers with tax liability > ₹10,000 (except senior citizens without business income)",
            "penalty_info": "Interest u/s 234C for deferment of advance tax",
            "priority": "critical",
        })

    # ITR filing — July 31 (individuals), Oct 31 (audit cases), Nov 30 (TP)
    if month == 7:
        deadlines.append({
            "id": f"it-itr-nonaudit-{year}",
            "title": f"ITR Filing — Non-Audit Cases — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Last date for filing income tax returns for individuals, HUF, and firms not requiring audit for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 7, 31).isoformat(),
            "category": "income-tax",
            "form_or_section": "ITR-1/2/3/4",
            "applicable_to": "Individuals, HUFs, firms (non-audit)",
            "penalty_info": "Late fee u/s 234F — ₹5,000 (₹1,000 if income < ₹5 lakh) + interest u/s 234A",
            "priority": "critical",
        })

    if month == 10:
        deadlines.append({
            "id": f"it-itr-audit-{year}",
            "title": f"ITR Filing — Audit Cases — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Last date for filing ITR for taxpayers whose accounts are required to be audited for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 10, 31).isoformat(),
            "category": "income-tax",
            "form_or_section": "ITR-3/5/6/7",
            "applicable_to": "Companies, firms requiring audit, trusts",
            "penalty_info": "Late fee u/s 234F + interest u/s 234A @ 1% per month",
            "priority": "critical",
        })

    if month == 11:
        deadlines.append({
            "id": f"it-itr-tp-{year}",
            "title": f"ITR Filing — Transfer Pricing Cases — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Last date for ITR filing for assessees with international/specified domestic transactions for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 11, 30).isoformat(),
            "category": "income-tax",
            "form_or_section": "ITR-6 + Form 3CEB",
            "applicable_to": "Companies with international transactions",
            "penalty_info": "Late fee u/s 234F + penal interest",
            "priority": "high",
        })

    # Tax Audit Report — Sept 30
    if month == 9:
        deadlines.append({
            "id": f"it-taxaudit-{year}",
            "title": f"Tax Audit Report — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Last date for furnishing tax audit report u/s 44AB for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 9, 30).isoformat(),
            "category": "audit",
            "form_or_section": "Form 3CA/3CB + 3CD",
            "applicable_to": "Business turnover > ₹1 Cr (₹10 Cr if 95% digital) or Profession > ₹50 lakh",
            "penalty_info": "Penalty u/s 271B — 0.5% of turnover or ₹1,50,000 (whichever is lower)",
            "priority": "critical",
        })

    # Belated / Revised ITR — December 31
    if month == 12:
        deadlines.append({
            "id": f"it-belated-{year}",
            "title": f"Belated / Revised ITR — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Last date to file belated return u/s 139(4) or revised return u/s 139(5) for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 12, 31).isoformat(),
            "category": "income-tax",
            "form_or_section": "Section 139(4)/139(5)",
            "applicable_to": "All taxpayers who missed the original due date",
            "penalty_info": "Late fee ₹5,000 + may lose right to carry forward losses",
            "priority": "high",
        })

    # Updated Return u/s 139(8A) — March 31 (2 years from end of AY)
    if month == 3:
        deadlines.append({
            "id": f"it-updated-{year}",
            "title": f"Updated Return u/s 139(8A) — AY {year - 2}-{str(year - 1)[-2:]}",
            "description": f"Last date for filing updated return with additional tax for AY {year - 2}-{str(year - 1)[-2:]}.",
            "due_date": date(year, 3, 31).isoformat(),
            "category": "income-tax",
            "form_or_section": "ITR-U (Section 139(8A))",
            "applicable_to": "Taxpayers who omitted/understated income",
            "penalty_info": "Additional tax of 25% (within 12 months) or 50% (within 24 months)",
            "priority": "medium",
        })

    return deadlines


def _tds_deadlines(year: int, month: int) -> List[dict]:
    """TDS/TCS compliance deadlines for a given month."""
    deadlines = []
    py, pm = _prev_month(year, month)
    prev_mon = _month_name(pm)
    fy = _fy_for_month(year, month)

    # TDS deposit — 7th of every month (for previous month deductions)
    deadlines.append({
        "id": f"tds-deposit-{year}{month:02d}",
        "title": f"TDS/TCS Deposit — {prev_mon} {py}",
        "description": f"Deposit TDS/TCS deducted/collected during {prev_mon} {py} to the government.",
        "due_date": date(year, month, 7).isoformat(),
        "category": "tds",
        "form_or_section": "Challan 281",
        "applicable_to": "All TDS/TCS deductors/collectors",
        "penalty_info": "Interest @ 1.5% per month from date of deduction to date of payment",
        "priority": "critical",
    })

    # Quarterly TDS returns — 31st of month following quarter
    # Q1 (Apr-Jun) → Jul 31, Q2 (Jul-Sep) → Oct 31, Q3 (Oct-Dec) → Jan 31, Q4 (Jan-Mar) → May 31
    quarter_returns = {
        7: ("Q1 (Apr-Jun)", 31),
        10: ("Q2 (Jul-Sep)", 31),
        1: ("Q3 (Oct-Dec)", 31),
        5: ("Q4 (Jan-Mar)", 31),
    }
    if month in quarter_returns:
        q, day = quarter_returns[month]
        deadlines.append({
            "id": f"tds-return-{year}{month:02d}",
            "title": f"TDS Return (24Q/26Q/27Q) — {q}",
            "description": f"Quarterly TDS return filing for {q}. Form 24Q (salary), 26Q (non-salary), 27Q (NRI).",
            "due_date": date(year, month, day).isoformat(),
            "category": "tds",
            "form_or_section": "Form 24Q / 26Q / 27Q",
            "applicable_to": "All TDS deductors",
            "penalty_info": "Late fee ₹200/day u/s 234E + penalty u/s 271H (₹10,000 to ₹1 lakh)",
            "priority": "critical",
        })

    # TCS Return — same quarter dates
    if month in quarter_returns:
        q, day = quarter_returns[month]
        deadlines.append({
            "id": f"tcs-return-{year}{month:02d}",
            "title": f"TCS Return (27EQ) — {q}",
            "description": f"Quarterly TCS return filing for {q}.",
            "due_date": date(year, month, day).isoformat(),
            "category": "tds",
            "form_or_section": "Form 27EQ",
            "applicable_to": "All TCS collectors (specified sellers)",
            "penalty_info": "Late fee ₹200/day u/s 234E",
            "priority": "high",
        })

    # Form 16 / 16A issue dates
    if month == 6:  # June 15 — Form 16 for salary
        deadlines.append({
            "id": f"tds-form16-{year}",
            "title": f"Form 16 (Salary TDS Certificate) — FY {_fy_for_month(year, 3)}",
            "description": f"Issue Form 16 to all employees for salary TDS deducted during FY {_fy_for_month(year, 3)}.",
            "due_date": date(year, 6, 15).isoformat(),
            "category": "tds",
            "form_or_section": "Form 16",
            "applicable_to": "All employers deducting TDS on salary",
            "penalty_info": "Penalty u/s 272A — ₹100/day of default",
            "priority": "high",
        })

    # Form 16A — 15 days from due date of TDS return
    if month in [8, 11, 2]:
        deadlines.append({
            "id": f"tds-form16a-{year}{month:02d}",
            "title": f"Form 16A (Non-Salary TDS Certificate)",
            "description": f"Issue Form 16A for non-salary TDS to deductees within 15 days of filing TDS return.",
            "due_date": date(year, month, 15).isoformat(),
            "category": "tds",
            "form_or_section": "Form 16A",
            "applicable_to": "All TDS deductors (non-salary payments)",
            "penalty_info": "Penalty u/s 272A — ₹100/day of default",
            "priority": "medium",
        })

    return deadlines


def _roc_deadlines(year: int, month: int) -> List[dict]:
    """ROC/MCA compliance deadlines."""
    deadlines = []

    # Annual Return (MGT-7/7A) — 60 days from AGM (typically Nov 29 if AGM by Sep 30)
    if month == 11:
        deadlines.append({
            "id": f"roc-mgt7-{year}",
            "title": f"Annual Return (MGT-7/7A) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"File Annual Return within 60 days of AGM. MGT-7 for companies, MGT-7A for OPCs/small companies.",
            "due_date": date(year, 11, 29).isoformat(),
            "category": "roc",
            "form_or_section": "MGT-7 / MGT-7A",
            "applicable_to": "All companies registered under Companies Act, 2013",
            "penalty_info": "₹100/day of default for company + ₹50/day for officers",
            "priority": "high",
        })

    # Financial Statements (AOC-4) — 30 days from AGM (typically Oct 30)
    if month == 10:
        deadlines.append({
            "id": f"roc-aoc4-{year}",
            "title": f"Financial Statements (AOC-4) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"File financial statements within 30 days of AGM for FY {year - 1}-{str(year)[-2:]}.",
            "due_date": date(year, 10, 30).isoformat(),
            "category": "roc",
            "form_or_section": "AOC-4 / AOC-4 XBRL",
            "applicable_to": "All companies",
            "penalty_info": "₹100/day of default + ₹50/day for officers",
            "priority": "high",
        })

    # Annual General Meeting — September 30
    if month == 9:
        deadlines.append({
            "id": f"roc-agm-{year}",
            "title": f"Annual General Meeting (AGM) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"Hold AGM within 6 months from end of FY. For FY ending March {year}, AGM must be held by Sep 30.",
            "due_date": date(year, 9, 30).isoformat(),
            "category": "roc",
            "form_or_section": "Section 96",
            "applicable_to": "All companies (except OPCs)",
            "penalty_info": "Penalty of ₹1 lakh for company + ₹25,000 for every defaulting officer",
            "priority": "critical",
        })

    # LLP Form 11 — May 30
    if month == 5:
        deadlines.append({
            "id": f"roc-llp11-{year}",
            "title": f"LLP Annual Return (Form 11) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"LLP annual return filing for FY {year - 1}-{str(year)[-2:]}.",
            "due_date": date(year, 5, 30).isoformat(),
            "category": "roc",
            "form_or_section": "LLP Form 11",
            "applicable_to": "All LLPs",
            "penalty_info": "₹100/day of default",
            "priority": "high",
        })

    # LLP Form 8 — October 30
    if month == 10:
        deadlines.append({
            "id": f"roc-llp8-{year}",
            "title": f"LLP Statement of Accounts (Form 8) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"Statement of Account & Solvency for FY {year - 1}-{str(year)[-2:]}.",
            "due_date": date(year, 10, 30).isoformat(),
            "category": "roc",
            "form_or_section": "LLP Form 8",
            "applicable_to": "All LLPs",
            "penalty_info": "₹100/day of default",
            "priority": "high",
        })

    # DIR-3 KYC — September 30
    if month == 9:
        deadlines.append({
            "id": f"roc-dir3kyc-{year}",
            "title": f"Director KYC (DIR-3 KYC) — For DINs allotted on or before March 31, {year}",
            "description": f"Annual KYC for all directors holding DIN as on March 31, {year}.",
            "due_date": date(year, 9, 30).isoformat(),
            "category": "roc",
            "form_or_section": "DIR-3 KYC / DIR-3 KYC-WEB",
            "applicable_to": "All directors with DIN",
            "penalty_info": "₹5,000 penalty for late filing; DIN deactivated",
            "priority": "high",
        })

    # DPT-3 — June 30
    if month == 6:
        deadlines.append({
            "id": f"roc-dpt3-{year}",
            "title": f"Return of Deposits (DPT-3) — FY {year - 1}-{str(year)[-2:]}",
            "description": f"Return of deposits/outstanding receipts for FY {year - 1}-{str(year)[-2:]}.",
            "due_date": date(year, 6, 30).isoformat(),
            "category": "roc",
            "form_or_section": "DPT-3",
            "applicable_to": "Companies accepting deposits or having outstanding loan amounts",
            "penalty_info": "Penalty for non-filing under Companies Act",
            "priority": "medium",
        })

    # MSME-1 — Half-yearly (Apr & Oct)
    if month in [4, 10]:
        half = "Oct-Mar" if month == 4 else "Apr-Sep"
        deadlines.append({
            "id": f"roc-msme1-{year}{month:02d}",
            "title": f"MSME-1 — Outstanding Payments ({half})",
            "description": f"Half-yearly return for outstanding MSME payments exceeding 45 days for {half}.",
            "due_date": date(year, month, 30).isoformat(),
            "category": "roc",
            "form_or_section": "MSME-1",
            "applicable_to": "Companies with outstanding MSME payments > 45 days",
            "penalty_info": "Penalty under Companies Act, 2013",
            "priority": "medium",
        })

    return deadlines


def _audit_deadlines(year: int, month: int) -> List[dict]:
    """Statutory and Tax Audit deadlines."""
    deadlines = []

    # Statutory Audit completion — before AGM (typically Sep)
    if month == 9:
        deadlines.append({
            "id": f"audit-statutory-{year}",
            "title": f"Statutory Audit Completion — FY {year - 1}-{str(year)[-2:]}",
            "description": f"Complete statutory audit and issue audit report before AGM for FY {year - 1}-{str(year)[-2:]}.",
            "due_date": date(year, 9, 27).isoformat(),
            "category": "audit",
            "form_or_section": "Section 143 of Companies Act",
            "applicable_to": "All companies",
            "penalty_info": "Disciplinary action by ICAI + company penalties",
            "priority": "critical",
        })

    # Transfer Pricing Audit (Form 3CEB) — November 30
    if month == 11:
        deadlines.append({
            "id": f"audit-tp-{year}",
            "title": f"Transfer Pricing Report (Form 3CEB) — AY {year}-{str(year + 1)[-2:]}",
            "description": f"Report on international or specified domestic transactions for AY {year}-{str(year + 1)[-2:]}.",
            "due_date": date(year, 11, 30).isoformat(),
            "category": "audit",
            "form_or_section": "Form 3CEB (Section 92E)",
            "applicable_to": "Entities with international/specified domestic transactions",
            "penalty_info": "Penalty u/s 271BA — ₹1,00,000",
            "priority": "high",
        })

    return deadlines


def _esipf_deadlines(year: int, month: int) -> List[dict]:
    """ESI and PF compliance deadlines."""
    deadlines = []
    py, pm = _prev_month(year, month)
    prev_mon = _month_name(pm)

    # PF deposit — 15th of every month
    deadlines.append({
        "id": f"pf-deposit-{year}{month:02d}",
        "title": f"PF Payment — {prev_mon} {py}",
        "description": f"EPF/EPS/EDLI contribution deposit for {prev_mon} {py} wages.",
        "due_date": date(year, month, 15).isoformat(),
        "category": "esi-pf",
        "form_or_section": "EPF Challan (ECR)",
        "applicable_to": "All establishments with 20+ employees",
        "penalty_info": "Damages @ 5-25% of arrears + penal interest @ 12% p.a.",
        "priority": "high",
    })

    # ESI deposit — 15th of every month
    deadlines.append({
        "id": f"esi-deposit-{year}{month:02d}",
        "title": f"ESI Payment — {prev_mon} {py}",
        "description": f"ESI contribution deposit for {prev_mon} {py} wages.",
        "due_date": date(year, month, 15).isoformat(),
        "category": "esi-pf",
        "form_or_section": "ESI Challan",
        "applicable_to": "Establishments with 10+ employees (wages ≤ ₹21,000/month)",
        "penalty_info": "Damages @ 5-25% + simple interest @ 12% p.a.",
        "priority": "high",
    })

    return deadlines


# ──────────────────────────────────────────────────
# API Endpoints
# ──────────────────────────────────────────────────
@router.get("/deadlines", response_model=List[Deadline])
async def get_deadlines(
    year: int = Query(..., description="Year (e.g. 2026)"),
    month: int = Query(..., ge=1, le=12, description="Month (1-12)"),
    category: Optional[str] = Query(None, description="Filter by category: gst, income-tax, tds, roc, audit, esi-pf"),
):
    """
    Get all statutory compliance deadlines for Indian CAs for a given month/year.
    Dynamically computed — always current and accurate.
    """
    all_deadlines = []
    all_deadlines.extend(_gst_deadlines(year, month))
    all_deadlines.extend(_income_tax_deadlines(year, month))
    all_deadlines.extend(_tds_deadlines(year, month))
    all_deadlines.extend(_roc_deadlines(year, month))
    all_deadlines.extend(_audit_deadlines(year, month))
    all_deadlines.extend(_esipf_deadlines(year, month))

    if category:
        all_deadlines = [d for d in all_deadlines if d["category"] == category]

    # Sort by due date
    all_deadlines.sort(key=lambda d: d["due_date"])

    return all_deadlines


@router.get("/upcoming")
async def get_upcoming_deadlines(
    days: int = Query(30, ge=1, le=90, description="Number of days to look ahead"),
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """
    Get upcoming statutory deadlines from today for the next N days.
    Useful for dashboard widgets and notifications.
    """
    today = date.today()
    end = today + timedelta(days=days)

    all_deadlines = []
    # Cover current and next month(s)
    seen_months = set()
    d = today
    while d <= end:
        key = (d.year, d.month)
        if key not in seen_months:
            seen_months.add(key)
            all_deadlines.extend(_gst_deadlines(d.year, d.month))
            all_deadlines.extend(_income_tax_deadlines(d.year, d.month))
            all_deadlines.extend(_tds_deadlines(d.year, d.month))
            all_deadlines.extend(_roc_deadlines(d.year, d.month))
            all_deadlines.extend(_audit_deadlines(d.year, d.month))
            all_deadlines.extend(_esipf_deadlines(d.year, d.month))
        d += timedelta(days=1)

    # Filter to range
    all_deadlines = [
        dl for dl in all_deadlines
        if today.isoformat() <= dl["due_date"] <= end.isoformat()
    ]

    if category:
        all_deadlines = [d for d in all_deadlines if d["category"] == category]

    all_deadlines.sort(key=lambda d: d["due_date"])

    # De-duplicate by id
    seen = set()
    unique = []
    for dl in all_deadlines:
        if dl["id"] not in seen:
            seen.add(dl["id"])
            unique.append(dl)

    return unique


@router.get("/categories")
async def get_categories():
    """Return available deadline categories with labels and counts."""
    return [
        {"id": "gst", "label": "GST", "color": "#3b82f6", "icon": "💰"},
        {"id": "income-tax", "label": "Income Tax", "color": "#f59e0b", "icon": "📋"},
        {"id": "tds", "label": "TDS / TCS", "color": "#8b5cf6", "icon": "📑"},
        {"id": "roc", "label": "ROC / MCA", "color": "#10b981", "icon": "🏢"},
        {"id": "audit", "label": "Audit", "color": "#06b6d4", "icon": "🔍"},
        {"id": "esi-pf", "label": "ESI / PF", "color": "#ec4899", "icon": "👷"},
    ]
