"""
Financial Instrument Classifier
────────────────────────────────
Deterministic classification of Tally ledgers and vouchers into
Financial Instrument categories. No AI — pure rule-based matching.

Usage:
    from app.services.fi_classifier import classify_company_fi
    result = await classify_company_fi(db, "KANSAL FABRICATIONS PRIVATE LIMITED")
"""

import re
import logging
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
#  LAYER 1: Parent Group Classification
# ═══════════════════════════════════════════════════════
# Tally parent groups that are ALWAYS financial instruments.
# Maps lowercase parent → FI sub-category.

FI_PARENT_GROUPS = {
    # ── Asset-side: Investment ledgers ──
    "investments":                "Investments",
    "current investments":        "Investments",
    "non-current investments":    "Investments",
}

# Parent groups where we still apply keyword matching (Layer 1.5)
# These are NOT entirely FI — only specific ledgers within them are.
FI_INCOME_EXPENSE_PARENTS = {
    "income (indirect)",
    "indirect income",
    "indirect incomes",
    "indirect expenses",
    "expenses (indirect)",
    "duties & taxes",      # TDS ledgers
}


# ═══════════════════════════════════════════════════════
#  LAYER 2: Keyword Classification
# ═══════════════════════════════════════════════════════
# Applied to ledger name AND parent group when Layer 1 misses.
# Tuple: (keyword, fi_sub_category)

FI_KEYWORDS = [
    # AIF / PMS — check BEFORE equity ("equity" keyword could false-match AIF names)
    ("alternative investment", "AIF"),
    ("aif ",                 "AIF"),
    ("aif-",                 "AIF"),
    ("cat iii",              "AIF"),
    ("cat-iii",              "AIF"),
    ("category iii",         "AIF"),
    ("pioneer equity",       "AIF"),
    ("portfolio management", "PMS"),
    ("pms ",                 "PMS"),
    ("pms-",                 "PMS"),
    ("discretionary",        "PMS"),

    # Equity / Shares (direct holdings)
    ("shares of ",           "Equity Holdings"),
    ("preference share",     "Preference Shares"),
    ("bonus share",          "Equity Holdings"),
    ("equity share",         "Equity Holdings"),

    # Mutual Funds
    ("mutual fund",          "Mutual Funds"),

    # Debentures / Bonds
    ("debenture",            "Debentures"),
    (" ncd",                 "Debentures"),

    # Fixed Deposits
    ("fixed deposit",        "Fixed Deposits"),
    (" fd ",                 "Fixed Deposits"),
    (" fdr",                 "Fixed Deposits"),

    # Interest / Dividend income
    ("interest received",    "Interest Income"),
    ("interest income",      "Interest Income"),
    ("interest on fd",       "Interest Income"),
    ("interest on fdr",      "Interest Income"),
    ("interest on deposit",  "Interest Income"),
    ("interest on invest",   "Interest Income"),
    ("interest on loan",     "Interest Income"),
    ("interest on saving",   "Interest Income"),
    ("dividend receivable",  "Dividend Income"),
    ("dividend income",      "Dividend Income"),
    ("dividend received",    "Dividend Income"),
    ("dividend on share",    "Dividend Income"),
    ("dividend on invest",   "Dividend Income"),
    ("income from dividend", "Dividend Income"),
    ("income from mutual",   "Dividend Income"),

    # Capital Gains
    ("profit on sale of share",   "Capital Gains"),
    ("loss on sale of share",     "Capital Gains"),
    ("profit on sale of invest",  "Capital Gains"),
    ("loss on sale of invest",    "Capital Gains"),
    ("profit/loss on sale of share", "Capital Gains"),
    ("profit on sale of mutual",  "Capital Gains"),
    ("loss on sale of mutual",    "Capital Gains"),
    ("capital gain",              "Capital Gains"),
    ("short term capital",        "Capital Gains"),
    ("long term capital",         "Capital Gains"),
    ("stcg",                      "Capital Gains"),
    ("ltcg",                      "Capital Gains"),

    # Trading expenses
    ("brokerage",            "Trading Expenses"),
    (" stt",                 "Trading Expenses"),
    ("securities transaction", "Trading Expenses"),
    ("stamp duty",           "Trading Expenses"),
    ("demat",                "Trading Expenses"),
    ("depository",           "Trading Expenses"),
    ("dp charges",           "Trading Expenses"),
    ("turnover tax",         "Trading Expenses"),

    # TDS on FI income
    ("tds on dividend",      "TDS on FI"),
    ("tds on interest",      "TDS on FI"),

]


# ═══════════════════════════════════════════════════════
#  LAYER 3: Voucher Narration Patterns
# ═══════════════════════════════════════════════════════
# Regex patterns applied to voucher narration to classify FI transactions.
# Order matters — first match wins.

NARRATION_PATTERNS = [
    (re.compile(r"purchase\s+\d+\s+shares?\s*@", re.I),        "Share Purchase"),
    (re.compile(r"sale\s+of?\s*\d*\s*shares?\s*@", re.I),      "Share Sale"),
    (re.compile(r"being\s+purchase\s+\d+\s+shares?", re.I),    "Share Purchase"),
    (re.compile(r"being\s+sale\s+\d+\s+shares?", re.I),        "Share Sale"),
    (re.compile(r"gain\s*/?\s*loss.*transfer.*share", re.I),    "Capital Gain/Loss"),
    (re.compile(r"profit.*sale.*share", re.I),                  "Capital Gain/Loss"),
    (re.compile(r"loss.*sale.*share", re.I),                    "Capital Gain/Loss"),
    (re.compile(r"dividend\s+(recd|received|from)", re.I),      "Dividend Receipt"),
    (re.compile(r"interest\s+(recd|received|from|on)", re.I),   "Interest Receipt"),
    (re.compile(r"fd\s*maturity|fdr\s*maturity", re.I),         "FD Maturity"),
    (re.compile(r"loan.*against.*fd", re.I),                    "FD-Backed Loan"),
    (re.compile(r"brokerage|stt\s|stamp\s*duty", re.I),         "Trading Expense"),
    (re.compile(r"tds.*dividend", re.I),                        "TDS on Dividend"),
    (re.compile(r"tds.*interest", re.I),                        "TDS on Interest"),
    (re.compile(r"mutual\s*fund|sip\s|switch\s|redeem", re.I),  "Mutual Fund Txn"),
    (re.compile(r"invest|debenture|bond|ncd", re.I),            "Investment Txn"),
]


# ═══════════════════════════════════════════════════════
#  CLASSIFICATION FUNCTIONS
# ═══════════════════════════════════════════════════════

def classify_ledger(name: str, parent: str, primary_group: str = "") -> Optional[str]:
    """Classify a single ledger. Returns FI sub-category or None.

    Args:
        name: Ledger name (e.g. 'SHARES OF HDFC BANK LTD - IIFL')
        parent: Immediate parent group (e.g. 'WHITE OAK PIONEER EQUITY')
        primary_group: Root BS group from Tally hierarchy (e.g. 'Investments')
    """
    name_lower = (name or "").lower().strip()
    parent_lower = (parent or "").lower().strip()
    primary_lower = (primary_group or "").lower().strip()

    # Layer 1: Check primary_group (root BS group)
    # If under 'Investments', try keyword sub-classification first (AIF vs Equity vs MF)
    # before falling back to generic 'Investments'
    is_investment_parent = (
        (primary_lower and primary_lower in FI_PARENT_GROUPS)
        or parent_lower in FI_PARENT_GROUPS
    )
    if is_investment_parent:
        # Try specific sub-classification via keywords
        combined_check = f" {name_lower} {parent_lower} "
        for keyword, category in FI_KEYWORDS:
            if keyword in combined_check:
                return category
        # No keyword match → generic 'Investments'
        return FI_PARENT_GROUPS.get(primary_lower) or FI_PARENT_GROUPS.get(parent_lower, "Investments")

    # Layer 1.5: For Income/Expense parent groups, apply keyword matching
    # to catch FI-related income (dividends, interest, capital gains) and
    # expenses (brokerage, STT, TDS) that live outside 'Investments'
    combined = f" {name_lower} {parent_lower} "
    is_fi_income_expense = (
        primary_lower in FI_INCOME_EXPENSE_PARENTS
        or parent_lower in FI_INCOME_EXPENSE_PARENTS
    )
    if is_fi_income_expense:
        for keyword, category in FI_KEYWORDS:
            if keyword in combined:
                return category

    # Layer 2: Keyword match in name or parent (catch-all for any ledger)
    for keyword, category in FI_KEYWORDS:
        if keyword in combined:
            return category

    return None


def classify_narration(narration: str) -> Optional[str]:
    """Classify a voucher by its narration. Returns FI txn type or None."""
    if not narration:
        return None
    for pattern, txn_type in NARRATION_PATTERNS:
        if pattern.search(narration):
            return txn_type
    return None


def parse_share_details(narration: str) -> dict:
    """Extract share purchase/sale details from narration text.
    E.g. 'BEING PURCHASE 597 SHARES @ 441.6125.' → {qty: 597, price: 441.61, scrip: ''}
    """
    result = {"quantity": None, "price": None, "scrip": None}
    m = re.search(r"(\d+)\s+shares?\s*@\s*([\d,.]+)", narration, re.I)
    if m:
        result["quantity"] = int(m.group(1))
        price_str = m.group(2).replace(",", "").rstrip(".")
        try:
            result["price"] = round(float(price_str), 2)
        except ValueError:
            pass

    # Try to extract scrip name from narration like "DIVIDEND RECD. FROM INFOSYS LIMITED"
    m2 = re.search(r"(?:from|of)\s+(.+?)(?:\.|$)", narration, re.I)
    if m2:
        result["scrip"] = m2.group(1).strip().rstrip(".")
    return result


# ═══════════════════════════════════════════════════════
#  MAIN CLASSIFICATION ENTRY POINT
# ═══════════════════════════════════════════════════════

async def classify_company_fi(db, company_name: str, date_from: str = None, date_to: str = None) -> dict:
    """
    Classify all ledgers and vouchers for a Tally-synced company
    into Financial Instrument categories.

    Args:
        date_from: YYYYMMDD — filter vouchers from this date (inclusive)
        date_to:   YYYYMMDD — filter vouchers up to this date (inclusive)
        When date range is provided, ledger balances are recomputed as
        opening_balance + net voucher entry movements within the period,
        so AUM/holdings reflect the selected FY correctly.

    Returns a rich dashboard-ready dict with:
      - fi_ledgers: classified ledger list with balances
      - fi_vouchers: classified voucher list with parsed details
      - holdings: grouped equity/MF/FD holdings from ledger balances
      - summary: aggregate stats (total invested, gains, dividends, etc.)
      - category_breakdown: count/amount by FI sub-category
    """
    from sqlalchemy.future import select
    from sqlalchemy import func, desc
    from app.models.models import Ledger, Voucher, VoucherEntry

    # ── 1. Classify Ledgers ──────────────────────────────
    # Derive fy_period from date_from (e.g. "20250401" → "2025-26")
    fy_period = None
    if date_from and len(date_from) >= 4:
        try:
            y = int(date_from[:4])
            fy_period = f"{y}-{(y + 1) % 100:02d}"
        except ValueError:
            pass

    # Get last sync status
    sync_query = select(func.max(Ledger.synced_at)).where(Ledger.company_name == company_name)
    last_sync = (await db.execute(sync_query)).scalar()

    ledger_query = (
        select(Ledger)
        .where(Ledger.company_name == company_name)
    )
    if fy_period:
        ledger_query = ledger_query.where(Ledger.fy_period == fy_period)
    ledger_query = ledger_query.order_by(Ledger.parent, Ledger.name)

    ledgers = (await db.execute(ledger_query)).scalars().all()

    # Fallback: if no FY-specific rows found, try without fy_period filter
    if not ledgers and fy_period:
        logger.info(f"No ledgers found for fy_period={fy_period}, falling back to all ledgers")
        ledgers = (await db.execute(
            select(Ledger)
            .where(Ledger.company_name == company_name)
            .order_by(Ledger.parent, Ledger.name)
        )).scalars().all()

    if not ledgers:
        return {"error": f"No ledgers found for '{company_name}'", "has_data": False}

    fi_ledgers = []
    fi_ledger_names = set()
    non_fi_count = 0

    for l in ledgers:
        fi_cat = classify_ledger(l.name, l.parent or "", getattr(l, "primary_group", "") or "")
        if fi_cat:
            ob = float(l.opening_balance or 0)
            cb = float(l.closing_balance or 0)
            fi_ledgers.append({
                "id": str(l.id),
                "name": l.name,
                "parent": l.parent or "",
                "primary_group": getattr(l, "primary_group", "") or "",
                "fi_category": fi_cat,
                "opening_balance": round(ob, 2),
                "closing_balance": round(cb, 2),
                "net_movement": round(cb - ob, 2),
            })
            fi_ledger_names.add(l.name)
        else:
            non_fi_count += 1

    # ── 2. Classify Vouchers ─────────────────────────────
    voucher_query = (
        select(Voucher)
        .where(Voucher.company_name == company_name)
    )
    # Apply date range filtering when FY is selected
    if date_from:
        voucher_query = voucher_query.where(Voucher.date >= date_from)
    if date_to:
        voucher_query = voucher_query.where(Voucher.date <= date_to)
    voucher_query = voucher_query.order_by(desc(Voucher.date))

    vouchers = (await db.execute(voucher_query)).scalars().all()

    fi_vouchers = []
    non_fi_voucher_count = 0

    for v in vouchers:
        narration = v.narration or ""
        fi_txn_type = classify_narration(narration)

        if fi_txn_type:
            amt = float(v.amount or 0)
            details = parse_share_details(narration)
            fi_vouchers.append({
                "id": str(v.id),
                "guid": v.guid,
                "date": v.date,
                "voucher_type": v.voucher_type or "",
                "voucher_number": v.voucher_number or "",
                "party_name": v.party_name or "",
                "amount": round(amt, 2),
                "narration": narration,
                "fi_txn_type": fi_txn_type,
                "quantity": details["quantity"],
                "price": details["price"],
                "scrip": details["scrip"],
            })
        else:
            non_fi_voucher_count += 1

    # ── 3. Also check voucher entries touching FI ledgers ─
    # (catches vouchers with no FI narration but touching FI ledgers)
    if fi_ledger_names:
        fi_guids_from_narration = {v["guid"] for v in fi_vouchers}
        entries = (await db.execute(
            select(VoucherEntry)
            .where(
                VoucherEntry.company_name == company_name,
                VoucherEntry.ledger_name.in_(fi_ledger_names),
            )
        )).scalars().all()

        extra_guids = set()
        for e in entries:
            if e.voucher_guid not in fi_guids_from_narration:
                extra_guids.add(e.voucher_guid)

        if extra_guids:
            extra_q = select(Voucher).where(
                Voucher.company_name == company_name,
                Voucher.guid.in_(extra_guids),
            )
            # Apply the same date range filter as the main voucher query
            if date_from:
                extra_q = extra_q.where(Voucher.date >= date_from)
            if date_to:
                extra_q = extra_q.where(Voucher.date <= date_to)
            extra_vouchers = (await db.execute(extra_q)).scalars().all()
            for v in extra_vouchers:
                amt = float(v.amount or 0)
                fi_vouchers.append({
                    "id": str(v.id),
                    "guid": v.guid,
                    "date": v.date,
                    "voucher_type": v.voucher_type or "",
                    "voucher_number": v.voucher_number or "",
                    "party_name": v.party_name or "",
                    "amount": round(amt, 2),
                    "narration": v.narration or "",
                    "fi_txn_type": "FI Ledger Entry",
                    "quantity": None,
                    "price": None,
                    "scrip": None,
                })
                non_fi_voucher_count -= 1

        # Sort combined list by date descending
        fi_vouchers.sort(key=lambda x: x["date"] or "", reverse=True)

    # ── 3.5. Compute FY-specific balances when date range is provided ──
    # Reconstruct the ledger balance at any point in time using:
    #   Balance@date = Tally opening_balance + SUM(voucher_entries WHERE date <= that_date)
    #
    # FY Opening = Tally OB + cumulative movements BEFORE date_from
    # FY Closing = Tally OB + cumulative movements UP TO date_to
    # FY Movement = FY Closing - FY Opening (net change during this FY)
    if (date_from or date_to) and fi_ledger_names:
        # Query 1: Cumulative movements UP TO date_to → FY closing balance
        closing_query = (
            select(
                VoucherEntry.ledger_name,
                func.sum(VoucherEntry.amount).label("cumulative"),
            )
            .where(
                VoucherEntry.company_name == company_name,
                VoucherEntry.ledger_name.in_(fi_ledger_names),
            )
            .group_by(VoucherEntry.ledger_name)
        )
        if date_to:
            closing_query = closing_query.where(VoucherEntry.voucher_date <= date_to)

        closing_rows = (await db.execute(closing_query)).all()
        closing_map = {row.ledger_name: float(row.cumulative or 0) for row in closing_rows}

        # Query 2: Cumulative movements BEFORE date_from → FY opening balance
        opening_map = {}
        if date_from:
            opening_query = (
                select(
                    VoucherEntry.ledger_name,
                    func.sum(VoucherEntry.amount).label("cumulative"),
                )
                .where(
                    VoucherEntry.company_name == company_name,
                    VoucherEntry.ledger_name.in_(fi_ledger_names),
                    VoucherEntry.voucher_date < date_from,
                )
                .group_by(VoucherEntry.ledger_name)
            )
            opening_rows = (await db.execute(opening_query)).all()
            opening_map = {row.ledger_name: float(row.cumulative or 0) for row in opening_rows}

        # Update fi_ledgers with FY-specific balances
        for fl in fi_ledgers:
            tally_ob = fl["opening_balance"]  # Tally's opening balance (start of loaded period)

            # FY closing = Tally OB + all movements up to end of selected FY
            cum_to_end = closing_map.get(fl["name"], 0)
            fy_closing = tally_ob + cum_to_end

            # FY opening = Tally OB + all movements before selected FY started
            cum_before_start = opening_map.get(fl["name"], 0)
            fy_opening = tally_ob + cum_before_start

            fl["tally_closing_balance"] = fl["closing_balance"]  # preserve original
            fl["opening_balance"] = round(fy_opening, 2)
            fl["closing_balance"] = round(fy_closing, 2)
            fl["fy_movement"] = round(fy_closing - fy_opening, 2)
            fl["net_movement"] = round(fy_closing - fy_opening, 2)

        logger.info(
            f"FI FY-specific balances: {len(closing_map)} ledgers computed "
            f"({date_from} to {date_to})"
        )

    # ── 4. Build Holdings Summary ────────────────────────
    holdings = defaultdict(lambda: {"ledgers": [], "total_ob": 0, "total_cb": 0, "count": 0})
    for fl in fi_ledgers:
        cat = fl["fi_category"]
        holdings[cat]["ledgers"].append({
            "name": fl["name"],
            "parent": fl["parent"],
            "opening_balance": fl["opening_balance"],
            "closing_balance": fl["closing_balance"],
        })
        holdings[cat]["total_ob"] += fl["opening_balance"]
        holdings[cat]["total_cb"] += fl["closing_balance"]
        holdings[cat]["count"] += 1

    holdings_summary = []
    for cat, data in sorted(holdings.items(), key=lambda x: abs(x[1]["total_cb"]), reverse=True):
        # Only include ledgers with non-zero balances in the top-level summary
        active_ledgers = [l for l in data["ledgers"] if abs(l["closing_balance"]) > 0.01]
        holdings_summary.append({
            "category": cat,
            "count": data["count"],
            "active_count": len(active_ledgers),
            "total_opening": round(data["total_ob"], 2),
            "total_closing": round(data["total_cb"], 2),
            "net_movement": round(data["total_cb"] - data["total_ob"], 2),
            "top_holdings": sorted(active_ledgers, key=lambda x: abs(x["closing_balance"]), reverse=True)[:10],
        })

    # ── 5. Build Transaction Type Breakdown ──────────────
    txn_breakdown = defaultdict(lambda: {"count": 0, "total_amount": 0})
    for fv in fi_vouchers:
        t = fv["fi_txn_type"]
        txn_breakdown[t]["count"] += 1
        txn_breakdown[t]["total_amount"] += abs(fv["amount"])

    txn_summary = [
        {"type": t, "count": d["count"], "total_amount": round(d["total_amount"], 2)}
        for t, d in sorted(txn_breakdown.items(), key=lambda x: x[1]["count"], reverse=True)
    ]

    # ── 6. Aggregate Summary ─────────────────────────────
    total_invested = sum(abs(fl["closing_balance"]) for fl in fi_ledgers
                         if fl["fi_category"] in ("Equity Holdings", "Investments", "Mutual Funds", "Debentures", "Bonds", "PMS"))
    total_fd = sum(abs(fl["closing_balance"]) for fl in fi_ledgers
                   if fl["fi_category"] == "Fixed Deposits")
    total_gains = sum(abs(fv["amount"]) for fv in fi_vouchers
                      if fv["fi_txn_type"] == "Capital Gain/Loss")
    total_dividends = sum(abs(fv["amount"]) for fv in fi_vouchers
                          if fv["fi_txn_type"] == "Dividend Receipt")
    total_interest = sum(abs(fv["amount"]) for fv in fi_vouchers
                         if fv["fi_txn_type"] == "Interest Receipt")
    total_purchases = sum(abs(fv["amount"]) for fv in fi_vouchers
                          if fv["fi_txn_type"] == "Share Purchase")
    total_sales = sum(abs(fv["amount"]) for fv in fi_vouchers
                      if fv["fi_txn_type"] == "Share Sale")

    summary = {
        "total_invested": round(total_invested, 2),
        "total_fixed_deposits": round(total_fd, 2),
        "total_purchases": round(total_purchases, 2),
        "total_sales": round(total_sales, 2),
        "total_capital_gains": round(total_gains, 2),
        "total_dividends": round(total_dividends, 2),
        "total_interest_income": round(total_interest, 2),
    }

    logger.info(
        f"FI Classification: company={company_name}, "
        f"fi_ledgers={len(fi_ledgers)}/{len(ledgers)}, "
        f"fi_vouchers={len(fi_vouchers)}/{len(vouchers)}"
    )

    return {
        "has_data": True,
        "company_name": company_name,
        "last_sync": last_sync.isoformat() if last_sync else None,
        "total_ledgers": len(ledgers),
        "total_vouchers": len(vouchers),
        "fi_ledger_count": len(fi_ledgers),
        "fi_voucher_count": len(fi_vouchers),
        "non_fi_ledger_count": non_fi_count,
        "non_fi_voucher_count": non_fi_voucher_count,
        "fi_percentage": round(len(fi_ledgers) / max(len(ledgers), 1) * 100, 1),
        "summary": summary,
        "holdings": holdings_summary,
        "transaction_breakdown": txn_summary,
        "fi_ledgers": fi_ledgers,
        "fi_vouchers": fi_vouchers,
    }
