"""
Financial Instruments — Tally Classification API
─────────────────────────────────────────────────
Identifies which ledgers and vouchers from synced Tally data
are Financial Instruments (shares, FDs, MFs, loans, etc.)

Endpoints:
  GET  /fi-tally/classify          — full FI classification for a company
  GET  /fi-tally/holdings          — holdings-only view
  GET  /fi-tally/transactions      — FI transactions with filters
  GET  /fi-tally/summary           — quick summary stats
  GET  /fi-tally/dividends         — dividend tracker
  GET  /fi-tally/capital-gains     — capital gain/loss breakdown
"""

from typing import Any, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import deps
from app.models.models import User
from app.services.fi_classifier import classify_company_fi, classify_narration

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/classify")
async def classify_fi(
    company_name: str = Query(..., description="Tally company name"),
    date_from: str = Query(None, description="YYYYMMDD — filter vouchers from this date"),
    date_to: str = Query(None, description="YYYYMMDD — filter vouchers up to this date"),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Full Financial Instrument classification for a synced Tally company.
    Returns ledgers, vouchers, holdings, transactions, and summary stats.
    Optionally filter vouchers by date range (ledger balances always returned)."""
    result = await classify_company_fi(db, company_name, date_from=date_from, date_to=date_to)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))
    return result


@router.get("/holdings")
async def get_fi_holdings(
    company_name: str = Query(...),
    category: Optional[str] = Query(None, description="Filter by FI category"),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Holdings summary — ledgers grouped by FI category with balances."""
    result = await classify_company_fi(db, company_name)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))

    holdings = result["holdings"]
    if category:
        holdings = [h for h in holdings if h["category"].lower() == category.lower()]

    return {
        "company_name": company_name,
        "total_fi_ledgers": result["fi_ledger_count"],
        "holdings": holdings,
        "summary": result["summary"],
    }


@router.get("/transactions")
async def get_fi_transactions(
    company_name: str = Query(...),
    txn_type: Optional[str] = Query(None, description="Filter: Share Purchase, Dividend Receipt, etc."),
    date_from: Optional[str] = Query(None, description="YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="YYYYMMDD"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """FI transaction list with optional filters and pagination."""
    result = await classify_company_fi(db, company_name)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))

    txns = result["fi_vouchers"]

    # Apply filters
    if txn_type:
        txns = [t for t in txns if t["fi_txn_type"].lower() == txn_type.lower()]
    if date_from:
        txns = [t for t in txns if (t["date"] or "") >= date_from]
    if date_to:
        txns = [t for t in txns if (t["date"] or "") <= date_to]

    total = len(txns)
    start = (page - 1) * per_page
    paged = txns[start:start + per_page]

    return {
        "company_name": company_name,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": -(-total // per_page),
        "transaction_breakdown": result["transaction_breakdown"],
        "data": paged,
    }


@router.get("/summary")
async def get_fi_summary(
    company_name: str = Query(...),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Quick FI summary — counts and totals without full ledger/voucher lists."""
    result = await classify_company_fi(db, company_name)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))

    return {
        "company_name": company_name,
        "fi_ledger_count": result["fi_ledger_count"],
        "fi_voucher_count": result["fi_voucher_count"],
        "fi_percentage": result["fi_percentage"],
        "total_ledgers": result["total_ledgers"],
        "total_vouchers": result["total_vouchers"],
        "summary": result["summary"],
        "transaction_breakdown": result["transaction_breakdown"],
        "category_counts": [
            {"category": h["category"], "count": h["count"], "active": h["active_count"],
             "total_closing": h["total_closing"]}
            for h in result["holdings"]
        ],
    }


@router.get("/dividends")
async def get_fi_dividends(
    company_name: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Dividend tracker — all dividend receipts with scrip details."""
    result = await classify_company_fi(db, company_name)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))

    dividends = [t for t in result["fi_vouchers"] if t["fi_txn_type"] == "Dividend Receipt"]
    if date_from:
        dividends = [d for d in dividends if (d["date"] or "") >= date_from]
    if date_to:
        dividends = [d for d in dividends if (d["date"] or "") <= date_to]

    total_amount = sum(abs(d["amount"]) for d in dividends)

    # Group by scrip
    by_scrip = {}
    for d in dividends:
        scrip = d["scrip"] or "Unknown"
        by_scrip.setdefault(scrip, {"count": 0, "total": 0})
        by_scrip[scrip]["count"] += 1
        by_scrip[scrip]["total"] += abs(d["amount"])

    scrip_summary = [
        {"scrip": s, "count": v["count"], "total_amount": round(v["total"], 2)}
        for s, v in sorted(by_scrip.items(), key=lambda x: x[1]["total"], reverse=True)
    ]

    return {
        "company_name": company_name,
        "total_dividends": len(dividends),
        "total_amount": round(total_amount, 2),
        "by_scrip": scrip_summary,
        "data": dividends,
    }


@router.get("/capital-gains")
async def get_fi_capital_gains(
    company_name: str = Query(...),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Capital gain/loss breakdown from share transfer vouchers."""
    result = await classify_company_fi(db, company_name)
    if not result.get("has_data"):
        raise HTTPException(404, result.get("error", "No data found"))

    gains = [t for t in result["fi_vouchers"] if t["fi_txn_type"] == "Capital Gain/Loss"]
    if date_from:
        gains = [g for g in gains if (g["date"] or "") >= date_from]
    if date_to:
        gains = [g for g in gains if (g["date"] or "") <= date_to]

    total_amount = sum(abs(g["amount"]) for g in gains)

    # Monthly breakdown
    monthly = {}
    for g in gains:
        month = (g["date"] or "")[:6]  # YYYYMM
        if month:
            monthly.setdefault(month, {"count": 0, "total": 0})
            monthly[month]["count"] += 1
            monthly[month]["total"] += abs(g["amount"])

    monthly_summary = [
        {"month": m, "count": v["count"], "total_amount": round(v["total"], 2)}
        for m, v in sorted(monthly.items())
    ]

    return {
        "company_name": company_name,
        "total_entries": len(gains),
        "total_amount": round(total_amount, 2),
        "monthly_breakdown": monthly_summary,
        "data": gains,
    }
