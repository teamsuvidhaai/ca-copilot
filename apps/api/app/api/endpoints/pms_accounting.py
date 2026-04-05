"""
PMS Accounting API
──────────────────
Endpoints for PMS account management, opening balances, FIFO lot tracking,
capital gains computation, and deterministic journal entry generation.
Sits alongside the existing financial_instruments.py (which handles Demat/MF).
"""

import json
import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import deps
from app.core.config import settings
from app.models.models import (
    User, FinancialInstrumentUpload, PMSAccount, SecurityMaster,
    PMSTransaction, FIFOLot, CapitalGainMatch, PMSDividend, PMSExpense,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
# Pydantic schemas
# ═══════════════════════════════════════════════════════

class PMSAccountCreate(BaseModel):
    client_id: str
    provider_name: str
    strategy_name: Optional[str] = None
    account_code: Optional[str] = None
    pms_start_date: Optional[str] = None  # YYYY-MM-DD
    accrual_mode: str = "quarterly_actual"  # "daily" or "quarterly_actual"

class PMSAccountOut(BaseModel):
    id: str
    client_id: str
    provider_name: str
    strategy_name: Optional[str] = None
    account_code: Optional[str] = None
    pms_start_date: Optional[str] = None
    is_active: bool
    config: dict = {}
    created_at: Optional[str] = None

class OpeningBalanceItem(BaseModel):
    security_name: str
    isin: Optional[str] = None
    quantity: float
    cost_per_unit: float
    purchase_date: str  # YYYY-MM-DD

class OpeningBalanceRequest(BaseModel):
    pms_account_id: str
    balances: List[OpeningBalanceItem]


# ═══════════════════════════════════════════════════════
# PMS Account CRUD
# ═══════════════════════════════════════════════════════

@router.post("/accounts")
async def create_pms_account(
    body: PMSAccountCreate,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Create a new PMS account for a client."""
    # Check for existing
    existing = (await db.execute(
        select(PMSAccount).where(
            PMSAccount.client_id == body.client_id,
            PMSAccount.provider_name == body.provider_name,
            PMSAccount.strategy_name == body.strategy_name,
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(409, f"PMS account already exists for {body.provider_name} - {body.strategy_name or 'default'}")

    pms_date = None
    if body.pms_start_date:
        try:
            pms_date = date.fromisoformat(body.pms_start_date)
        except ValueError:
            pass

    account = PMSAccount(
        id=str(uuid.uuid4()),
        client_id=body.client_id,
        provider_name=body.provider_name,
        strategy_name=body.strategy_name,
        account_code=body.account_code,
        pms_start_date=pms_date,
        config={"accrual_mode": body.accrual_mode},
    )
    db.add(account)
    await db.commit()
    return {"id": str(account.id), "message": f"PMS account created: {body.provider_name}"}


@router.get("/accounts")
async def list_pms_accounts(
    client_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """List all PMS accounts for a client."""
    rows = (await db.execute(
        select(PMSAccount).where(PMSAccount.client_id == client_id).order_by(PMSAccount.created_at.desc())
    )).scalars().all()
    return [
        {
            "id": str(r.id),
            "client_id": str(r.client_id),
            "provider_name": r.provider_name,
            "strategy_name": r.strategy_name,
            "account_code": r.account_code,
            "pms_start_date": r.pms_start_date.isoformat() if r.pms_start_date else None,
            "is_active": r.is_active,
            "config": r.config or {},
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


@router.delete("/accounts/{account_id}")
async def delete_pms_account(
    account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Delete a PMS account and all its data (transactions, lots, dividends, expenses)."""
    row = (await db.execute(
        select(PMSAccount).where(PMSAccount.id == account_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "PMS account not found")
    await db.delete(row)
    await db.commit()
    return {"message": "PMS account deleted"}


# ═══════════════════════════════════════════════════════
# Opening Balances
# ═══════════════════════════════════════════════════════

@router.post("/opening-balances")
async def set_opening_balances(
    body: OpeningBalanceRequest,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Set opening balance lots for a PMS account (one-time setup)."""
    # Verify account exists
    account = (await db.execute(
        select(PMSAccount).where(PMSAccount.id == body.pms_account_id)
    )).scalar_one_or_none()
    if not account:
        raise HTTPException(404, "PMS account not found")

    # Delete existing opening lots (re-upload scenario)
    await db.execute(
        delete(FIFOLot).where(
            FIFOLot.pms_account_id == body.pms_account_id,
            FIFOLot.is_opening == True,
        )
    )

    created = 0
    for item in body.balances:
        if item.quantity <= 0 or item.cost_per_unit <= 0:
            continue

        # Resolve or create security
        security_id = await _resolve_security(db, item.security_name, item.isin)

        try:
            purchase_date = date.fromisoformat(item.purchase_date)
        except ValueError:
            purchase_date = date.today()

        lot = FIFOLot(
            id=str(uuid.uuid4()),
            pms_account_id=body.pms_account_id,
            security_id=security_id,
            security_name=item.security_name,
            purchase_date=purchase_date,
            original_qty=Decimal(str(item.quantity)),
            remaining_qty=Decimal(str(item.quantity)),
            cost_per_unit=Decimal(str(item.cost_per_unit)),
            total_cost=Decimal(str(item.quantity * item.cost_per_unit)),
            is_opening=True,
        )
        db.add(lot)
        created += 1

    await db.commit()
    return {"message": f"{created} opening balance lots created", "count": created}


@router.get("/opening-balances")
async def get_opening_balances(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get opening balance lots for a PMS account."""
    lots = (await db.execute(
        select(FIFOLot).where(
            FIFOLot.pms_account_id == pms_account_id,
            FIFOLot.is_opening == True,
        ).order_by(FIFOLot.security_name)
    )).scalars().all()
    return [
        {
            "id": str(l.id),
            "security_name": l.security_name,
            "quantity": float(l.original_qty),
            "cost_per_unit": float(l.cost_per_unit),
            "purchase_date": l.purchase_date.isoformat() if l.purchase_date else None,
        }
        for l in lots
    ]


# ═══════════════════════════════════════════════════════
# Security Master helpers
# ═══════════════════════════════════════════════════════

async def _resolve_security(db: AsyncSession, name: str, isin: Optional[str] = None) -> Optional[str]:
    """Find or create a security in the master table. Returns security ID."""
    if isin:
        existing = (await db.execute(
            select(SecurityMaster).where(SecurityMaster.isin == isin)
        )).scalar_one_or_none()
        if existing:
            # Add name as alias if not already there
            aliases = existing.aliases or []
            if name not in aliases and name != existing.name:
                aliases.append(name)
                existing.aliases = aliases
            return str(existing.id)

    # Fuzzy match: try exact name first
    existing = (await db.execute(
        select(SecurityMaster).where(
            func.lower(SecurityMaster.name) == func.lower(name)
        )
    )).scalar_one_or_none()
    if existing:
        return str(existing.id)

    # Create new
    sec = SecurityMaster(
        id=str(uuid.uuid4()),
        isin=isin,
        name=name,
        aliases=[],
    )
    db.add(sec)
    await db.flush()  # get the ID without committing
    return str(sec.id)


# ═══════════════════════════════════════════════════════
# FIFO Engine
# ═══════════════════════════════════════════════════════

async def process_fifo_for_account(db: AsyncSession, pms_account_id: str):
    """Re-compute FIFO lots and capital gains for an entire PMS account.
    Called after new transactions are imported or opening balances change.

    Algorithm:
    1. Delete all non-opening lots and all gain matches
    2. Get all transactions sorted by date
    3. For each BUY → create a lot
    4. For each SELL → consume oldest lots (FIFO), create gain matches
    """
    logger.info(f"🔄 Running FIFO engine for PMS account {pms_account_id}")

    # Step 1: Clear previous computed data (keep opening lots)
    await db.execute(
        delete(CapitalGainMatch).where(
            CapitalGainMatch.sell_tx_id.in_(
                select(PMSTransaction.id).where(PMSTransaction.pms_account_id == pms_account_id)
            )
        )
    )
    await db.execute(
        delete(FIFOLot).where(
            FIFOLot.pms_account_id == pms_account_id,
            FIFOLot.is_opening == False,
        )
    )
    # Reset remaining_qty on opening lots to original
    opening_lots = (await db.execute(
        select(FIFOLot).where(
            FIFOLot.pms_account_id == pms_account_id,
            FIFOLot.is_opening == True,
        )
    )).scalars().all()
    for lot in opening_lots:
        lot.remaining_qty = lot.original_qty

    await db.flush()

    # Step 2: Get all transactions sorted by date, then by type (BUY before SELL on same day)
    type_order = {"BUY": 0, "BONUS": 1, "SPLIT": 2, "SELL": 3, "DIVIDEND": 4, "TDS_TRANSFER": 5}
    transactions = (await db.execute(
        select(PMSTransaction).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.is_duplicate == False,
        ).order_by(PMSTransaction.tx_date, PMSTransaction.tx_type)
    )).scalars().all()

    # Sort properly (BUY before SELL on same date)
    transactions.sort(key=lambda t: (t.tx_date, type_order.get(t.tx_type, 9)))

    buy_count = 0
    sell_count = 0
    total_gain = Decimal("0")

    for tx in transactions:
        if tx.tx_type == "BUY":
            # Create a new FIFO lot
            qty = Decimal(str(tx.quantity or 0))
            if qty <= 0:
                continue
            settlement = Decimal(str(tx.settlement_amt or 0))
            brokerage = Decimal(str(tx.brokerage or 0))
            stt = Decimal(str(tx.stt or 0))
            cost_per_unit = (abs(settlement) + brokerage + stt) / qty if qty else Decimal("0")

            lot = FIFOLot(
                id=str(uuid.uuid4()),
                pms_account_id=pms_account_id,
                security_id=tx.security_id,
                security_name=tx.security_name,
                purchase_tx_id=str(tx.id),
                purchase_date=tx.tx_date,
                original_qty=qty,
                remaining_qty=qty,
                cost_per_unit=cost_per_unit,
                total_cost=qty * cost_per_unit,
                is_opening=False,
            )
            db.add(lot)
            buy_count += 1

        elif tx.tx_type == "SELL":
            # Consume oldest lots — FIFO
            sell_qty = Decimal(str(abs(tx.quantity or 0)))
            if sell_qty <= 0:
                continue

            settlement = Decimal(str(abs(tx.settlement_amt or 0)))
            sale_price_per_unit = settlement / sell_qty if sell_qty else Decimal("0")

            # Get available lots for this security, oldest first
            lots = (await db.execute(
                select(FIFOLot).where(
                    FIFOLot.pms_account_id == pms_account_id,
                    FIFOLot.security_name == tx.security_name,
                    FIFOLot.remaining_qty > 0,
                ).order_by(FIFOLot.purchase_date.asc(), FIFOLot.created_at.asc())
            )).scalars().all()

            remaining = sell_qty
            for lot in lots:
                if remaining <= 0:
                    break

                consumed = min(remaining, lot.remaining_qty)

                # Section 112A grandfathering check
                effective_cost = lot.cost_per_unit
                is_grandfathered = False

                if lot.purchase_date and lot.purchase_date < date(2018, 1, 31):
                    # Try to get FMV from security master
                    if lot.security_id:
                        sec = (await db.execute(
                            select(SecurityMaster).where(SecurityMaster.id == lot.security_id)
                        )).scalar_one_or_none()
                        if sec and sec.fmv_31jan2018:
                            fmv = sec.fmv_31jan2018
                            grandfathered_cost = max(lot.cost_per_unit, fmv)
                            # Cap at sale price (can't have negative gain from grandfathering)
                            effective_cost = min(grandfathered_cost, sale_price_per_unit)
                            is_grandfathered = True

                # Holding period
                holding_days = (tx.tx_date - lot.purchase_date).days if lot.purchase_date else 0
                gain_type = "LTCG" if holding_days > 365 else "STCG"  # 12 months ≈ 365 days for equity

                cost_basis = consumed * effective_cost
                proceeds = consumed * sale_price_per_unit
                gain = proceeds - cost_basis

                match = CapitalGainMatch(
                    id=str(uuid.uuid4()),
                    sell_tx_id=str(tx.id),
                    lot_id=str(lot.id),
                    qty_consumed=consumed,
                    cost_basis=cost_basis,
                    sale_proceeds=proceeds,
                    gain_loss=gain,
                    holding_days=holding_days,
                    gain_type=gain_type,
                    is_grandfathered=is_grandfathered,
                    effective_cost_per_unit=effective_cost,
                )
                db.add(match)

                lot.remaining_qty -= consumed
                remaining -= consumed
                total_gain += gain

            sell_count += 1

            if remaining > 0:
                logger.warning(
                    f"⚠ FIFO shortfall: {remaining} units of {tx.security_name} "
                    f"on {tx.tx_date} — no lots available"
                )

    await db.commit()
    logger.info(
        f"✅ FIFO complete: {buy_count} buys, {sell_count} sells, "
        f"net gain: ₹{total_gain:,.2f}"
    )
    return {
        "buys_processed": buy_count,
        "sells_processed": sell_count,
        "net_gain": float(total_gain),
    }


@router.post("/run-fifo")
async def run_fifo_engine(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Trigger FIFO re-computation for a PMS account."""
    account = (await db.execute(
        select(PMSAccount).where(PMSAccount.id == pms_account_id)
    )).scalar_one_or_none()
    if not account:
        raise HTTPException(404, "PMS account not found")

    result = await process_fifo_for_account(db, pms_account_id)
    return {"message": "FIFO computation complete", **result}


# ═══════════════════════════════════════════════════════
# Stock Register (Unit Ledger)
# ═══════════════════════════════════════════════════════

@router.get("/stock-register")
async def get_stock_register(
    pms_account_id: str,
    fy_start: Optional[str] = None,  # YYYY-MM-DD  e.g. 2025-04-01
    fy_end: Optional[str] = None,    # YYYY-MM-DD  e.g. 2026-03-31
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get the unit ledger / stock register for a PMS account.
    Shows per-security: opening + purchases - sales = closing.

    If fy_start/fy_end are provided, the register is scoped to that
    financial year:
      - Opening  = lots purchased BEFORE fy_start (remaining as of fy_start)
      - Purchases = BUY lots with purchase_date within [fy_start, fy_end]
      - Sales     = SELL transactions within [fy_start, fy_end]
      - Closing   = Opening + Purchases - Sales
    """

    # Parse FY dates (optional)
    fy_start_date = None
    fy_end_date = None
    if fy_start:
        try:
            fy_start_date = date.fromisoformat(fy_start)
        except ValueError:
            raise HTTPException(400, "Invalid fy_start date format. Use YYYY-MM-DD.")
    if fy_end:
        try:
            fy_end_date = date.fromisoformat(fy_end)
        except ValueError:
            raise HTTPException(400, "Invalid fy_end date format. Use YYYY-MM-DD.")

    # Get all lots (both opening and purchase)
    lots = (await db.execute(
        select(FIFOLot).where(FIFOLot.pms_account_id == pms_account_id)
        .order_by(FIFOLot.security_name, FIFOLot.purchase_date)
    )).scalars().all()

    # Get all sell transactions
    sell_query = select(PMSTransaction).where(
        PMSTransaction.pms_account_id == pms_account_id,
        PMSTransaction.tx_type == "SELL",
        PMSTransaction.is_duplicate == False,
    )
    if fy_start_date:
        sell_query = sell_query.where(PMSTransaction.tx_date >= fy_start_date)
    if fy_end_date:
        sell_query = sell_query.where(PMSTransaction.tx_date <= fy_end_date)
    sell_txns = (await db.execute(
        sell_query.order_by(PMSTransaction.tx_date)
    )).scalars().all()

    # Get gain matches for the filtered sell transactions
    all_matches = []
    if sell_txns:
        sell_ids = [str(t.id) for t in sell_txns]
        all_matches = (await db.execute(
            select(CapitalGainMatch).where(
                CapitalGainMatch.sell_tx_id.in_(sell_ids)
            )
        )).scalars().all()

    # Build per-security register
    securities = {}

    # Pre-fetch security metadata (sector, market cap) from SecurityMaster
    security_ids = list(set(str(lot.security_id) for lot in lots if lot.security_id))
    sec_meta_map = {}  # security_id → {sector, market_cap_category}
    if security_ids:
        sec_rows = (await db.execute(
            select(SecurityMaster).where(SecurityMaster.id.in_(security_ids))
        )).scalars().all()
        for s in sec_rows:
            sec_meta_map[str(s.id)] = {
                "sector": s.sector,
                "market_cap_category": s.market_cap_category,
            }

    for lot in lots:
        name = lot.security_name
        if name not in securities:
            # Look up sector/market_cap from pre-fetched metadata
            meta = sec_meta_map.get(str(lot.security_id), {}) if lot.security_id else {}
            securities[name] = {
                "security_name": name,
                "sector": meta.get("sector"),
                "market_cap_category": meta.get("market_cap_category"),
                "opening": {"qty": 0, "value": 0},
                "purchases": [],
                "sales": [],
                "closing": {"qty": 0, "avg_cost": 0, "book_value": 0},
                "remaining_lots": [],
            }
        sec = securities[name]

        # Classify lot as opening vs purchase based on FY dates
        is_opening_for_fy = lot.is_opening
        if fy_start_date and lot.purchase_date and not lot.is_opening:
            # Lot purchased BEFORE FY start → treat as opening balance for this FY
            if lot.purchase_date < fy_start_date:
                is_opening_for_fy = True
            # Lot purchased AFTER FY end → skip entirely
            elif fy_end_date and lot.purchase_date > fy_end_date:
                continue

        if is_opening_for_fy:
            sec["opening"]["qty"] += float(lot.original_qty)
            sec["opening"]["value"] += float(lot.total_cost or 0)
        else:
            sec["purchases"].append({
                "date": lot.purchase_date.isoformat() if lot.purchase_date else None,
                "qty": float(lot.original_qty),
                "rate": float(lot.cost_per_unit),
                "value": float(lot.total_cost or 0),
            })

        # Track remaining lots for drill-down (lots that still have inventory)
        if float(lot.remaining_qty) > 0:
            sec["remaining_lots"].append({
                "id": str(lot.id),
                "purchase_date": lot.purchase_date.isoformat() if lot.purchase_date else None,
                "original_qty": float(lot.original_qty),
                "remaining_qty": float(lot.remaining_qty),
                "cost_per_unit": float(lot.cost_per_unit),
                "book_value": round(float(lot.remaining_qty) * float(lot.cost_per_unit), 2),
                "is_opening": lot.is_opening,
            })

    # Add sales data from gain matches
    sell_tx_map = {str(t.id): t for t in sell_txns}
    for match in all_matches:
        tx = sell_tx_map.get(str(match.sell_tx_id))
        if not tx:
            continue
        name = tx.security_name
        if name not in securities:
            continue
        securities[name]["sales"].append({
            "date": tx.tx_date.isoformat() if tx.tx_date else None,
            "qty": float(match.qty_consumed),
            "cost_basis": float(match.cost_basis),
            "sale_proceeds": float(match.sale_proceeds),
            "gain_loss": float(match.gain_loss),
            "gain_type": match.gain_type,
            "holding_days": match.holding_days,
        })

    # Compute closing balances using actual remaining lot costs
    totals = {"opening_qty": 0, "opening_value": 0, "bought_qty": 0, "bought_value": 0,
              "sold_qty": 0, "sold_cost_basis": 0, "closing_qty": 0, "closing_book_value": 0}

    for name, sec in securities.items():
        bought_qty = sum(p["qty"] for p in sec["purchases"])
        bought_value = sum(p["value"] for p in sec["purchases"])
        sold_qty = sum(s["qty"] for s in sec["sales"])
        sold_cost_basis = sum(s["cost_basis"] for s in sec["sales"])

        closing_qty = sec["opening"]["qty"] + bought_qty - sold_qty
        # Book value = sum of remaining lots' individual costs (true FIFO cost)
        closing_book_value = sum(lot["book_value"] for lot in sec["remaining_lots"])

        sec["bought_qty"] = round(bought_qty, 6)
        sec["bought_value"] = round(bought_value, 2)
        sec["sold_qty"] = round(sold_qty, 6)
        sec["sold_cost_basis"] = round(sold_cost_basis, 2)
        sec["closing"]["qty"] = round(closing_qty, 6)
        sec["closing"]["book_value"] = round(closing_book_value, 2)
        sec["closing"]["avg_cost"] = round(closing_book_value / closing_qty, 4) if closing_qty > 0 else 0

        # Accumulate totals
        totals["opening_qty"] += sec["opening"]["qty"]
        totals["opening_value"] += sec["opening"]["value"]
        totals["bought_qty"] += bought_qty
        totals["bought_value"] += bought_value
        totals["sold_qty"] += sold_qty
        totals["sold_cost_basis"] += sold_cost_basis
        totals["closing_qty"] += closing_qty
        totals["closing_book_value"] += closing_book_value

    # Round totals
    for k in totals:
        totals[k] = round(totals[k], 2 if 'value' in k or 'cost' in k or 'book' in k else 6)

    return {
        "securities": list(securities.values()),
        "totals": totals,
        "fy_start": fy_start,
        "fy_end": fy_end,
    }


# ═══════════════════════════════════════════════════════
# Capital Gains Report
# ═══════════════════════════════════════════════════════

@router.get("/capital-gains")
async def get_capital_gains(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get lot-wise capital gains breakdown for a PMS account."""

    # Get all sell transactions
    sell_txns = (await db.execute(
        select(PMSTransaction).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.tx_type == "SELL",
            PMSTransaction.is_duplicate == False,
        ).order_by(PMSTransaction.tx_date)
    )).scalars().all()

    if not sell_txns:
        return {"entries": [], "summary": {"stcg": 0, "ltcg": 0, "total": 0}}

    # Get all gain matches
    sell_ids = [str(t.id) for t in sell_txns]
    matches = (await db.execute(
        select(CapitalGainMatch).where(
            CapitalGainMatch.sell_tx_id.in_(sell_ids)
        ).order_by(CapitalGainMatch.created_at)
    )).scalars().all()

    # Get lot details for purchase dates
    lot_ids = list(set(str(m.lot_id) for m in matches))
    lots_map = {}
    if lot_ids:
        lots = (await db.execute(
            select(FIFOLot).where(FIFOLot.id.in_(lot_ids))
        )).scalars().all()
        lots_map = {str(l.id): l for l in lots}

    sell_tx_map = {str(t.id): t for t in sell_txns}

    entries = []
    total_stcg = Decimal("0")
    total_ltcg = Decimal("0")

    for match in matches:
        tx = sell_tx_map.get(str(match.sell_tx_id))
        lot = lots_map.get(str(match.lot_id))
        entries.append({
            "security_name": tx.security_name if tx else "Unknown",
            "sale_date": tx.tx_date.isoformat() if tx and tx.tx_date else None,
            "purchase_date": lot.purchase_date.isoformat() if lot and lot.purchase_date else None,
            "qty": float(match.qty_consumed),
            "cost_per_unit": float(match.effective_cost_per_unit or (lot.cost_per_unit if lot else 0)),
            "sale_price_per_unit": float(match.sale_proceeds / match.qty_consumed) if match.qty_consumed else 0,
            "cost_basis": float(match.cost_basis),
            "sale_proceeds": float(match.sale_proceeds),
            "gain_loss": float(match.gain_loss),
            "holding_days": match.holding_days,
            "gain_type": match.gain_type,
            "is_grandfathered": match.is_grandfathered,
        })

        if match.gain_type == "STCG":
            total_stcg += match.gain_loss
        else:
            total_ltcg += match.gain_loss

    return {
        "entries": entries,
        "summary": {
            "stcg": float(total_stcg),
            "ltcg": float(total_ltcg),
            "total": float(total_stcg + total_ltcg),
            "stcg_tax": float(max(Decimal("0"), total_stcg) * Decimal("0.20")),  # 20% STCG
            "ltcg_tax": float(max(Decimal("0"), total_ltcg - Decimal("125000")) * Decimal("0.125")),  # 12.5% above ₹1.25L
        },
    }


# ═══════════════════════════════════════════════════════
# Transactions / Dividends / Expenses — List & Manage
# ═══════════════════════════════════════════════════════

@router.get("/transactions")
async def get_pms_transactions(
    pms_account_id: str,
    tx_type: Optional[str] = None,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """List all transactions for a PMS account."""
    query = select(PMSTransaction).where(
        PMSTransaction.pms_account_id == pms_account_id
    )
    if tx_type:
        query = query.where(PMSTransaction.tx_type == tx_type.upper())
    query = query.order_by(PMSTransaction.tx_date.desc())

    rows = (await db.execute(query)).scalars().all()
    return [
        {
            "id": str(r.id),
            "tx_date": r.tx_date.isoformat() if r.tx_date else None,
            "tx_type": r.tx_type,
            "security_name": r.security_name,
            "exchange": r.exchange,
            "quantity": float(r.quantity) if r.quantity else None,
            "unit_price": float(r.unit_price) if r.unit_price else None,
            "brokerage": float(r.brokerage) if r.brokerage else 0,
            "stt": float(r.stt) if r.stt else 0,
            "stamp_duty": float(r.stamp_duty) if r.stamp_duty else 0,
            "settlement_amt": float(r.settlement_amt) if r.settlement_amt else None,
            "narration": r.narration,
            "is_duplicate": r.is_duplicate,
            "je_status": r.je_status,
        }
        for r in rows
    ]


@router.get("/dividends")
async def get_pms_dividends(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """List all dividends for a PMS account."""
    rows = (await db.execute(
        select(PMSDividend).where(PMSDividend.pms_account_id == pms_account_id)
        .order_by(PMSDividend.ex_date.desc())
    )).scalars().all()
    return [
        {
            "id": str(r.id),
            "security_name": r.security_name,
            "ex_date": r.ex_date.isoformat() if r.ex_date else None,
            "received_date": r.received_date.isoformat() if r.received_date else None,
            "quantity": float(r.quantity) if r.quantity else None,
            "rate_per_share": float(r.rate_per_share) if r.rate_per_share else None,
            "gross_amount": float(r.gross_amount),
            "tds_deducted": float(r.tds_deducted) if r.tds_deducted else 0,
            "net_received": float(r.net_received) if r.net_received else None,
            "je_status": r.je_status,
        }
        for r in rows
    ]


@router.get("/expenses")
async def get_pms_expenses(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """List all expenses for a PMS account."""
    rows = (await db.execute(
        select(PMSExpense).where(PMSExpense.pms_account_id == pms_account_id)
        .order_by(PMSExpense.expense_date.desc().nullslast())
    )).scalars().all()
    return [
        {
            "id": str(r.id),
            "expense_type": r.expense_type,
            "expense_date": r.expense_date.isoformat() if r.expense_date else None,
            "period_from": r.period_from.isoformat() if r.period_from else None,
            "period_to": r.period_to.isoformat() if r.period_to else None,
            "amount": float(r.amount),
            "gst_amount": float(r.gst_amount) if r.gst_amount else 0,
            "tds_applicable": float(r.tds_applicable) if r.tds_applicable else 0,
            "net_payable": float(r.net_payable) if r.net_payable else None,
            "is_paid": r.is_paid,
            "is_accrual": r.is_accrual,
            "is_stt_recon_only": r.is_stt_recon_only,
            "je_status": r.je_status,
        }
        for r in rows
    ]


# ═══════════════════════════════════════════════════════
# Journal Entry Generation (Deterministic)
# ═══════════════════════════════════════════════════════

@router.get("/journal-entries-pms")
async def generate_pms_journal_entries(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Generate deterministic journal entries for all PMS transactions.
    Uses exact CA-standard formats (not AI-generated)."""

    account = (await db.execute(
        select(PMSAccount).where(PMSAccount.id == pms_account_id)
    )).scalar_one_or_none()
    if not account:
        raise HTTPException(404, "PMS account not found")

    provider = account.provider_name
    entries = []

    # ── BUY transactions ──
    buys = (await db.execute(
        select(PMSTransaction).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.tx_type == "BUY",
            PMSTransaction.is_duplicate == False,
        ).order_by(PMSTransaction.tx_date)
    )).scalars().all()

    for tx in buys:
        settlement = abs(float(tx.settlement_amt or 0))
        brokerage = float(tx.brokerage or 0)
        stt = float(tx.stt or 0)
        total = settlement

        ledger_entries = [
            {"ledger_name": f"Shares of {tx.security_name}", "amount": round(settlement, 2), "side": "Dr"},
        ]
        if stt > 0:
            ledger_entries.append({"ledger_name": "STT Paid", "amount": round(stt, 2), "side": "Dr"})
        if brokerage > 0:
            ledger_entries.append({"ledger_name": "Brokerage Paid", "amount": round(brokerage, 2), "side": "Dr"})
        ledger_entries.append(
            {"ledger_name": f"PMS Account - {provider}", "amount": round(total + stt + brokerage, 2), "side": "Cr"}
        )

        entries.append({
            "date": tx.tx_date.isoformat() if tx.tx_date else None,
            "voucher_type": "Journal",
            "narration": f"Being purchase of {float(tx.quantity or 0):.4f} shares of {tx.security_name} via {provider} PMS",
            "ledger_entries": ledger_entries,
            "source": "transaction",
            "tx_id": str(tx.id),
        })

    # ── SELL transactions (with FIFO gain/loss) ──
    sells = (await db.execute(
        select(PMSTransaction).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.tx_type == "SELL",
            PMSTransaction.is_duplicate == False,
        ).order_by(PMSTransaction.tx_date)
    )).scalars().all()

    for tx in sells:
        settlement = abs(float(tx.settlement_amt or 0))
        brokerage = float(tx.brokerage or 0)
        stt = float(tx.stt or 0)

        # Get gain matches for this sale
        matches = (await db.execute(
            select(CapitalGainMatch).where(CapitalGainMatch.sell_tx_id == str(tx.id))
        )).scalars().all()

        total_cost_basis = sum(float(m.cost_basis) for m in matches)
        total_gain = sum(float(m.gain_loss) for m in matches)

        ledger_entries = [
            {"ledger_name": f"PMS Account - {provider}", "amount": round(settlement, 2), "side": "Dr"},
        ]
        if stt > 0:
            ledger_entries.append({"ledger_name": "STT Paid", "amount": round(stt, 2), "side": "Dr"})
        if brokerage > 0:
            ledger_entries.append({"ledger_name": "Brokerage Paid", "amount": round(brokerage, 2), "side": "Dr"})

        ledger_entries.append(
            {"ledger_name": f"Shares of {tx.security_name}", "amount": round(total_cost_basis, 2), "side": "Cr"}
        )

        if total_gain >= 0:
            ledger_entries.append(
                {"ledger_name": "Profit on Sale of Shares", "amount": round(total_gain, 2), "side": "Cr"}
            )
        else:
            ledger_entries.append(
                {"ledger_name": "Loss on Sale of Shares", "amount": round(abs(total_gain), 2), "side": "Dr"}
            )

        gain_types = set(m.gain_type for m in matches)
        gain_note = f" ({'/'.join(gain_types)})" if gain_types else ""

        entries.append({
            "date": tx.tx_date.isoformat() if tx.tx_date else None,
            "voucher_type": "Journal",
            "narration": f"Being sale of {abs(float(tx.quantity or 0)):.4f} shares of {tx.security_name}{gain_note} via {provider} PMS",
            "ledger_entries": ledger_entries,
            "source": "transaction",
            "tx_id": str(tx.id),
        })

    # ── Dividend entries ──
    dividends = (await db.execute(
        select(PMSDividend).where(PMSDividend.pms_account_id == pms_account_id)
        .order_by(PMSDividend.received_date)
    )).scalars().all()

    for div in dividends:
        gross = float(div.gross_amount or 0)
        tds = float(div.tds_deducted or 0)
        net = float(div.net_received or (gross - tds))

        ledger_entries = [
            {"ledger_name": f"PMS Account - {provider}", "amount": round(net, 2), "side": "Dr"},
        ]
        if tds > 0:
            ledger_entries.append({"ledger_name": "TDS/TCS Receivable", "amount": round(tds, 2), "side": "Dr"})
        ledger_entries.append(
            {"ledger_name": "Dividend Received", "amount": round(gross, 2), "side": "Cr"}
        )

        entries.append({
            "date": (div.received_date or div.ex_date).isoformat() if (div.received_date or div.ex_date) else None,
            "voucher_type": "Receipt",
            "narration": f"Being dividend received on {div.security_name}, TDS u/s 194 deducted",
            "ledger_entries": ledger_entries,
            "source": "dividend",
            "div_id": str(div.id),
        })

    # ── Expense entries (Paid only, skip recon-only STT) ──
    expenses = (await db.execute(
        select(PMSExpense).where(
            PMSExpense.pms_account_id == pms_account_id,
            PMSExpense.is_stt_recon_only == False,
        ).order_by(PMSExpense.expense_date)
    )).scalars().all()

    for exp in expenses:
        amt = float(exp.amount or 0)
        gst = float(exp.gst_amount or 0)
        tds = float(exp.tds_applicable or 0)
        net = float(exp.net_payable or (amt + gst - tds))

        ledger_entries = [
            {"ledger_name": f"PMS {exp.expense_type}", "amount": round(amt + gst, 2), "side": "Dr"},
        ]
        ledger_entries.append(
            {"ledger_name": f"PMS Account - {provider}", "amount": round(net, 2), "side": "Cr"}
        )
        if tds > 0:
            ledger_entries.append(
                {"ledger_name": "TDS Payable - Professional Fees", "amount": round(tds, 2), "side": "Cr"}
            )

        entries.append({
            "date": exp.expense_date.isoformat() if exp.expense_date else None,
            "voucher_type": "Payment",
            "narration": f"Being {exp.expense_type} {'paid' if exp.is_paid else 'payable'} to {provider} PMS",
            "ledger_entries": ledger_entries,
            "source": "expense",
            "exp_id": str(exp.id),
        })

    return {
        "journal_entries": entries,
        "summary": {
            "total_entries": len(entries),
            "buys": len(buys),
            "sells": len(sells),
            "dividends": len(dividends),
            "expenses": len(expenses),
        },
    }


# ═══════════════════════════════════════════════════════
# Dashboard Summary
# ═══════════════════════════════════════════════════════

@router.get("/dashboard")
async def get_pms_dashboard(
    pms_account_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get aggregated dashboard data for a PMS account."""

    # Transaction counts
    tx_count = (await db.execute(
        select(func.count(PMSTransaction.id)).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.is_duplicate == False,
        )
    )).scalar_one()

    buy_count = (await db.execute(
        select(func.count(PMSTransaction.id)).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.tx_type == "BUY",
        )
    )).scalar_one()

    sell_count = (await db.execute(
        select(func.count(PMSTransaction.id)).where(
            PMSTransaction.pms_account_id == pms_account_id,
            PMSTransaction.tx_type == "SELL",
        )
    )).scalar_one()

    # Holdings value from remaining lots
    lots = (await db.execute(
        select(FIFOLot).where(
            FIFOLot.pms_account_id == pms_account_id,
            FIFOLot.remaining_qty > 0,
        )
    )).scalars().all()

    holdings_cost = sum(float(l.remaining_qty * l.cost_per_unit) for l in lots)
    holdings_qty = sum(float(l.remaining_qty) for l in lots)
    unique_securities = len(set(l.security_name for l in lots))

    # Capital gains totals
    cg_matches = (await db.execute(
        select(CapitalGainMatch).where(
            CapitalGainMatch.sell_tx_id.in_(
                select(PMSTransaction.id).where(PMSTransaction.pms_account_id == pms_account_id)
            )
        )
    )).scalars().all()

    stcg = sum(float(m.gain_loss) for m in cg_matches if m.gain_type == "STCG")
    ltcg = sum(float(m.gain_loss) for m in cg_matches if m.gain_type == "LTCG")

    # Dividend totals
    div_total = (await db.execute(
        select(func.coalesce(func.sum(PMSDividend.gross_amount), 0)).where(
            PMSDividend.pms_account_id == pms_account_id
        )
    )).scalar_one()

    tds_total = (await db.execute(
        select(func.coalesce(func.sum(PMSDividend.tds_deducted), 0)).where(
            PMSDividend.pms_account_id == pms_account_id
        )
    )).scalar_one()

    return {
        "total_transactions": tx_count,
        "buys": buy_count,
        "sells": sell_count,
        "holdings_cost": round(holdings_cost, 2),
        "holdings_securities": unique_securities,
        "stcg": round(stcg, 2),
        "ltcg": round(ltcg, 2),
        "net_gain": round(stcg + ltcg, 2),
        "dividend_total": float(div_total),
        "tds_total": float(tds_total),
    }


# ═══════════════════════════════════════════════════════
# PMS PDF Upload + AI Parsing
# ═══════════════════════════════════════════════════════

PMS_TX_PROMPT = """You are a financial data extraction AI specialising in Indian PMS (Portfolio Management Service) Transaction Statements.

Given the raw text from a PMS Transaction Statement, extract EVERY trade and credit entry.

**Rules:**
1. Extract EVERY row — buys, sells, dividend credits, TDS transfers. Do NOT skip any.
2. Dates: "YYYY-MM-DD". Amounts: plain numbers only (no commas, no ₹).
3. Classify each row:
   - "BUY" for purchases
   - "SELL" for sales/redemptions
   - "DIVIDEND" for dividend credits
   - "TDS_TRANSFER" for TDS deductions/transfers
   - "BONUS" for bonus shares
   - "SPLIT" for stock splits
4. If the PDF has multiple strategies/accounts, detect the headers and tag each transaction with strategy_name.
5. For Buy: settlement_amt is the DEBIT (money going out). For Sell: settlement_amt is the CREDIT (money coming in).
6. Extract brokerage, STT, stamp_duty separately if shown.

Return this exact JSON:
{
    "provider_name": "string",
    "client_name": "string or null",
    "period_from": "YYYY-MM-DD or null",
    "period_to": "YYYY-MM-DD or null",
    "strategies": ["string"],
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "security_name": "string",
            "isin": "string or null",
            "exchange": "NSE or BSE or null",
            "tx_type": "BUY/SELL/DIVIDEND/TDS_TRANSFER/BONUS/SPLIT",
            "quantity": number or null,
            "unit_price": number or null,
            "brokerage": number or null,
            "stt": number or null,
            "stamp_duty": number or null,
            "settlement_amt": number,
            "strategy_name": "string or null",
            "narration": "string or null"
        }
    ]
}"""

PMS_DIV_PROMPT = """You are a financial data extraction AI specialising in Indian PMS Dividend Statements.

Given the raw text from a PMS Dividend Statement, extract EVERY dividend entry.

**Rules:**
1. Extract EVERY dividend row — do not skip any.
2. Dates: "YYYY-MM-DD". Amounts: plain numbers only.
3. Capture ex-date, record date, received date, quantity held, rate per share.
4. Distinguish gross dividend, TDS deducted (u/s 194), and net received.

Return this exact JSON:
{
    "provider_name": "string",
    "period_from": "YYYY-MM-DD or null",
    "period_to": "YYYY-MM-DD or null",
    "dividends": [
        {
            "security_name": "string",
            "isin": "string or null",
            "ex_date": "YYYY-MM-DD or null",
            "received_date": "YYYY-MM-DD or null",
            "quantity": number or null,
            "rate_per_share": number or null,
            "gross_amount": number,
            "tds_deducted": number or null,
            "net_received": number or null
        }
    ]
}"""

PMS_EXP_PROMPT = """You are a financial data extraction AI specialising in Indian PMS Statement of Expenses.

Given the raw text from a PMS Statement of Expenses, extract EVERY expense line item.

**Rules:**
1. Extract EVERY expense — STT, management fees, custody fees, fund accounting fees, stamp duty, SEBI charges, performance fees, GST, exit loads.
2. Dates: "YYYY-MM-DD". Amounts: plain numbers only.
3. Distinguish between "Paid" and "Payable" sections.
4. Flag daily accrual entries (management fee accrual, custody charge accrual) with is_accrual=true.
5. Flag STT entries with is_stt=true (these are reconciliation-only, not booked as separate JE since STT is already in transactions).
6. If TDS is applicable (e.g., on management fees), capture it.

Return this exact JSON:
{
    "provider_name": "string",
    "period_from": "YYYY-MM-DD or null",
    "period_to": "YYYY-MM-DD or null",
    "expenses": [
        {
            "expense_type": "STT/Management Fee/Custody Fee/Fund Accounting Fee/Stamp Duty/SEBI Charges/Performance Fee/GST/Exit Load/Other",
            "expense_date": "YYYY-MM-DD or null",
            "period_from": "YYYY-MM-DD or null",
            "period_to": "YYYY-MM-DD or null",
            "amount": number,
            "gst_amount": number or null,
            "tds_applicable": number or null,
            "net_payable": number or null,
            "is_paid": true or false,
            "is_accrual": true or false,
            "is_stt": true or false,
            "narration": "string or null"
        }
    ]
}"""

import hashlib
import openai

from app.db.session import AsyncSessionLocal


async def _extract_text_for_pms(file_bytes: bytes, filename: str) -> str:
    """Extract text from PDF using LlamaParse (reuses FI pipeline)."""
    from app.api.endpoints.financial_instruments import _extract_text
    return await _extract_text(file_bytes, filename)


async def _parse_pms_with_openai(text: str, statement_type: str) -> dict:
    """Parse extracted text with statement-type-specific prompt.
    For large PDFs, splits into chunks and merges results to capture ALL entries."""
    prompts = {
        "transaction": PMS_TX_PROMPT,
        "dividend": PMS_DIV_PROMPT,
        "expenses": PMS_EXP_PROMPT,
    }
    prompt = prompts.get(statement_type, PMS_TX_PROMPT)
    array_key = {"transaction": "transactions", "dividend": "dividends", "expenses": "expenses"}.get(statement_type, "data")

    # gpt-4o: 128K context window, 16K completion tokens (~60K chars output)
    # 25K input was too large — GPT output exceeded 16K tokens, got truncated
    # 15K input → ~35K JSON output, safely within 16K output token limit
    CHUNK_SIZE = 15000

    if len(text) <= CHUNK_SIZE:
        # Small file — single call
        result = await _call_openai_json(prompt, text)
        return result
    else:
        # Large file — split into chunks, parse ALL in parallel, merge
        chunks = _split_text_smartly(text, CHUNK_SIZE)
        logger.info(f"Large PMS PDF: {len(text)} chars → {len(chunks)} chunks (parallel)")

        # Build prompts for each chunk
        tasks = []
        for i, chunk in enumerate(chunks):
            chunk_prompt = prompt + f"\n\n[NOTE: This is chunk {i+1} of {len(chunks)}. Extract ALL entries from this section.]"
            tasks.append(_call_openai_json(chunk_prompt, chunk))

        # Run ALL chunks in parallel
        import asyncio
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items = []
        metadata = {}
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"  Chunk {i+1}/{len(chunks)} failed: {result}")
                continue
            if i == 0:
                metadata = {k: v for k, v in result.items() if k != array_key}
            items = result.get(array_key, [])
            all_items.extend(items)
            logger.info(f"  Chunk {i+1}/{len(chunks)}: {len(items)} {array_key}")

        logger.info(f"Total from all chunks: {len(all_items)} {array_key}")
        metadata[array_key] = all_items
        return metadata


def _split_text_smartly(text: str, chunk_size: int) -> list:
    """Split text at line boundaries (not mid-row). Looks for page/section breaks."""
    chunks = []
    lines = text.split('\n')
    current_chunk = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1  # +1 for newline
        if current_len + line_len > chunk_size and current_chunk:
            chunks.append('\n'.join(current_chunk))
            current_chunk = []
            current_len = 0
        current_chunk.append(line)
        current_len += line_len

    if current_chunk:
        chunks.append('\n'.join(current_chunk))

    return chunks


async def _call_openai_json(system_prompt: str, user_text: str) -> dict:
    """Single OpenAI call with JSON response + repair on truncation."""
    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=16384,
    )

    raw = response.choices[0].message.content
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON truncated ({len(raw)} chars). Repairing...")
        return _repair_truncated_json(raw)


def _repair_truncated_json(raw: str) -> dict:
    """Repair truncated JSON from GPT (output hit token limit mid-array)."""
    # Strategy: find the last complete JSON object in the array, then close it
    last_good = -1
    brace_depth = 0
    in_string = False
    escape_next = False

    for i, ch in enumerate(raw):
        if escape_next:
            escape_next = False
            continue
        if ch == '\\':
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            brace_depth += 1
        elif ch == '}':
            brace_depth -= 1
            if brace_depth >= 1:  # closed an inner object (array element)
                last_good = i

    if last_good > 0:
        candidate = raw[:last_good + 1].rstrip().rstrip(',') + ']}'
        try:
            result = json.loads(candidate)
            # Find the array key
            for k, v in result.items():
                if isinstance(v, list):
                    logger.info(f"JSON repair: recovered {len(v)} items from key '{k}'")
                    break
            return result
        except json.JSONDecodeError:
            pass

    # Fallback: extract individual {...} objects
    import re
    objects = []
    for match in re.finditer(r'\{[^{}]*\}', raw):
        try:
            objects.append(json.loads(match.group()))
        except json.JSONDecodeError:
            continue

    if objects:
        logger.info(f"JSON repair (regex): recovered {len(objects)} items")
    return {"data": objects}


def _normalize_for_review(parsed: dict, statement_type: str) -> dict:
    """Map PMS AI output fields to fi-review.html expected fields."""
    result = dict(parsed)

    # Header fields
    result["investor_name"] = parsed.get("investor_name") or parsed.get("account_name") or parsed.get("provider_name", "")
    result["pms_provider"] = parsed.get("provider_name", "")
    result["statement_period_start"] = parsed.get("period_from", "")
    result["statement_period_end"] = parsed.get("period_to", "")

    if statement_type == "transaction":
        txns = parsed.get("transactions", [])
        normalized_txns = []
        for t in txns:
            normalized_txns.append({
                **t,
                "scrip_name": t.get("security_name", ""),
                "type": (t.get("tx_type", "Buy") or "Buy").capitalize(),
                "quantity": t.get("quantity"),
                "price": t.get("unit_price"),
                "amount": t.get("settlement_amt"),
                "date": t.get("date", ""),
            })
        result["transactions"] = normalized_txns

    elif statement_type == "dividend":
        divs = parsed.get("dividends", [])
        normalized_divs = []
        for d in divs:
            normalized_divs.append({
                **d,
                "scrip_name": d.get("security_name", ""),
                "date": d.get("received_date") or d.get("ex_date", ""),
                "amount": d.get("gross_amount"),
                "tds_deducted": d.get("tds_deducted"),
            })
        result["dividends"] = normalized_divs

    elif statement_type == "expenses":
        exps = parsed.get("expenses", [])
        normalized_exps = []
        for e in exps:
            normalized_exps.append({
                **e,
                "scrip_name": e.get("expense_type", ""),
                "date": e.get("expense_date", ""),
                "amount": e.get("amount"),
            })
        result["transactions"] = normalized_exps  # show as transactions in review

    return result


async def _process_pms_upload(
    upload_id: str, pms_account_id: str, statement_type: str,
    file_bytes: bytes, filename: str,
):
    """Background: extract → parse → insert into normalized tables."""
    async with AsyncSessionLocal() as db:
        try:
            # Update status
            row = (await db.execute(
                select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
            )).scalar_one()
            row.status = "extracting"
            await db.commit()

            # Extract text
            raw_text = await _extract_text_for_pms(file_bytes, filename)
            row.raw_text = raw_text[:2000]
            row.status = "structuring"
            await db.commit()

            # Parse with AI
            parsed = await _parse_pms_with_openai(raw_text, statement_type)

            # Normalize field names so fi-review.html renders correctly
            normalized = _normalize_for_review(parsed, statement_type)
            row.structured_data = normalized
            row.status = "importing"
            await db.commit()

            count = 0

            if statement_type == "transaction":
                txns = parsed.get("transactions", [])
                for t in txns:
                    try:
                        tx_date = date.fromisoformat(t.get("date", ""))
                    except (ValueError, TypeError):
                        continue

                    security_id = await _resolve_security(
                        db, t.get("security_name", "Unknown"), t.get("isin")
                    )

                    tx = PMSTransaction(
                        id=str(uuid.uuid4()),
                        pms_account_id=pms_account_id,
                        upload_id=upload_id,
                        security_id=security_id,
                        tx_date=tx_date,
                        tx_type=t.get("tx_type", "BUY").upper(),
                        security_name=t.get("security_name", "Unknown"),
                        exchange=t.get("exchange"),
                        quantity=Decimal(str(t.get("quantity", 0) or 0)),
                        unit_price=Decimal(str(t.get("unit_price", 0) or 0)),
                        brokerage=Decimal(str(t.get("brokerage", 0) or 0)),
                        stt=Decimal(str(t.get("stt", 0) or 0)),
                        stamp_duty=Decimal(str(t.get("stamp_duty", 0) or 0)),
                        settlement_amt=Decimal(str(t.get("settlement_amt", 0) or 0)),
                        narration=t.get("narration"),
                    )
                    db.add(tx)
                    count += 1

                await db.commit()

                # Auto-run FIFO after transaction import
                fifo_result = await process_fifo_for_account(db, pms_account_id)
                logger.info(f"FIFO auto-run: {fifo_result}")

            elif statement_type == "dividend":
                divs = parsed.get("dividends", [])
                for d in divs:
                    try:
                        ex_date = date.fromisoformat(d.get("ex_date", "")) if d.get("ex_date") else None
                    except (ValueError, TypeError):
                        ex_date = None
                    try:
                        received_date = date.fromisoformat(d.get("received_date", "")) if d.get("received_date") else None
                    except (ValueError, TypeError):
                        received_date = None

                    security_id = await _resolve_security(
                        db, d.get("security_name", "Unknown"), d.get("isin")
                    )

                    div = PMSDividend(
                        id=str(uuid.uuid4()),
                        pms_account_id=pms_account_id,
                        upload_id=upload_id,
                        security_id=security_id,
                        security_name=d.get("security_name", "Unknown"),
                        ex_date=ex_date,
                        received_date=received_date,
                        quantity=Decimal(str(d.get("quantity", 0) or 0)),
                        rate_per_share=Decimal(str(d.get("rate_per_share", 0) or 0)),
                        gross_amount=Decimal(str(d.get("gross_amount", 0) or 0)),
                        tds_deducted=Decimal(str(d.get("tds_deducted", 0) or 0)),
                        net_received=Decimal(str(d.get("net_received", 0) or 0)),
                    )
                    db.add(div)
                    count += 1

            elif statement_type == "expenses":
                exps = parsed.get("expenses", [])
                for e in exps:
                    try:
                        exp_date = date.fromisoformat(e.get("expense_date", "")) if e.get("expense_date") else None
                    except (ValueError, TypeError):
                        exp_date = None
                    try:
                        period_from = date.fromisoformat(e.get("period_from", "")) if e.get("period_from") else None
                    except (ValueError, TypeError):
                        period_from = None
                    try:
                        period_to = date.fromisoformat(e.get("period_to", "")) if e.get("period_to") else None
                    except (ValueError, TypeError):
                        period_to = None

                    exp = PMSExpense(
                        id=str(uuid.uuid4()),
                        pms_account_id=pms_account_id,
                        upload_id=upload_id,
                        expense_type=e.get("expense_type", "Other"),
                        expense_date=exp_date,
                        period_from=period_from,
                        period_to=period_to,
                        amount=Decimal(str(e.get("amount", 0) or 0)),
                        gst_amount=Decimal(str(e.get("gst_amount", 0) or 0)),
                        tds_applicable=Decimal(str(e.get("tds_applicable", 0) or 0)),
                        net_payable=Decimal(str(e.get("net_payable", 0) or 0)),
                        is_paid=e.get("is_paid", True),
                        is_accrual=e.get("is_accrual", False),
                        is_stt_recon_only=e.get("is_stt", False),
                        narration=e.get("narration"),
                    )
                    db.add(exp)
                    count += 1

            row.journal_entry_count = count
            row.status = "completed"
            await db.commit()
            logger.info(f"✅ PMS Upload {upload_id}: {count} {statement_type} records imported")

        except Exception as e:
            logger.error(f"❌ PMS Upload {upload_id} failed: {e}")
            try:
                row = (await db.execute(
                    select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
                )).scalar_one_or_none()
                if row:
                    row.status = "failed"
                    row.error_message = str(e)[:2000]
                    await db.commit()
            except Exception:
                pass


@router.post("/upload")
async def upload_pms_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_id: str = Form(...),
    pms_account_id: str = Form(...),
    statement_type: str = Form(...),  # transaction, dividend, expenses
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Upload a PMS statement PDF for AI parsing into normalized tables."""

    if statement_type not in ("transaction", "dividend", "expenses"):
        raise HTTPException(400, "statement_type must be: transaction, dividend, or expenses")

    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("pdf", "xlsx", "xls", "csv"):
        raise HTTPException(400, f"Supported formats: PDF, XLSX, XLS, CSV. Got .{ext}")

    file_bytes = await file.read()
    if len(file_bytes) > 25 * 1024 * 1024:
        raise HTTPException(400, "File too large. Max 25 MB.")
    if len(file_bytes) == 0:
        raise HTTPException(400, "File is empty")

    # Verify PMS account exists
    account = (await db.execute(
        select(PMSAccount).where(PMSAccount.id == pms_account_id)
    )).scalar_one_or_none()
    if not account:
        raise HTTPException(404, "PMS account not found")

    # Duplicate detection
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    existing = (await db.execute(
        select(FinancialInstrumentUpload).where(
            FinancialInstrumentUpload.client_id == client_id,
            FinancialInstrumentUpload.file_hash == file_hash,
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(409, f"Duplicate file — already uploaded as \"{existing.filename}\"")

    upload_id = str(uuid.uuid4())
    row = FinancialInstrumentUpload(
        id=upload_id,
        client_id=client_id,
        user_id=str(current_user.id),
        instrument_type=f"pms_{statement_type}",  # pms_transaction, pms_dividend, pms_expenses
        pms_account_id=pms_account_id,
        filename=file.filename,
        file_hash=file_hash,
        status="processing",
    )
    db.add(row)
    await db.commit()

    # Store file to Supabase storage (same bucket as Demat/MF uploads)
    try:
        from app.api.endpoints.financial_instruments import _upload_to_supabase
        _upload_to_supabase(file_bytes, upload_id, file.filename)
    except Exception as e:
        logger.warning(f"PMS file storage failed (non-fatal): {e}")

    background_tasks.add_task(
        _process_pms_upload, upload_id, pms_account_id, statement_type, file_bytes, file.filename
    )

    return {
        "id": upload_id,
        "status": "processing",
        "statement_type": statement_type,
        "message": f"Processing {file.filename} as PMS {statement_type} statement.",
    }

