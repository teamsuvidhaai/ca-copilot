"""
Tally Data API — serves ledgers, vouchers, voucher_entries,
and voucher_inventory_entries from the Supabase tables synced
by the Tally connector.
"""
from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, desc, asc, or_

from app.api import deps
from app.models.models import Ledger, Voucher, VoucherEntry, VoucherInventoryEntry, User

router = APIRouter()


# ──────────────────────────────────────────
# LEDGERS
# ──────────────────────────────────────────
@router.get("/ledgers")
async def list_ledgers(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    search: Optional[str] = None,
    parent: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
) -> Any:
    """List ledgers with search/filter/pagination."""
    q = select(Ledger)

    if company_name:
        q = q.where(Ledger.company_name == company_name)
    if parent:
        q = q.where(Ledger.parent == parent)
    if search:
        q = q.where(or_(
            Ledger.name.ilike(f"%{search}%"),
            Ledger.parent.ilike(f"%{search}%"),
            Ledger.party_gstin.ilike(f"%{search}%"),
        ))

    # Count
    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    # Paginate
    q = q.order_by(Ledger.name).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(q)
    ledgers = result.scalars().all()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": -(-total // per_page),  # ceil division
        "data": [
            {
                "id": str(l.id),
                "name": l.name,
                "parent": l.parent or "",
                "company_name": l.company_name,
                "party_gstin": l.party_gstin or "",
                "gst_registration_type": l.gst_registration_type or "",
                "opening_balance": float(l.opening_balance) if l.opening_balance else 0,
                "closing_balance": float(l.closing_balance) if l.closing_balance else 0,
                "state": l.state or "",
                "email": l.email or "",
                "mobile": l.mobile or "",
                "address": l.address or "",
                "synced_at": l.synced_at.isoformat() if l.synced_at else None,
            }
            for l in ledgers
        ],
    }


@router.get("/ledgers/groups")
async def list_ledger_groups(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Get distinct ledger parent groups."""
    q = select(Ledger.parent, func.count(Ledger.id).label("count")).group_by(Ledger.parent).order_by(Ledger.parent)
    result = await db.execute(q)
    return [{"group": row[0] or "Ungrouped", "count": row[1]} for row in result]


@router.get("/ledgers/stats")
async def ledger_stats(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
) -> Any:
    """Summary stats for the dashboard, optionally filtered by company."""
    base = select(func.count(Ledger.id))
    if company_name:
        base = base.where(Ledger.company_name == company_name)

    total = (await db.execute(base)).scalar() or 0

    gstin_q = select(func.count(Ledger.id)).where(Ledger.party_gstin != None, Ledger.party_gstin != "")
    if company_name:
        gstin_q = gstin_q.where(Ledger.company_name == company_name)
    with_gstin = (await db.execute(gstin_q)).scalar() or 0

    grp_q = select(func.count(func.distinct(Ledger.parent)))
    if company_name:
        grp_q = grp_q.where(Ledger.company_name == company_name)
    groups = (await db.execute(grp_q)).scalar() or 0

    companies = (await db.execute(
        select(func.count(func.distinct(Ledger.company_name)))
    )).scalar() or 0

    return {"total_ledgers": total, "with_gstin": with_gstin, "groups": groups, "companies": companies}


# ──────────────────────────────────────────
# VOUCHERS
# ──────────────────────────────────────────
@router.get("/vouchers")
async def list_vouchers(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    voucher_type: Optional[str] = None,
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
) -> Any:
    """List vouchers with filters. Includes entry_count for each voucher."""
    q = select(Voucher)
    if company_name:
        q = q.where(Voucher.company_name == company_name)
    if voucher_type:
        q = q.where(Voucher.voucher_type == voucher_type)
    if search:
        q = q.where(or_(
            Voucher.party_name.ilike(f"%{search}%"),
            Voucher.voucher_number.ilike(f"%{search}%"),
        ))

    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    q = q.order_by(desc(Voucher.date)).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(q)
    vouchers = result.scalars().all()

    # Batch-load entry counts for all GUIDs on this page
    guids = [v.guid for v in vouchers]
    entry_counts = {}
    if guids:
        ec_q = (
            select(VoucherEntry.voucher_guid, func.count(VoucherEntry.id))
            .where(VoucherEntry.voucher_guid.in_(guids))
            .group_by(VoucherEntry.voucher_guid)
        )
        ec_result = await db.execute(ec_q)
        entry_counts = {row[0]: row[1] for row in ec_result}

    return {
        "total": total, "page": page, "per_page": per_page,
        "total_pages": -(-total // per_page),
        "data": [
            {
                "id": str(v.id),
                "company_name": v.company_name,
                "date": v.date,
                "voucher_type": v.voucher_type or "",
                "voucher_number": v.voucher_number or "",
                "party_name": v.party_name or "",
                "amount": float(v.amount) if v.amount else 0,
                "narration": v.narration or "",
                "guid": v.guid,
                "entry_count": entry_counts.get(v.guid, 0),
                "synced_at": v.synced_at.isoformat() if v.synced_at else None,
            }
            for v in vouchers
        ],
    }



@router.get("/vouchers/stats")
async def voucher_stats(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
) -> Any:
    """Dashboard stats for Tally vouchers by type, optionally filtered by company."""
    base = select(func.count(Voucher.id))
    if company_name:
        base = base.where(Voucher.company_name == company_name)
    total = (await db.execute(base)).scalar() or 0

    type_counts = {}
    for vtype in ["Sales", "Purchase", "Payment", "Receipt", "Journal", "Contra"]:
        q = select(func.count(Voucher.id)).where(Voucher.voucher_type == vtype)
        if company_name:
            q = q.where(Voucher.company_name == company_name)
        c = (await db.execute(q)).scalar() or 0
        type_counts[vtype.lower()] = c

    return {"total": total, **type_counts}


@router.get("/vouchers/{guid}")
async def get_voucher_detail(
    guid: str,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Get a single voucher with all its ledger entries and inventory entries."""
    # Fetch voucher
    v_result = await db.execute(select(Voucher).where(Voucher.guid == guid))
    voucher = v_result.scalars().first()
    if not voucher:
        raise HTTPException(status_code=404, detail="Voucher not found")

    # Fetch ledger entries
    le_result = await db.execute(
        select(VoucherEntry)
        .where(VoucherEntry.voucher_guid == guid)
        .order_by(VoucherEntry.is_debit.desc(), VoucherEntry.ledger_name)
    )
    ledger_entries = le_result.scalars().all()

    # Fetch inventory entries
    ie_result = await db.execute(
        select(VoucherInventoryEntry)
        .where(VoucherInventoryEntry.voucher_guid == guid)
        .order_by(VoucherInventoryEntry.stock_item_name)
    )
    inventory_entries = ie_result.scalars().all()

    return {
        "voucher": {
            "id": str(voucher.id),
            "company_name": voucher.company_name,
            "date": voucher.date,
            "voucher_type": voucher.voucher_type or "",
            "voucher_number": voucher.voucher_number or "",
            "party_name": voucher.party_name or "",
            "amount": float(voucher.amount) if voucher.amount else 0,
            "narration": voucher.narration or "",
            "guid": voucher.guid,
            "synced_at": voucher.synced_at.isoformat() if voucher.synced_at else None,
        },
        "ledger_entries": [
            {
                "id": str(e.id),
                "ledger_name": e.ledger_name,
                "amount": float(e.amount) if e.amount else 0,
                "is_debit": e.is_debit,
            }
            for e in ledger_entries
        ],
        "inventory_entries": [
            {
                "id": str(e.id),
                "stock_item_name": e.stock_item_name,
                "quantity": float(e.quantity) if e.quantity else 0,
                "rate": float(e.rate) if e.rate else 0,
                "amount": float(e.amount) if e.amount else 0,
                "uom": e.uom or "",
                "hsn_code": e.hsn_code or "",
                "gst_rate": float(e.gst_rate) if e.gst_rate else 0,
                "godown": e.godown or "",
                "batch": e.batch or "",
                "discount": float(e.discount) if e.discount else 0,
            }
            for e in inventory_entries
        ],
    }


# ──────────────────────────────────────────
# COMPANIES  (distinct company names from synced ledgers)
# ──────────────────────────────────────────
@router.get("/companies")
async def list_companies(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Return distinct Tally company names from synced ledgers."""
    q = select(func.distinct(Ledger.company_name)).order_by(Ledger.company_name)
    result = await db.execute(q)
    return [row[0] for row in result if row[0]]


# ──────────────────────────────────────────
# INVENTORY ENTRIES
# ──────────────────────────────────────────
@router.get("/inventory-entries")
async def list_inventory_entries(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    voucher_type: Optional[str] = None,
    stock_item: Optional[str] = None,
    hsn_code: Optional[str] = None,
    godown: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
) -> Any:
    """List inventory entries with filters."""
    q = select(VoucherInventoryEntry)
    if company_name:
        q = q.where(VoucherInventoryEntry.company_name == company_name)
    if voucher_type:
        q = q.where(VoucherInventoryEntry.voucher_type == voucher_type)
    if stock_item:
        q = q.where(VoucherInventoryEntry.stock_item_name == stock_item)
    if hsn_code:
        q = q.where(VoucherInventoryEntry.hsn_code == hsn_code)
    if godown:
        q = q.where(VoucherInventoryEntry.godown == godown)
    if date_from:
        q = q.where(VoucherInventoryEntry.voucher_date >= date_from)
    if date_to:
        q = q.where(VoucherInventoryEntry.voucher_date <= date_to)
    if search:
        q = q.where(or_(
            VoucherInventoryEntry.stock_item_name.ilike(f"%{search}%"),
            VoucherInventoryEntry.hsn_code.ilike(f"%{search}%"),
        ))

    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    q = q.order_by(desc(VoucherInventoryEntry.voucher_date)).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(q)
    entries = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "total_pages": -(-total // per_page),
        "data": [
            {
                "id": str(e.id),
                "company_name": e.company_name,
                "voucher_guid": e.voucher_guid,
                "voucher_date": e.voucher_date,
                "voucher_type": e.voucher_type or "",
                "stock_item_name": e.stock_item_name,
                "quantity": float(e.quantity) if e.quantity else 0,
                "rate": float(e.rate) if e.rate else 0,
                "amount": float(e.amount) if e.amount else 0,
                "uom": e.uom or "",
                "hsn_code": e.hsn_code or "",
                "gst_rate": float(e.gst_rate) if e.gst_rate else 0,
                "godown": e.godown or "",
                "batch": e.batch or "",
                "discount": float(e.discount) if e.discount else 0,
                "synced_at": e.synced_at.isoformat() if e.synced_at else None,
            }
            for e in entries
        ],
    }


@router.get("/inventory-entries/stats")
async def inventory_stats(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
) -> Any:
    """Dashboard stats for inventory entries."""
    base = select(func.count(VoucherInventoryEntry.id))
    if company_name:
        base = base.where(VoucherInventoryEntry.company_name == company_name)
    total = (await db.execute(base)).scalar() or 0

    # Unique stock items
    items_q = select(func.count(func.distinct(VoucherInventoryEntry.stock_item_name)))
    if company_name:
        items_q = items_q.where(VoucherInventoryEntry.company_name == company_name)
    unique_items = (await db.execute(items_q)).scalar() or 0

    # Unique HSN codes
    hsn_q = select(func.count(func.distinct(VoucherInventoryEntry.hsn_code))).where(
        VoucherInventoryEntry.hsn_code != None, VoucherInventoryEntry.hsn_code != ""
    )
    if company_name:
        hsn_q = hsn_q.where(VoucherInventoryEntry.company_name == company_name)
    unique_hsn = (await db.execute(hsn_q)).scalar() or 0

    # Top 5 items by total amount
    top_q = (
        select(
            VoucherInventoryEntry.stock_item_name,
            func.sum(VoucherInventoryEntry.amount).label("total_amount"),
            func.sum(VoucherInventoryEntry.quantity).label("total_qty"),
        )
        .group_by(VoucherInventoryEntry.stock_item_name)
        .order_by(desc("total_amount"))
        .limit(5)
    )
    if company_name:
        top_q = top_q.where(VoucherInventoryEntry.company_name == company_name)
    top_result = await db.execute(top_q)
    top_items = [
        {
            "name": row[0],
            "total_amount": float(row[1]) if row[1] else 0,
            "total_qty": float(row[2]) if row[2] else 0,
        }
        for row in top_result
    ]

    return {
        "total_entries": total,
        "unique_items": unique_items,
        "unique_hsn_codes": unique_hsn,
        "top_items": top_items,
    }


@router.get("/inventory-entries/hsn-summary")
async def inventory_hsn_summary(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    voucher_type: Optional[str] = None,
) -> Any:
    """HSN-wise summary of inventory — useful for GSTR-1 HSN table."""
    q = (
        select(
            VoucherInventoryEntry.hsn_code,
            VoucherInventoryEntry.uom,
            VoucherInventoryEntry.gst_rate,
            func.sum(VoucherInventoryEntry.quantity).label("total_qty"),
            func.sum(VoucherInventoryEntry.amount).label("total_amount"),
            func.count(VoucherInventoryEntry.id).label("entry_count"),
        )
        .where(VoucherInventoryEntry.hsn_code != None, VoucherInventoryEntry.hsn_code != "")
        .group_by(
            VoucherInventoryEntry.hsn_code,
            VoucherInventoryEntry.uom,
            VoucherInventoryEntry.gst_rate,
        )
        .order_by(desc("total_amount"))
    )
    if company_name:
        q = q.where(VoucherInventoryEntry.company_name == company_name)
    if voucher_type:
        q = q.where(VoucherInventoryEntry.voucher_type == voucher_type)

    result = await db.execute(q)
    return [
        {
            "hsn_code": row[0] or "",
            "uom": row[1] or "",
            "gst_rate": float(row[2]) if row[2] else 0,
            "total_qty": float(row[3]) if row[3] else 0,
            "total_amount": float(row[4]) if row[4] else 0,
            "entry_count": row[5],
        }
        for row in result
    ]
