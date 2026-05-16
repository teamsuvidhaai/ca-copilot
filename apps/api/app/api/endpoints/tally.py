"""
Tally Data API — serves ledgers, vouchers, voucher_entries,
and voucher_inventory_entries from the Supabase tables synced
by the Tally connector.
"""
from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, desc, asc, or_, text

from app.api import deps
from app.models.models import Ledger, Voucher, VoucherEntry, VoucherInventoryEntry, StockItem, User

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
    per_page: int = Query(50, ge=1, le=1000),
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

    # Stock items count (distinct stock item names from inventory entries)
    items_q = select(func.count(func.distinct(VoucherInventoryEntry.stock_item_name)))
    if company_name:
        items_q = items_q.where(VoucherInventoryEntry.company_name == company_name)
    total_items = (await db.execute(items_q)).scalar() or 0

    # Cost centres count (ledgers under Cost Centre parent group)
    cc_q = select(func.count(Ledger.id)).where(
        Ledger.parent.in_(['Cost Centre', 'Cost Centres', 'Primary Cost Centre'])
    )
    if company_name:
        cc_q = cc_q.where(Ledger.company_name == company_name)
    total_cost_centres = (await db.execute(cc_q)).scalar() or 0

    return {
        "total_ledgers": total,
        "with_gstin": with_gstin,
        "total_groups": groups,
        "groups": groups,
        "companies": companies,
        "total_items": total_items,
        "total_cost_centres": total_cost_centres,
    }


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
    date_from: Optional[str] = Query(None, description="YYYYMMDD — FY start"),
    date_to: Optional[str] = Query(None, description="YYYYMMDD — FY end"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
) -> Any:
    """List vouchers with filters. Includes entry_count for each voucher."""
    q = select(Voucher)
    if company_name:
        q = q.where(Voucher.company_name == company_name)
    if voucher_type:
        q = q.where(Voucher.voucher_type == voucher_type)
    if date_from:
        q = q.where(Voucher.date >= date_from)
    if date_to:
        q = q.where(Voucher.date <= date_to)
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
    date_from: Optional[str] = Query(None, description="YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="YYYYMMDD"),
) -> Any:
    """Dashboard stats for Tally vouchers by type, optionally filtered by company and date range."""
    base = select(func.count(Voucher.id))
    if company_name:
        base = base.where(Voucher.company_name == company_name)
    if date_from:
        base = base.where(Voucher.date >= date_from)
    if date_to:
        base = base.where(Voucher.date <= date_to)
    total = (await db.execute(base)).scalar() or 0

    type_counts = {}
    for vtype in ["Sales", "Purchase", "Payment", "Receipt", "Journal", "Contra"]:
        q = select(func.count(Voucher.id)).where(Voucher.voucher_type == vtype)
        if company_name:
            q = q.where(Voucher.company_name == company_name)
        if date_from:
            q = q.where(Voucher.date >= date_from)
        if date_to:
            q = q.where(Voucher.date <= date_to)
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


@router.get("/mis-reports")
async def mis_reports(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    date_from: Optional[str] = Query(None, description="YYYYMMDD — filters vouchers/transactions"),
    date_to: Optional[str] = Query(None, description="YYYYMMDD — filters vouchers/transactions"),
) -> Any:
    """
    MIS Reports — Profitability, Sales, Cost & Inventory analysis
    computed from synced Tally ledgers, vouchers, and inventory entries.
    Optionally scoped to a date range (for FY filtering).
    """
    if not company_name:
        raise HTTPException(400, "company_name is required")

    # ─── 1. PROFITABILITY ANALYSIS ────────────────────────
    # Revenue groups: Sales Accounts
    revenue_groups = ['Sales Accounts', 'Sales Account']
    rev_q = (
        select(func.coalesce(func.sum(Ledger.closing_balance), 0))
        .where(Ledger.company_name == company_name, Ledger.parent.in_(revenue_groups))
    )
    total_revenue = float((await db.execute(rev_q)).scalar() or 0)
    # Tally: positive closing_balance = Credit (revenue)
    total_revenue = abs(total_revenue)

    # COGS / Direct expenses
    direct_groups = ['Purchase Accounts', 'Purchase Account', 'Direct Expenses', 'Stock-in-Hand']
    cogs_q = (
        select(func.coalesce(func.sum(Ledger.closing_balance), 0))
        .where(Ledger.company_name == company_name, Ledger.parent.in_(direct_groups))
    )
    total_cogs = abs(float((await db.execute(cogs_q)).scalar() or 0))

    # Indirect expenses
    indirect_groups = ['Indirect Expenses', 'Administrative Expenses', 'Selling Expenses',
                       'Indirect Expenses (Mfg.)', 'Misc. Expenses (ASSET)']
    indirect_q = (
        select(func.coalesce(func.sum(Ledger.closing_balance), 0))
        .where(Ledger.company_name == company_name, Ledger.parent.in_(indirect_groups))
    )
    total_indirect = abs(float((await db.execute(indirect_q)).scalar() or 0))

    gross_profit = total_revenue - total_cogs
    net_profit = gross_profit - total_indirect
    gross_margin = (gross_profit / total_revenue * 100) if total_revenue else 0
    net_margin = (net_profit / total_revenue * 100) if total_revenue else 0

    # ─── 2. SALES ANALYSIS ────────────────────────────────
    # Monthly sales trend
    month_col = func.substr(Voucher.date, 1, 6)
    monthly_sales_q = (
        select(
            month_col.label("month"),
            func.count(Voucher.id).label("count"),
            func.sum(func.abs(Voucher.amount)).label("total")
        )
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Sales")
    )
    if date_from:
        monthly_sales_q = monthly_sales_q.where(Voucher.date >= date_from)
    if date_to:
        monthly_sales_q = monthly_sales_q.where(Voucher.date <= date_to)
    monthly_sales_q = monthly_sales_q.group_by(month_col).order_by(month_col)
    monthly_result = await db.execute(monthly_sales_q)
    monthly_sales = [
        {"month": row[0], "count": row[1], "total": float(row[2]) if row[2] else 0}
        for row in monthly_result
    ]

    # Top 10 customers by sales amount
    top_customers_q = (
        select(
            Voucher.party_name,
            func.count(Voucher.id).label("invoice_count"),
            func.sum(func.abs(Voucher.amount)).label("total")
        )
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Sales",
               Voucher.party_name != None, Voucher.party_name != "")
    )
    if date_from:
        top_customers_q = top_customers_q.where(Voucher.date >= date_from)
    if date_to:
        top_customers_q = top_customers_q.where(Voucher.date <= date_to)
    top_customers_q = top_customers_q.group_by(Voucher.party_name).order_by(desc("total")).limit(10)
    top_cust_result = await db.execute(top_customers_q)
    top_customers = [
        {"name": row[0], "invoice_count": row[1], "total": float(row[2]) if row[2] else 0}
        for row in top_cust_result
    ]

    # Total sales stats
    sales_total_q = (
        select(
            func.count(Voucher.id),
            func.sum(func.abs(Voucher.amount))
        )
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Sales")
    )
    if date_from:
        sales_total_q = sales_total_q.where(Voucher.date >= date_from)
    if date_to:
        sales_total_q = sales_total_q.where(Voucher.date <= date_to)
    st_res = (await db.execute(sales_total_q)).first()
    total_sales_count = st_res[0] or 0
    total_sales_amount = float(st_res[1]) if st_res[1] else 0

    # ─── 3. COST ANALYSIS ─────────────────────────────────
    # Expense breakdown by ledger group (parent)
    expense_parents = direct_groups + indirect_groups
    expense_cat_q = (
        select(
            Ledger.parent,
            func.count(Ledger.id).label("ledger_count"),
            func.sum(func.abs(Ledger.closing_balance)).label("total")
        )
        .where(Ledger.company_name == company_name, Ledger.parent.in_(expense_parents))
        .group_by(Ledger.parent)
        .order_by(desc("total"))
    )
    exp_cat_result = await db.execute(expense_cat_q)
    expense_categories = [
        {"category": row[0] or "Other", "ledger_count": row[1], "total": float(row[2]) if row[2] else 0}
        for row in exp_cat_result
    ]

    # Top 10 individual expense ledgers
    top_expenses_q = (
        select(
            Ledger.name,
            Ledger.parent,
            func.abs(Ledger.closing_balance).label("amount")
        )
        .where(
            Ledger.company_name == company_name,
            Ledger.parent.in_(expense_parents),
            Ledger.closing_balance != 0
        )
        .order_by(desc("amount"))
        .limit(10)
    )
    top_exp_result = await db.execute(top_expenses_q)
    top_expenses = [
        {"name": row[0], "group": row[1] or "", "amount": float(row[2]) if row[2] else 0}
        for row in top_exp_result
    ]

    # Monthly purchase/expense trend
    pmonth_col = func.substr(Voucher.date, 1, 6)
    monthly_purchase_q = (
        select(
            pmonth_col.label("month"),
            func.count(Voucher.id).label("count"),
            func.sum(func.abs(Voucher.amount)).label("total")
        )
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Purchase")
    )
    if date_from:
        monthly_purchase_q = monthly_purchase_q.where(Voucher.date >= date_from)
    if date_to:
        monthly_purchase_q = monthly_purchase_q.where(Voucher.date <= date_to)
    monthly_purchase_q = monthly_purchase_q.group_by(pmonth_col).order_by(pmonth_col)
    monthly_purch_result = await db.execute(monthly_purchase_q)
    monthly_purchases = [
        {"month": row[0], "count": row[1], "total": float(row[2]) if row[2] else 0}
        for row in monthly_purch_result
    ]

    # ─── 4. INVENTORY ANALYSIS ────────────────────────────
    # Top 10 stock items by total sales value
    top_items_sales_q = (
        select(
            VoucherInventoryEntry.stock_item_name,
            func.sum(VoucherInventoryEntry.quantity).label("total_qty"),
            func.sum(func.abs(VoucherInventoryEntry.amount)).label("total_value"),
            func.count(VoucherInventoryEntry.id).label("txn_count")
        )
        .where(
            VoucherInventoryEntry.company_name == company_name,
            VoucherInventoryEntry.voucher_type == "Sales"
        )
    )
    if date_from:
        top_items_sales_q = top_items_sales_q.where(VoucherInventoryEntry.voucher_date >= date_from)
    if date_to:
        top_items_sales_q = top_items_sales_q.where(VoucherInventoryEntry.voucher_date <= date_to)
    top_items_sales_q = (
        top_items_sales_q
        .group_by(VoucherInventoryEntry.stock_item_name)
        .order_by(desc("total_value"))
        .limit(10)
    )
    top_items_result = await db.execute(top_items_sales_q)
    top_items_by_sales = [
        {
            "name": row[0], "total_qty": float(row[1]) if row[1] else 0,
            "total_value": float(row[2]) if row[2] else 0, "txn_count": row[3]
        }
        for row in top_items_result
    ]

    # Godown/Warehouse analysis
    godown_q = (
        select(
            VoucherInventoryEntry.godown,
            func.count(VoucherInventoryEntry.id).label("entries"),
            func.sum(func.abs(VoucherInventoryEntry.amount)).label("total_value")
        )
        .where(
            VoucherInventoryEntry.company_name == company_name,
            VoucherInventoryEntry.godown != None,
            VoucherInventoryEntry.godown != ""
        )
        .group_by(VoucherInventoryEntry.godown)
        .order_by(desc("total_value"))
        .limit(10)
    )
    godown_result = await db.execute(godown_q)
    godown_analysis = [
        {"godown": row[0] or "Main Location", "entries": row[1], "total_value": float(row[2]) if row[2] else 0}
        for row in godown_result
    ]

    # Total inventory stats
    inv_stats_q = (
        select(
            func.count(func.distinct(VoucherInventoryEntry.stock_item_name)),
            func.sum(func.abs(VoucherInventoryEntry.amount)),
        )
        .where(VoucherInventoryEntry.company_name == company_name)
    )
    inv_stats = (await db.execute(inv_stats_q)).first()

    # ─── 5. CASH FLOW ANALYSIS ────────────────────────────
    async def ledger_group_total(groups):
        q = select(func.coalesce(func.sum(Ledger.closing_balance), 0)).where(
            Ledger.company_name == company_name, Ledger.parent.in_(groups))
        return abs(float((await db.execute(q)).scalar() or 0))

    cash_bank_groups = ['Cash-in-Hand', 'Cash-in-hand', 'Bank Accounts', 'Bank OD A/c', 'Bank OCC A/c']
    cash_bank_balance = await ledger_group_total(cash_bank_groups)

    # Opening cash
    cash_opening_q = select(func.coalesce(func.sum(Ledger.opening_balance), 0)).where(
        Ledger.company_name == company_name, Ledger.parent.in_(cash_bank_groups))
    cash_opening = abs(float((await db.execute(cash_opening_q)).scalar() or 0))

    # Operating: revenue - expenses (simplified)
    cf_operating = total_revenue - total_cogs - total_indirect

    # Investing: Fixed Assets movement
    invest_groups = ['Fixed Assets', 'Investments']
    invest_closing = await ledger_group_total(invest_groups)
    invest_opening_q = select(func.coalesce(func.sum(func.abs(Ledger.opening_balance)), 0)).where(
        Ledger.company_name == company_name, Ledger.parent.in_(invest_groups))
    invest_opening = abs(float((await db.execute(invest_opening_q)).scalar() or 0))
    cf_investing = -(invest_closing - invest_opening)  # increase in assets = cash outflow

    # Financing: Loans & Capital
    finance_groups = ['Loans (Liability)', 'Secured Loans', 'Unsecured Loans', 'Capital Account',
                      'Reserves & Surplus', 'Share Capital']
    finance_closing = await ledger_group_total(finance_groups)
    finance_opening_q = select(func.coalesce(func.sum(func.abs(Ledger.opening_balance)), 0)).where(
        Ledger.company_name == company_name, Ledger.parent.in_(finance_groups))
    finance_opening = abs(float((await db.execute(finance_opening_q)).scalar() or 0))
    cf_financing = finance_closing - finance_opening

    net_cash_change = cf_operating + cf_investing + cf_financing

    # ─── 6. WORKING CAPITAL & RATIOS ──────────────────────
    ca_groups = ['Sundry Debtors', 'Cash-in-Hand', 'Cash-in-hand', 'Bank Accounts', 'Bank OD A/c',
                 'Stock-in-Hand', 'Deposits (Asset)', 'Loans & Advances (Asset)',
                 'Current Assets']
    cl_groups = ['Sundry Creditors', 'Duties & Taxes', 'Provisions',
                 'Current Liabilities']

    current_assets = await ledger_group_total(ca_groups)
    current_liabilities = await ledger_group_total(cl_groups)
    working_capital = current_assets - current_liabilities
    current_ratio = round(current_assets / current_liabilities, 2) if current_liabilities else 0

    # Quick ratio (exclude stock)
    stock_val = await ledger_group_total(['Stock-in-Hand'])
    quick_assets = current_assets - stock_val
    quick_ratio = round(quick_assets / current_liabilities, 2) if current_liabilities else 0

    # Debt-Equity
    total_debt = await ledger_group_total(['Secured Loans', 'Unsecured Loans', 'Loans (Liability)'])
    total_equity = await ledger_group_total(['Capital Account', 'Reserves & Surplus', 'Share Capital'])
    debt_equity = round(total_debt / total_equity, 2) if total_equity else 0

    # ─── 7. RECEIVABLES & PAYABLES ────────────────────────
    # Top debtors
    debtors_q = (
        select(Ledger.name, Ledger.closing_balance, Ledger.party_gstin)
        .where(Ledger.company_name == company_name,
               Ledger.parent.in_(['Sundry Debtors']),
               Ledger.closing_balance != 0)
        .order_by(desc(func.abs(Ledger.closing_balance)))
        .limit(15)
    )
    debtors = [{"name": r[0], "balance": abs(float(r[1])) if r[1] else 0, "gstin": r[2] or ""}
               for r in await db.execute(debtors_q)]
    total_receivables = sum(d["balance"] for d in debtors)

    # Top creditors
    creditors_q = (
        select(Ledger.name, Ledger.closing_balance, Ledger.party_gstin)
        .where(Ledger.company_name == company_name,
               Ledger.parent.in_(['Sundry Creditors']),
               Ledger.closing_balance != 0)
        .order_by(desc(func.abs(Ledger.closing_balance)))
        .limit(15)
    )
    creditors = [{"name": r[0], "balance": abs(float(r[1])) if r[1] else 0, "gstin": r[2] or ""}
                 for r in await db.execute(creditors_q)]
    total_payables = sum(c["balance"] for c in creditors)

    # Avg invoice value
    avg_invoice = round(total_sales_amount / total_sales_count, 2) if total_sales_count else 0

    return {
        "company_name": company_name,
        "profitability": {
            "total_revenue": total_revenue,
            "total_cogs": total_cogs,
            "gross_profit": gross_profit,
            "indirect_expenses": total_indirect,
            "net_profit": net_profit,
            "gross_margin": round(gross_margin, 2),
            "net_margin": round(net_margin, 2),
            "operating_profit": cf_operating,
            "operating_margin": round(cf_operating / total_revenue * 100, 2) if total_revenue else 0,
        },
        "sales": {
            "total_invoices": total_sales_count,
            "total_amount": total_sales_amount,
            "avg_invoice_value": avg_invoice,
            "monthly_trend": monthly_sales,
            "top_customers": top_customers,
        },
        "costs": {
            "total_expenses": total_cogs + total_indirect,
            "direct_costs": total_cogs,
            "indirect_costs": total_indirect,
            "cost_ratio": round((total_cogs + total_indirect) / total_revenue * 100, 2) if total_revenue else 0,
            "categories": expense_categories,
            "top_expenses": top_expenses,
            "monthly_purchases": monthly_purchases,
        },
        "inventory": {
            "unique_items": inv_stats[0] or 0,
            "total_value": float(inv_stats[1]) if inv_stats[1] else 0,
            "top_items_by_sales": top_items_by_sales,
            "godown_analysis": godown_analysis,
        },
        "cash_flow": {
            "opening_cash": cash_opening,
            "closing_cash": cash_bank_balance,
            "operating": round(cf_operating, 2),
            "investing": round(cf_investing, 2),
            "financing": round(cf_financing, 2),
            "net_change": round(net_cash_change, 2),
        },
        "working_capital": {
            "current_assets": current_assets,
            "current_liabilities": current_liabilities,
            "working_capital": working_capital,
            "current_ratio": current_ratio,
            "quick_ratio": quick_ratio,
            "debt_equity_ratio": debt_equity,
            "total_debt": total_debt,
            "total_equity": total_equity,
            "stock_value": stock_val,
            "total_receivables": total_receivables,
            "total_payables": total_payables,
        },
        "receivables_payables": {
            "total_receivables": total_receivables,
            "total_payables": total_payables,
            "net_position": total_receivables - total_payables,
            "debtors": debtors,
            "creditors": creditors,
        },
    }


@router.get("/stock-items")
async def list_stock_items(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = Query(None, description="YYYYMMDD — filter txn aggregates"),
    date_to: Optional[str] = Query(None, description="YYYYMMDD — filter txn aggregates"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
) -> Any:
    """
    List stock items from the stock_items master table synced from Tally,
    enriched with transaction aggregates from voucher_inventory_entries.
    """
    q = select(StockItem)
    if company_name:
        q = q.where(StockItem.company_name == company_name)
    if search:
        q = q.where(or_(
            StockItem.name.ilike(f"%{search}%"),
            StockItem.hsn_code.ilike(f"%{search}%"),
        ))

    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    q = q.order_by(StockItem.name).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(q)
    items = result.scalars().all()

    # Batch-load transaction aggregates from voucher_inventory_entries
    item_names = [item.name for item in items]
    txn_aggs = {}
    if item_names:
        agg_q = (
            select(
                VoucherInventoryEntry.stock_item_name,
                func.sum(func.abs(VoucherInventoryEntry.quantity)).label("total_qty"),
                func.sum(func.abs(VoucherInventoryEntry.amount)).label("total_value"),
                func.count(VoucherInventoryEntry.id).label("txn_count"),
                func.max(VoucherInventoryEntry.voucher_date).label("last_txn_date"),
                func.max(VoucherInventoryEntry.godown).label("godown"),
                # Fallback fields from inventory entries when master is empty
                func.max(VoucherInventoryEntry.hsn_code).label("inv_hsn"),
                func.max(VoucherInventoryEntry.uom).label("inv_uom"),
                func.max(VoucherInventoryEntry.gst_rate).label("inv_gst_rate"),
            )
            .where(VoucherInventoryEntry.stock_item_name.in_(item_names))
        )
        if company_name:
            agg_q = agg_q.where(VoucherInventoryEntry.company_name == company_name)
        if date_from:
            agg_q = agg_q.where(VoucherInventoryEntry.voucher_date >= date_from)
        if date_to:
            agg_q = agg_q.where(VoucherInventoryEntry.voucher_date <= date_to)
        agg_q = agg_q.group_by(VoucherInventoryEntry.stock_item_name)
        agg_result = await db.execute(agg_q)
        for row in agg_result:
            txn_aggs[row[0]] = {
                "total_qty": float(row[1]) if row[1] else 0,
                "total_value": float(row[2]) if row[2] else 0,
                "txn_count": row[3] or 0,
                "last_txn_date": row[4] or "",
                "godown": row[5] or "",
                "inv_hsn": row[6] or "",
                "inv_uom": row[7] or "",
                "inv_gst_rate": float(row[8]) if row[8] else 0,
            }

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": -(-total // per_page),
        "data": [
            {
                "name": item.name,
                "parent": item.parent or "",
                "category": item.category or "",
                "uom": item.uom or txn_aggs.get(item.name, {}).get("inv_uom", ""),
                "hsn_code": item.hsn_code or txn_aggs.get(item.name, {}).get("inv_hsn", ""),
                "gst_rate": float(item.gst_rate) if item.gst_rate else txn_aggs.get(item.name, {}).get("inv_gst_rate", 0),
                "opening_qty": float(item.opening_balance_qty) if item.opening_balance_qty else 0,
                "opening_rate": float(item.opening_balance_rate) if item.opening_balance_rate else 0,
                "opening_value": float(item.opening_balance_value) if item.opening_balance_value else 0,
                "description": item.description or "",
                "synced_at": item.synced_at.isoformat() if item.synced_at else None,
                # Transaction aggregates from voucher_inventory_entries
                "total_qty": txn_aggs.get(item.name, {}).get("total_qty", 0),
                "total_value": txn_aggs.get(item.name, {}).get("total_value", 0),
                "txn_count": txn_aggs.get(item.name, {}).get("txn_count", 0),
                "last_txn_date": txn_aggs.get(item.name, {}).get("last_txn_date", ""),
                "godown": txn_aggs.get(item.name, {}).get("godown", ""),
            }
            for item in items
        ],
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
