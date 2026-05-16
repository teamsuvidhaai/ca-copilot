"""
Tally Data API — serves ledgers, vouchers, voucher_entries,
and voucher_inventory_entries from the Supabase tables synced
by the Tally connector.
"""
from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, desc, asc, or_, and_, text

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


@router.get("/refund-data")
async def get_refund_data(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: str = Query(..., description="Tally company name"),
    date_from: Optional[str] = Query(None, description="Start date YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="End date YYYYMMDD"),
    refund_type: str = Query(..., description="Refund type key"),
) -> Any:
    """
    Aggregate Tally data into GST Refund Calculator fields.

    Extracts turnover, ITC, and tax data from synced Tally ledgers and vouchers
    for the selected company and date range. Returns pre-filled values in the
    exact format the /reconciliation/calculate-refund endpoint expects.

    The CA reviews these values and can adjust before submitting for calculation.
    """
    from sqlalchemy import case as sa_case

    # ── Helper: sum voucher amounts by type within date range ──
    async def sum_vouchers(v_type: str, date_from_: str = None, date_to_: str = None):
        q = (
            select(func.coalesce(func.sum(func.abs(Voucher.amount)), 0))
            .where(Voucher.company_name == company_name)
            .where(Voucher.voucher_type == v_type)
        )
        if date_from_:
            q = q.where(Voucher.date >= date_from_)
        if date_to_:
            q = q.where(Voucher.date <= date_to_)
        return float((await db.execute(q)).scalar() or 0)

    # ── Helper: sum ledger closing balances by parent groups ──
    async def sum_ledgers(parents: list):
        q = (
            select(func.coalesce(func.sum(func.abs(Ledger.closing_balance)), 0))
            .where(Ledger.company_name == company_name)
            .where(Ledger.parent.in_(parents))
        )
        return float((await db.execute(q)).scalar() or 0)

    # ── Helper: sum voucher entries by ledger parent within date range ──
    async def sum_voucher_entries_by_parent(parents: list, is_debit: bool = None,
                                            date_from_: str = None, date_to_: str = None):
        q = (
            select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
            .where(VoucherEntry.company_name == company_name)
        )
        if date_from_:
            q = q.where(VoucherEntry.voucher_date >= date_from_)
        if date_to_:
            q = q.where(VoucherEntry.voucher_date <= date_to_)
        if is_debit is not None:
            q = q.where(VoucherEntry.is_debit == is_debit)

        # Join with Ledger to filter by parent group
        q = q.where(
            VoucherEntry.ledger_name.in_(
                select(Ledger.name)
                .where(Ledger.company_name == company_name)
                .where(Ledger.parent.in_(parents))
            )
        )
        return float((await db.execute(q)).scalar() or 0)

    # ── Helper: sum sales voucher entries for export-type ledgers ──
    async def sum_export_sales(date_from_: str = None, date_to_: str = None):
        """
        Export sales identified by ledger names containing common export keywords.
        The CA should verify/adjust this value.
        """
        export_keywords = ['export', 'zero rated', 'zero-rated', 'sez', 'lut',
                           'deemed export', 'foreign', 'overseas']
        conditions = [Ledger.name.ilike(f"%{kw}%") for kw in export_keywords]

        export_ledger_q = (
            select(Ledger.name)
            .where(Ledger.company_name == company_name)
            .where(Ledger.parent.in_(['Sales Accounts', 'Sales Account']))
            .where(or_(*conditions))
        )
        export_ledgers = [row[0] for row in await db.execute(export_ledger_q)]

        if not export_ledgers:
            return 0.0, []

        q = (
            select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(export_ledgers))
            .where(VoucherEntry.is_debit == False)
        )
        if date_from_:
            q = q.where(VoucherEntry.voucher_date >= date_from_)
        if date_to_:
            q = q.where(VoucherEntry.voucher_date <= date_to_)

        total = float((await db.execute(q)).scalar() or 0)
        return total, export_ledgers

    # ══════════════════════════════════════════════════════════
    # AGGREGATE DATA BASED ON REFUND TYPE
    # ══════════════════════════════════════════════════════════
    notes = []
    fields = {}

    # ── Total Sales Turnover ──
    total_sales = await sum_vouchers("Sales", date_from, date_to)

    # ── Total Purchases ──
    total_purchases = await sum_vouchers("Purchase", date_from, date_to)

    # ── Tax ledger groups (ITC) ──
    tax_input_parents = ['Duties & Taxes']
    itc_total = await sum_voucher_entries_by_parent(
        tax_input_parents, is_debit=True, date_from_=date_from, date_to_=date_to
    )

    # ── Capital Goods ITC ──
    capital_parents = ['Fixed Assets']
    capital_goods_itc = await sum_voucher_entries_by_parent(
        tax_input_parents, is_debit=True, date_from_=date_from, date_to_=date_to
    )
    # Approximate: tax entries on purchase vouchers involving fixed asset ledgers
    cap_goods_voucher_q = (
        select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
        .where(VoucherEntry.company_name == company_name)
        .where(VoucherEntry.is_debit == True)
        .where(VoucherEntry.ledger_name.in_(
            select(Ledger.name)
            .where(Ledger.company_name == company_name)
            .where(Ledger.parent.in_(tax_input_parents))
        ))
        .where(VoucherEntry.voucher_guid.in_(
            select(VoucherEntry.voucher_guid)
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(
                select(Ledger.name)
                .where(Ledger.company_name == company_name)
                .where(Ledger.parent.in_(capital_parents))
            ))
        ))
    )
    if date_from:
        cap_goods_voucher_q = cap_goods_voucher_q.where(VoucherEntry.voucher_date >= date_from)
    if date_to:
        cap_goods_voucher_q = cap_goods_voucher_q.where(VoucherEntry.voucher_date <= date_to)
    capital_goods_itc = float((await db.execute(cap_goods_voucher_q)).scalar() or 0)

    # ── Input Services ITC (for goods export — Circular 135 exclusion) ──
    service_parents = ['Indirect Expenses', 'Administrative Expenses',
                       'Selling Expenses', 'Misc. Expenses (ASSET)']
    input_services_itc = await sum_voucher_entries_by_parent(
        tax_input_parents, is_debit=True, date_from_=date_from, date_to_=date_to
    )
    # Approximate by finding tax entries on vouchers that have service expense ledgers
    svc_itc_q = (
        select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
        .where(VoucherEntry.company_name == company_name)
        .where(VoucherEntry.is_debit == True)
        .where(VoucherEntry.ledger_name.in_(
            select(Ledger.name)
            .where(Ledger.company_name == company_name)
            .where(Ledger.parent.in_(tax_input_parents))
        ))
        .where(VoucherEntry.voucher_guid.in_(
            select(VoucherEntry.voucher_guid)
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(
                select(Ledger.name)
                .where(Ledger.company_name == company_name)
                .where(Ledger.parent.in_(service_parents))
            ))
        ))
    )
    if date_from:
        svc_itc_q = svc_itc_q.where(VoucherEntry.voucher_date >= date_from)
    if date_to:
        svc_itc_q = svc_itc_q.where(VoucherEntry.voucher_date <= date_to)
    input_services_itc = float((await db.execute(svc_itc_q)).scalar() or 0)

    # ── Export/Zero-Rated Turnover ──
    export_turnover, export_ledger_names = await sum_export_sales(date_from, date_to)

    # ── Exempt Turnover ──
    exempt_keywords = ['exempt', 'nil rated', 'nil-rated', 'non-taxable']
    exempt_conditions = [Ledger.name.ilike(f"%{kw}%") for kw in exempt_keywords]
    exempt_ledger_q = (
        select(Ledger.name)
        .where(Ledger.company_name == company_name)
        .where(Ledger.parent.in_(['Sales Accounts', 'Sales Account']))
        .where(or_(*exempt_conditions))
    )
    exempt_ledgers = [row[0] for row in await db.execute(exempt_ledger_q)]
    exempt_turnover = 0.0
    if exempt_ledgers:
        ex_q = (
            select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(exempt_ledgers))
            .where(VoucherEntry.is_debit == False)
        )
        if date_from:
            ex_q = ex_q.where(VoucherEntry.voucher_date >= date_from)
        if date_to:
            ex_q = ex_q.where(VoucherEntry.voucher_date <= date_to)
        exempt_turnover = float((await db.execute(ex_q)).scalar() or 0)

    # ── Output Tax (for IDS) ──
    output_tax = await sum_voucher_entries_by_parent(
        tax_input_parents, is_debit=False, date_from_=date_from, date_to_=date_to
    )

    # ── Voucher counts for metadata ──
    sales_count_q = (
        select(func.count(Voucher.id))
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Sales")
    )
    purchase_count_q = (
        select(func.count(Voucher.id))
        .where(Voucher.company_name == company_name, Voucher.voucher_type == "Purchase")
    )
    if date_from:
        sales_count_q = sales_count_q.where(Voucher.date >= date_from)
        purchase_count_q = purchase_count_q.where(Voucher.date >= date_from)
    if date_to:
        sales_count_q = sales_count_q.where(Voucher.date <= date_to)
        purchase_count_q = purchase_count_q.where(Voucher.date <= date_to)

    sales_count = (await db.execute(sales_count_q)).scalar() or 0
    purchase_count = (await db.execute(purchase_count_q)).scalar() or 0

    # ══════════════════════════════════════════════════════════
    # BUILD RESPONSE PER REFUND TYPE
    # ══════════════════════════════════════════════════════════

    if refund_type in ("export_goods_lut", "export_service_lut", "deemed_export"):
        if refund_type == "export_goods_lut":
            fields = {
                "turnover_zero_rated_goods": round(export_turnover, 2),
                "total_turnover": round(total_sales - export_turnover, 2),
                "exempt_turnover": round(exempt_turnover, 2),
                "itc_availed": round(itc_total, 2),
                "itc_capital_goods": round(capital_goods_itc, 2),
                "blocked_credit": 0,
                "itc_input_services": round(input_services_itc, 2),
            }
        elif refund_type == "export_service_lut":
            fields = {
                "turnover_zero_rated_services": round(export_turnover, 2),
                "total_turnover": round(total_sales - export_turnover, 2),
                "exempt_turnover": round(exempt_turnover, 2),
                "itc_availed": round(itc_total, 2),
                "itc_capital_goods": round(capital_goods_itc, 2),
                "blocked_credit": 0,
            }
        else:  # deemed_export
            fields = {
                "turnover_zero_rated_goods": round(export_turnover, 2),
                "total_turnover": round(total_sales - export_turnover, 2),
                "exempt_turnover": round(exempt_turnover, 2),
                "itc_availed": round(itc_total, 2),
                "itc_capital_goods": round(capital_goods_itc, 2),
                "blocked_credit": 0,
            }

        if export_ledger_names:
            notes.append(f"Export turnover from ledgers: {', '.join(export_ledger_names)}")
        else:
            notes.append(
                "No export-specific ledgers found. "
                "Please enter zero-rated turnover manually or rename Tally ledgers "
                "to include 'Export' / 'Zero Rated' / 'SEZ' in the name."
            )

    elif refund_type == "inverted_duty":
        fields = {
            "turnover_inverted": 0,
            "total_turnover": round(total_sales, 2),
            "exempt_turnover": round(exempt_turnover, 2),
            "itc_availed": round(itc_total, 2),
            "itc_capital_goods": round(capital_goods_itc, 2),
            "blocked_credit": 0,
            "tax_payable_inverted": round(output_tax, 2),
        }
        notes.append(
            "Inverted rated supply turnover must be entered manually — "
            "identify outputs where input GST rate exceeds output rate."
        )

    elif refund_type == "export_igst":
        # IGST paid: approximate from tax entries on sales vouchers
        igst_on_sales_q = (
            select(func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0))
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.is_debit == False)
            .where(VoucherEntry.ledger_name.ilike("%igst%"))
            .where(VoucherEntry.voucher_guid.in_(
                select(Voucher.guid)
                .where(Voucher.company_name == company_name)
                .where(Voucher.voucher_type == "Sales")
            ))
        )
        if date_from:
            igst_on_sales_q = igst_on_sales_q.where(VoucherEntry.voucher_date >= date_from)
        if date_to:
            igst_on_sales_q = igst_on_sales_q.where(VoucherEntry.voucher_date <= date_to)
        igst_on_exports = float((await db.execute(igst_on_sales_q)).scalar() or 0)

        fields = {
            "igst_paid_on_exports": round(igst_on_exports, 2),
            "igst_paid_on_services": 0,
            "shipping_bills_total": 0,
            "shipping_bills_matched": 0,
            "withheld_amount": 0,
        }
        notes.append("Shipping bill data not available in Tally — enter manually.")

    elif refund_type == "excess_cash":
        fields = {
            "cash_ledger_balance": 0,
            "amount_earmarked": 0,
            "refund_amount": 0,
        }
        notes.append("Electronic Cash Ledger balance must be fetched from GST Portal.")

    else:
        return {"error": f"Unknown refund type: {refund_type}"}

    # ── Add blocked credit note ──
    notes.append(
        "Blocked credit u/s 17(5) requires professional judgment — "
        "review personal expenses, club membership, motor vehicles, etc."
    )

    return {
        "refund_type": refund_type,
        "company_name": company_name,
        "date_from": date_from,
        "date_to": date_to,
        "fields": fields,
        "notes": notes,
        "metadata": {
            "total_sales": round(total_sales, 2),
            "total_purchases": round(total_purchases, 2),
            "sales_voucher_count": sales_count,
            "purchase_voucher_count": purchase_count,
            "export_turnover_detected": round(export_turnover, 2),
            "export_ledgers_matched": export_ledger_names,
            "exempt_ledgers_matched": exempt_ledgers,
            "itc_total": round(itc_total, 2),
            "capital_goods_itc": round(capital_goods_itc, 2),
            "input_services_itc": round(input_services_itc, 2),
            "output_tax": round(output_tax, 2),
        },
    }


# ──────────────────────────────────────────
# INVOICE-LEVEL DATA FOR RECONCILIATION
# ──────────────────────────────────────────
@router.get("/invoices")
async def list_invoices_for_recon(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: str = Query(...),
    voucher_type: str = Query("Sales", description="Sales or Purchase"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(500, ge=1, le=2000),
) -> Any:
    """Return invoice-level data from Tally vouchers for reconciliation.
    Joins with Ledger to get party GSTIN.
    Returns: party_name, gstin, voucher_number, date, amount, tax_entries."""

    # ── Base query: vouchers with party GSTIN from ledger ──
    q = (
        select(
            Voucher.voucher_number,
            Voucher.date,
            Voucher.party_name,
            Voucher.amount,
            Voucher.narration,
            Voucher.guid,
            Ledger.party_gstin,
            Ledger.state,
        )
        .outerjoin(
            Ledger,
            and_(
                Ledger.company_name == Voucher.company_name,
                Ledger.name == Voucher.party_name,
            ),
        )
        .where(Voucher.company_name == company_name)
        .where(Voucher.voucher_type == voucher_type)
    )

    if date_from:
        q = q.where(Voucher.date >= date_from)
    if date_to:
        q = q.where(Voucher.date <= date_to)

    q = q.order_by(Voucher.date.desc())

    # Count total
    count_q = (
        select(func.count(Voucher.id))
        .where(Voucher.company_name == company_name)
        .where(Voucher.voucher_type == voucher_type)
    )
    if date_from:
        count_q = count_q.where(Voucher.date >= date_from)
    if date_to:
        count_q = count_q.where(Voucher.date <= date_to)
    total = (await db.execute(count_q)).scalar() or 0

    # Paginate
    offset = (page - 1) * per_page
    q = q.offset(offset).limit(per_page)

    rows = (await db.execute(q)).all()

    # ── Get tax entries for each voucher ──
    voucher_guids = [r[5] for r in rows]  # guid column

    tax_entries_map = {}
    if voucher_guids:
        # Get all tax ledger entries (Duties & Taxes parent) for these vouchers
        tax_ledgers_q = (
            select(Ledger.name)
            .where(Ledger.company_name == company_name)
            .where(Ledger.parent.in_([
                "Duties & Taxes", "Duties and Taxes",
                "Current Liabilities", "Tax",
            ]))
        )
        tax_ledger_names = [r[0] for r in await db.execute(tax_ledgers_q)]

        if tax_ledger_names:
            tax_q = (
                select(
                    VoucherEntry.voucher_guid,
                    VoucherEntry.ledger_name,
                    VoucherEntry.amount,
                    VoucherEntry.is_debit,
                )
                .where(VoucherEntry.company_name == company_name)
                .where(VoucherEntry.voucher_guid.in_(voucher_guids))
                .where(VoucherEntry.ledger_name.in_(tax_ledger_names))
            )
            tax_rows = (await db.execute(tax_q)).all()
            for tr in tax_rows:
                guid = tr[0]
                if guid not in tax_entries_map:
                    tax_entries_map[guid] = []
                tax_entries_map[guid].append({
                    "ledger": tr[1],
                    "amount": abs(float(tr[2])) if tr[2] else 0,
                    "type": "debit" if tr[3] else "credit",
                })

    # ── Build response ──
    invoices = []
    for row in rows:
        inv_no, date, party, amount, narration, guid, gstin, state = row
        tax_entries = tax_entries_map.get(guid, [])
        total_tax = sum(t["amount"] for t in tax_entries)

        # Determine tax breakup (CGST/SGST/IGST)
        cgst = sum(t["amount"] for t in tax_entries if "cgst" in (t["ledger"] or "").lower())
        sgst = sum(t["amount"] for t in tax_entries if "sgst" in (t["ledger"] or "").lower())
        igst = sum(t["amount"] for t in tax_entries if "igst" in (t["ledger"] or "").lower())
        cess = sum(t["amount"] for t in tax_entries if "cess" in (t["ledger"] or "").lower())

        taxable_value = abs(float(amount)) - total_tax if amount else 0

        invoices.append({
            "invoice_no": inv_no or "",
            "date": date or "",
            "party_name": party or "",
            "gstin": gstin or "",
            "state": state or "",
            "total_amount": abs(float(amount)) if amount else 0,
            "taxable_value": round(taxable_value, 2),
            "cgst": round(cgst, 2),
            "sgst": round(sgst, 2),
            "igst": round(igst, 2),
            "cess": round(cess, 2),
            "total_tax": round(total_tax, 2),
            "narration": narration or "",
        })

    # ── Summary ──
    total_amount = sum(i["total_amount"] for i in invoices)
    total_taxable = sum(i["taxable_value"] for i in invoices)
    total_tax_sum = sum(i["total_tax"] for i in invoices)

    return {
        "company_name": company_name,
        "voucher_type": voucher_type,
        "date_from": date_from,
        "date_to": date_to,
        "total": total,
        "page": page,
        "per_page": per_page,
        "summary": {
            "invoice_count": len(invoices),
            "total_amount": round(total_amount, 2),
            "total_taxable": round(total_taxable, 2),
            "total_tax": round(total_tax_sum, 2),
        },
        "invoices": invoices,
    }


# ──────────────────────────────────────────
# REPORTS — MONTHLY SUMMARY
# ──────────────────────────────────────────
@router.get("/reports/monthly-summary")
async def monthly_summary_report(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
    company_name: str = Query(...),
    year: Optional[int] = None,
) -> Any:
    """Return month-wise aggregation of sales, purchases, and tax from Tally.
    Used to power the Reports tab with real book data."""

    # Determine financial year range
    if not year:
        from datetime import datetime
        now = datetime.now()
        year = now.year if now.month >= 4 else now.year - 1

    fy_start = f"{year}0401"
    fy_end = f"{year + 1}0331"

    # ── Monthly Sales ──
    sales_q = (
        select(
            func.substr(Voucher.date, 1, 6).label("month"),
            func.count(Voucher.id).label("count"),
            func.coalesce(func.sum(func.abs(Voucher.amount)), 0).label("total"),
        )
        .where(Voucher.company_name == company_name)
        .where(Voucher.voucher_type == "Sales")
        .where(Voucher.date >= fy_start)
        .where(Voucher.date <= fy_end)
        .group_by(func.substr(Voucher.date, 1, 6))
        .order_by(func.substr(Voucher.date, 1, 6))
    )
    sales_rows = (await db.execute(sales_q)).all()

    # ── Monthly Purchases ──
    purchase_q = (
        select(
            func.substr(Voucher.date, 1, 6).label("month"),
            func.count(Voucher.id).label("count"),
            func.coalesce(func.sum(func.abs(Voucher.amount)), 0).label("total"),
        )
        .where(Voucher.company_name == company_name)
        .where(Voucher.voucher_type == "Purchase")
        .where(Voucher.date >= fy_start)
        .where(Voucher.date <= fy_end)
        .group_by(func.substr(Voucher.date, 1, 6))
        .order_by(func.substr(Voucher.date, 1, 6))
    )
    purchase_rows = (await db.execute(purchase_q)).all()

    # ── Monthly Tax (from Duties & Taxes ledger entries) ──
    tax_ledgers_q = (
        select(Ledger.name)
        .where(Ledger.company_name == company_name)
        .where(Ledger.parent.in_([
            "Duties & Taxes", "Duties and Taxes",
            "Current Liabilities", "Tax",
        ]))
    )
    tax_ledger_names = [r[0] for r in await db.execute(tax_ledgers_q)]

    tax_month_data = {}
    if tax_ledger_names:
        tax_q = (
            select(
                func.substr(VoucherEntry.voucher_date, 1, 6).label("month"),
                VoucherEntry.ledger_name,
                func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0).label("total"),
            )
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(tax_ledger_names))
            .where(VoucherEntry.voucher_date >= fy_start)
            .where(VoucherEntry.voucher_date <= fy_end)
            .group_by(
                func.substr(VoucherEntry.voucher_date, 1, 6),
                VoucherEntry.ledger_name,
            )
            .order_by(func.substr(VoucherEntry.voucher_date, 1, 6))
        )
        tax_rows = (await db.execute(tax_q)).all()
        for row in tax_rows:
            month = row[0]
            ledger = (row[1] or "").lower()
            amt = float(row[2]) if row[2] else 0
            if month not in tax_month_data:
                tax_month_data[month] = {"cgst": 0, "sgst": 0, "igst": 0, "cess": 0, "other": 0}
            if "cgst" in ledger:
                tax_month_data[month]["cgst"] += amt
            elif "sgst" in ledger or "utgst" in ledger:
                tax_month_data[month]["sgst"] += amt
            elif "igst" in ledger:
                tax_month_data[month]["igst"] += amt
            elif "cess" in ledger:
                tax_month_data[month]["cess"] += amt
            else:
                tax_month_data[month]["other"] += amt

    # ── Export sales detection ──
    export_conditions = [
        Ledger.name.ilike("%export%"),
        Ledger.name.ilike("%zero rated%"),
        Ledger.name.ilike("%lut%"),
        Ledger.name.ilike("%foreign%"),
    ]
    export_ledger_q = (
        select(Ledger.name)
        .where(Ledger.company_name == company_name)
        .where(or_(*export_conditions))
    )
    export_ledger_names = [r[0] for r in await db.execute(export_ledger_q)]

    export_month_data = {}
    if export_ledger_names:
        exp_q = (
            select(
                func.substr(VoucherEntry.voucher_date, 1, 6).label("month"),
                func.coalesce(func.sum(func.abs(VoucherEntry.amount)), 0).label("total"),
            )
            .where(VoucherEntry.company_name == company_name)
            .where(VoucherEntry.ledger_name.in_(export_ledger_names))
            .where(VoucherEntry.is_debit == False)
            .where(VoucherEntry.voucher_date >= fy_start)
            .where(VoucherEntry.voucher_date <= fy_end)
            .group_by(func.substr(VoucherEntry.voucher_date, 1, 6))
            .order_by(func.substr(VoucherEntry.voucher_date, 1, 6))
        )
        exp_rows = (await db.execute(exp_q)).all()
        for row in exp_rows:
            export_month_data[row[0]] = float(row[1]) if row[1] else 0

    # ── Build monthly summary ──
    all_months = set()
    for r in sales_rows:
        all_months.add(r[0])
    for r in purchase_rows:
        all_months.add(r[0])
    for m in tax_month_data:
        all_months.add(m)

    sales_map = {r[0]: {"count": r[1], "total": round(float(r[2]), 2)} for r in sales_rows}
    purchase_map = {r[0]: {"count": r[1], "total": round(float(r[2]), 2)} for r in purchase_rows}

    months = []
    for m in sorted(all_months):
        s = sales_map.get(m, {"count": 0, "total": 0})
        p = purchase_map.get(m, {"count": 0, "total": 0})
        t = tax_month_data.get(m, {"cgst": 0, "sgst": 0, "igst": 0, "cess": 0, "other": 0})
        exp = export_month_data.get(m, 0)

        total_tax = t["cgst"] + t["sgst"] + t["igst"] + t["cess"]

        months.append({
            "month": m,  # YYYYMM format
            "sales": s["total"],
            "sales_count": s["count"],
            "purchases": p["total"],
            "purchase_count": p["count"],
            "export_sales": round(exp, 2),
            "cgst": round(t["cgst"], 2),
            "sgst": round(t["sgst"], 2),
            "igst": round(t["igst"], 2),
            "cess": round(t["cess"], 2),
            "total_tax": round(total_tax, 2),
        })

    # ── FY totals ──
    total_sales = sum(m["sales"] for m in months)
    total_purchases = sum(m["purchases"] for m in months)
    total_exports = sum(m["export_sales"] for m in months)
    total_cgst = sum(m["cgst"] for m in months)
    total_sgst = sum(m["sgst"] for m in months)
    total_igst = sum(m["igst"] for m in months)
    total_tax = sum(m["total_tax"] for m in months)

    return {
        "company_name": company_name,
        "financial_year": f"{year}-{year + 1}",
        "fy_start": fy_start,
        "fy_end": fy_end,
        "months": months,
        "totals": {
            "sales": round(total_sales, 2),
            "purchases": round(total_purchases, 2),
            "export_sales": round(total_exports, 2),
            "cgst": round(total_cgst, 2),
            "sgst": round(total_sgst, 2),
            "igst": round(total_igst, 2),
            "total_tax": round(total_tax, 2),
        },
        "export_ledgers": export_ledger_names,
        "tax_ledgers": tax_ledger_names[:10],  # Sample for reference
    }
