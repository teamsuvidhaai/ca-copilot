"""
Financial Statements — Tally Direct Generation
────────────────────────────────────────────────
Generate Schedule III Balance Sheet, P&L and Schedules
directly from synced Tally ledger data using deterministic
group mapping. No OpenAI dependency — instant results.

Endpoints:
  GET  /fs-tally/companies          — list companies with synced data
  GET  /fs-tally/trial-balance      — structured TB from Tally ledgers
  POST /fs-tally/generate           — generate full Schedule III statements
  GET  /fs-tally/preview            — quick summary without full generation
"""

from typing import Any
import hashlib
import json
import logging
import uuid
from collections import defaultdict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func

from app.api import deps
from app.models.models import FinancialStatementJob, User, Ledger, Voucher

logger = logging.getLogger(__name__)
router = APIRouter()


# ═══════════════════════════════════════════════════════
#  TALLY GROUP → SCHEDULE III MAPPING (single source of truth)
# ═══════════════════════════════════════════════════════

from app.services.schedule_iii_mapping import (
    TALLY_TO_SCHEDULE_III,
    match_tally_group as _match_group,
    BS_EQUITY_LIAB_ORDER,
    BS_ASSETS_ORDER,
    PL_INCOME_ORDER,
    PL_EXPENSE_ORDER,
)


# ═══════════════════════════════════════════════════════
#  ENDPOINT: Companies with synced data
# ═══════════════════════════════════════════════════════

@router.get("/companies")
async def list_companies_with_data(
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """List all companies that have synced Tally ledger data."""
    q = select(Ledger.company_name, func.count(Ledger.id).label('cnt')).group_by(Ledger.company_name)
    result = await db.execute(q)
    rows = result.all()
    return [{"company_name": r[0], "ledger_count": r[1]} for r in rows if r[0]]


# ═══════════════════════════════════════════════════════
#  ENDPOINT: Trial Balance preview
# ═══════════════════════════════════════════════════════

@router.get("/trial-balance")
async def get_trial_balance(
    company_name: str = Query(...),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Get structured Trial Balance from Tally ledgers with Schedule III mapping."""
    ledgers = (await db.execute(
        select(Ledger).where(Ledger.company_name == company_name).order_by(Ledger.parent, Ledger.name)
    )).scalars().all()

    if not ledgers:
        raise HTTPException(404, f"No ledgers found for '{company_name}'")

    accounts = []
    total_dr = 0.0
    total_cr = 0.0

    for l in ledgers:
        cb = float(l.closing_balance or 0)
        ob = float(l.opening_balance or 0)
        parent = (l.parent or '').strip()
        category, schedule_group, note_ref = _match_group(parent)

        # Tally convention: positive = Credit, negative = Debit
        dr = abs(cb) if cb < 0 else None
        cr = cb if cb > 0 else None
        total_dr += dr or 0
        total_cr += cr or 0

        accounts.append({
            'account_name': l.name,
            'tally_group': parent,
            'debit': round(dr, 2) if dr else None,
            'credit': round(cr, 2) if cr else None,
            'net_balance': round(cb, 2),
            'opening_balance': round(ob, 2),
            'category': category,
            'schedule_group': schedule_group,
            'note_ref': note_ref,
        })

    return {
        'company_name': company_name,
        'total_ledgers': len(accounts),
        'total_debit': round(total_dr, 2),
        'total_credit': round(total_cr, 2),
        'difference': round(abs(total_dr - total_cr), 2),
        'is_tallied': abs(total_dr - total_cr) < 1,
        'accounts': accounts,
    }


# ═══════════════════════════════════════════════════════
#  ENDPOINT: Quick Preview (summary stats)
# ═══════════════════════════════════════════════════════

@router.get("/preview")
async def preview_financials(
    company_name: str = Query(...),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Quick financial summary without full Schedule III generation."""
    ledgers = (await db.execute(
        select(Ledger).where(Ledger.company_name == company_name)
    )).scalars().all()

    if not ledgers:
        raise HTTPException(404, f"No ledgers found for '{company_name}'")

    # Aggregate by category
    totals = {'Asset': 0, 'Liability': 0, 'Equity': 0, 'Income': 0, 'Expense': 0}
    for l in ledgers:
        cb = float(l.closing_balance or 0)
        parent = (l.parent or '').strip()
        cat, _, _ = _match_group(parent)
        totals[cat] += abs(cb)

    return {
        'company_name': company_name,
        'total_ledgers': len(ledgers),
        'total_assets': round(totals['Asset'], 2),
        'total_liabilities': round(totals['Liability'], 2),
        'total_equity': round(totals['Equity'], 2),
        'total_income': round(totals['Income'], 2),
        'total_expenses': round(totals['Expense'], 2),
        'net_profit': round(totals['Income'] - totals['Expense'], 2),
    }


# ═══════════════════════════════════════════════════════
#  ENDPOINT: Generate Full Schedule III Statements
# ═══════════════════════════════════════════════════════

@router.post("/generate")
async def generate_schedule_iii(
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Generate complete Schedule III financial statements from Tally ledgers.
    Returns: Balance Sheet, Profit & Loss, and Note-wise Schedules.
    All computed deterministically — no AI calls needed.
    Results are persisted to FinancialStatementJob for history.
    """
    company_name = body.get("company_name")
    financial_year = body.get("financial_year", "2025-26")
    client_id = body.get("client_id")

    if not company_name:
        raise HTTPException(400, "company_name required")

    # Fetch all ledgers
    ledgers = (await db.execute(
        select(Ledger).where(Ledger.company_name == company_name).order_by(Ledger.parent, Ledger.name)
    )).scalars().all()

    if not ledgers:
        return JSONResponse(content={
            "has_data": False,
            "message": f"No Tally data found for '{company_name}'. Sync via Data Entry first.",
        })

    # ── Step 1: Classify every ledger ──
    classified = []
    for l in ledgers:
        cb = float(l.closing_balance or 0)
        ob = float(l.opening_balance or 0)
        parent = (l.parent or '').strip()
        category, schedule_group, note_ref = _match_group(parent)

        classified.append({
            'name': l.name,
            'parent': parent,
            'category': category,
            'schedule_group': schedule_group,
            'note_ref': note_ref,
            'closing': cb,
            'opening': ob,
        })

    # ── Step 2: Group by Schedule III line item ──
    groups = defaultdict(lambda: {'items': [], 'total': 0.0, 'opening_total': 0.0, 'note_ref': ''})
    for c in classified:
        key = c['schedule_group']
        groups[key]['items'].append({
            'description': c['name'],
            'current_year': abs(c['closing']),
            'previous_year': abs(c['opening']),
        })
        groups[key]['total'] += abs(c['closing'])
        groups[key]['opening_total'] += abs(c['opening'])
        groups[key]['note_ref'] = c['note_ref']

    # ── Step 3: Build Balance Sheet ──
    def build_bs_section(order_list):
        sections = []
        for heading, items in order_list:
            section_items = []
            sub_total_cy = 0
            sub_total_py = 0
            for item_name in items:
                g = groups.get(item_name)
                if g and g['total'] > 0:
                    section_items.append({
                        'name': item_name,
                        'note_ref': g['note_ref'],
                        'current_year': round(g['total'], 2),
                        'previous_year': round(g['opening_total'], 2) if g['opening_total'] else None,
                    })
                    sub_total_cy += g['total']
                    sub_total_py += g['opening_total']
                else:
                    # Include with zero to show full Schedule III structure
                    section_items.append({
                        'name': item_name,
                        'note_ref': groups.get(item_name, {}).get('note_ref', ''),
                        'current_year': 0,
                        'previous_year': None,
                    })
            sections.append({
                'heading': heading,
                'items': section_items,
                'sub_total': {
                    'current_year': round(sub_total_cy, 2),
                    'previous_year': round(sub_total_py, 2) if sub_total_py else None,
                },
            })
        return sections

    eq_liab_sections = build_bs_section(BS_EQUITY_LIAB_ORDER)
    asset_sections = build_bs_section(BS_ASSETS_ORDER)

    total_eq_liab = sum(s['sub_total']['current_year'] for s in eq_liab_sections)
    total_eq_liab_py = sum(s['sub_total']['previous_year'] or 0 for s in eq_liab_sections)
    total_assets = sum(s['sub_total']['current_year'] for s in asset_sections)
    total_assets_py = sum(s['sub_total']['previous_year'] or 0 for s in asset_sections)

    balance_sheet = {
        'equity_and_liabilities': eq_liab_sections,
        'assets': asset_sections,
        'total_equity_liabilities': {
            'current_year': round(total_eq_liab, 2),
            'previous_year': round(total_eq_liab_py, 2) if total_eq_liab_py else None,
        },
        'total_assets': {
            'current_year': round(total_assets, 2),
            'previous_year': round(total_assets_py, 2) if total_assets_py else None,
        },
        'is_balanced': abs(total_eq_liab - total_assets) < 1,
    }

    # ── Step 4: Build Profit & Loss ──
    income_items = []
    total_income_cy = 0
    total_income_py = 0
    for item_name in PL_INCOME_ORDER:
        g = groups.get(item_name)
        if g and g['total'] > 0:
            income_items.append({
                'name': item_name,
                'note_ref': g['note_ref'],
                'current_year': round(g['total'], 2),
                'previous_year': round(g['opening_total'], 2) if g['opening_total'] else None,
            })
            total_income_cy += g['total']
            total_income_py += g['opening_total']

    expense_items = []
    total_expense_cy = 0
    total_expense_py = 0
    for item_name in PL_EXPENSE_ORDER:
        g = groups.get(item_name)
        if g and g['total'] > 0:
            expense_items.append({
                'name': item_name,
                'note_ref': g['note_ref'],
                'current_year': round(g['total'], 2),
                'previous_year': round(g['opening_total'], 2) if g['opening_total'] else None,
            })
            total_expense_cy += g['total']
            total_expense_py += g['opening_total']

    # Tax expense (from Duties & Taxes that are income tax related)
    tax_cy = 0
    tax_py = 0
    for c in classified:
        if c['category'] == 'Liability' and 'tax' in c['parent'].lower() and 'income' in c['name'].lower():
            tax_cy += abs(c['closing'])
            tax_py += abs(c['opening'])

    pbt_cy = total_income_cy - total_expense_cy
    pat_cy = pbt_cy - tax_cy
    pbt_py = total_income_py - total_expense_py
    pat_py = pbt_py - tax_py

    profit_and_loss = {
        'income': income_items,
        'total_income': {
            'current_year': round(total_income_cy, 2),
            'previous_year': round(total_income_py, 2) if total_income_py else None,
        },
        'expenses': expense_items,
        'total_expenses': {
            'current_year': round(total_expense_cy, 2),
            'previous_year': round(total_expense_py, 2) if total_expense_py else None,
        },
        'profit_before_tax': {
            'current_year': round(pbt_cy, 2),
            'previous_year': round(pbt_py, 2) if pbt_py else None,
        },
        'tax_expense': {
            'current_year': round(tax_cy, 2),
            'previous_year': round(tax_py, 2) if tax_py else None,
        },
        'profit_after_tax': {
            'current_year': round(pat_cy, 2),
            'previous_year': round(pat_py, 2) if pat_py else None,
        },
    }

    # ── Step 5: Build Schedules (Note-wise breakdown) ──
    schedules = []
    # Collect all note_refs used
    note_map = {}
    for c in classified:
        nr = c['note_ref']
        sg = c['schedule_group']
        if nr not in note_map:
            note_map[nr] = {'title': sg, 'items': [], 'total_cy': 0, 'total_py': 0}
        note_map[nr]['items'].append({
            'description': c['name'],
            'current_year': round(abs(c['closing']), 2),
            'previous_year': round(abs(c['opening']), 2) if c['opening'] else None,
        })
        note_map[nr]['total_cy'] += abs(c['closing'])
        note_map[nr]['total_py'] += abs(c['opening'])

    for note_ref in sorted(note_map.keys(), key=lambda x: int(x.replace('Note ', '')) if x.replace('Note ', '').isdigit() else 99):
        nm = note_map[note_ref]
        if nm['total_cy'] > 0 or nm['total_py'] > 0:
            schedules.append({
                'note_ref': note_ref,
                'title': nm['title'],
                'items': nm['items'],
                'total': {
                    'current_year': round(nm['total_cy'], 2),
                    'previous_year': round(nm['total_py'], 2) if nm['total_py'] else None,
                },
            })

    # ── Step 6: Account Mappings (for audit trail) ──
    account_mappings = []
    for c in classified:
        confidence = 'High'
        parent_lower = (c['parent'] or '').lower().strip()
        if parent_lower not in TALLY_TO_SCHEDULE_III:
            confidence = 'Medium'
            # Check if partial match worked
            matched = False
            for key in TALLY_TO_SCHEDULE_III:
                if key in parent_lower or parent_lower in key:
                    matched = True
                    break
            if not matched:
                confidence = 'Low'

        account_mappings.append({
            'tb_account': c['name'],
            'tally_group': c['parent'],
            'mapped_to': c['schedule_group'],
            'schedule': c['note_ref'],
            'confidence': confidence,
            'amount': round(abs(c['closing']), 2),
        })

    # ── Warnings ──
    warnings = []
    if not balance_sheet['is_balanced']:
        diff = abs(total_eq_liab - total_assets)
        warnings.append(f"Balance Sheet is unbalanced by ₹{diff:,.0f}. Check account classifications.")
    low_conf = [m for m in account_mappings if m['confidence'] == 'Low']
    if low_conf:
        warnings.append(f"{len(low_conf)} account(s) mapped with low confidence — verify manually: {', '.join(m['tb_account'] for m in low_conf[:5])}")
    if total_income_cy == 0:
        warnings.append("No revenue accounts detected. Check if Sales/Income groups are synced from Tally.")

    # Voucher stats
    voucher_count = (await db.execute(
        select(func.count()).select_from(Voucher).where(Voucher.company_name == company_name)
    )).scalar() or 0

    logger.info(
        f"FS-Tally generated: company={company_name}, ledgers={len(ledgers)}, "
        f"vouchers={voucher_count}, balanced={balance_sheet['is_balanced']}"
    )

    # ── Persist to FinancialStatementJob for history ──
    result_payload = {
        'company_name': company_name,
        'financial_year': financial_year,
        'total_ledgers': len(ledgers),
        'total_vouchers': voucher_count,
        'balance_sheet': balance_sheet,
        'profit_and_loss': profit_and_loss,
        'schedules': schedules,
        'account_mappings': account_mappings,
        'warnings': warnings,
        'current_year_date': f"31st March {int(financial_year.split('-')[0]) + 1}" if '-' in financial_year else financial_year,
        'previous_year_date': None,
        'summary': {
            'total_assets': round(total_assets, 2),
            'total_equity_liabilities': round(total_eq_liab, 2),
            'total_income': round(total_income_cy, 2),
            'total_expenses': round(total_expense_cy, 2),
            'net_profit': round(pat_cy, 2),
            'is_balanced': balance_sheet['is_balanced'],
        },
    }

    job_id = None
    if client_id:
        job_id = str(uuid.uuid4())
        ledger_hash = hashlib.sha256(
            json.dumps({'company': company_name, 'fy': financial_year, 'n': len(ledgers)}).encode()
        ).hexdigest()

        job = FinancialStatementJob(
            id=job_id,
            client_id=client_id,
            user_id=str(current_user.id),
            filenames={'source': 'tally', 'company': company_name},
            file_hash=ledger_hash,
            status='completed',
            company_name=company_name,
            financial_year=financial_year,
            is_balanced=balance_sheet['is_balanced'],
            result=result_payload,
        )
        db.add(job)
        await db.commit()
        logger.info(f"✅ FS-Tally job {job_id} persisted for client {client_id}")

    return JSONResponse(content={
        'has_data': True,
        'job_id': job_id,
        **result_payload,
    })
