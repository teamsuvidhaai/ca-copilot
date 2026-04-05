"""
Depreciation — IT Act vs Companies Act
CRUD + Compute + Tally Sync + Excel Export

Persists fixed assets per client, computes depreciation under both
IT Act (Sec 32) and Companies Act 2013 (Schedule II), calculates
deferred tax impact per Ind AS 12.
"""
from typing import Any
from uuid import UUID
from datetime import datetime
import math, re, logging

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, or_

from app.api import deps
from app.models.models import User, DepreciationAsset, Ledger

logger = logging.getLogger(__name__)
router = APIRouter()

# ── IT Act Rate Map (Sec 32, WDV default) ──
RATE_MAP = {
    'Plant & Machinery':    {'it_rate': 15, 'it_method': 'WDV', 'co_life': 15, 'co_method': 'SLM'},
    'Furniture & Fixtures': {'it_rate': 10, 'it_method': 'WDV', 'co_life': 10, 'co_method': 'SLM'},
    'Motor Vehicles':       {'it_rate': 15, 'it_method': 'WDV', 'co_life':  8, 'co_method': 'SLM'},
    'Office Equipment':     {'it_rate': 15, 'it_method': 'WDV', 'co_life':  5, 'co_method': 'SLM'},
    'Computers':            {'it_rate': 40, 'it_method': 'WDV', 'co_life':  3, 'co_method': 'SLM'},
    'Buildings':            {'it_rate': 10, 'it_method': 'WDV', 'co_life': 30, 'co_method': 'SLM'},
    'Intangible Assets':    {'it_rate': 25, 'it_method': 'WDV', 'co_life': 10, 'co_method': 'SLM'},
}


def _classify_group(name: str) -> str:
    """Map Tally ledger parent/name to standard asset group."""
    n = (name or '').lower()
    if any(k in n for k in ['plant', 'machinery', 'machine']): return 'Plant & Machinery'
    if any(k in n for k in ['furniture', 'fixture']): return 'Furniture & Fixtures'
    if any(k in n for k in ['vehicle', 'motor', 'car']): return 'Motor Vehicles'
    if any(k in n for k in ['computer', 'laptop', 'server', 'software']): return 'Computers'
    if any(k in n for k in ['building', 'premises', 'factory']): return 'Buildings'
    if any(k in n for k in ['intangible', 'goodwill', 'patent', 'trademark']): return 'Intangible Assets'
    if any(k in n for k in ['office', 'equipment', 'printer']): return 'Office Equipment'
    return 'Plant & Machinery'


def compute_depreciation(asset: dict) -> dict:
    """Compute IT Act + Companies Act depreciation for a single asset."""
    cost = asset.get('cost', 0)
    residual = asset.get('residual_value', round(cost * 0.05))
    it_rate = asset.get('it_rate', 15)
    it_method = asset.get('it_method', 'WDV')
    co_life = asset.get('co_life', 15)
    co_method = asset.get('co_method', 'SLM')
    date_str = asset.get('date_acquired', '2025-04-01')

    # Parse acquisition month for half-year rule
    try:
        acq_month = int(date_str.split('-')[1])
    except:
        acq_month = 4

    # Whether acquired in 2nd half of FY (Oct-Mar → half dep in Y1)
    half_year = acq_month >= 10

    # ── IT Act Depreciation ──
    it_schedule = []
    it_wdv = cost
    years = max(co_life, 20)
    for y in range(1, years + 1):
        if it_method == 'WDV':
            dep = it_wdv * (it_rate / 100)
        else:
            dep = cost * (it_rate / 100)
        if y == 1 and half_year:
            dep = dep / 2
        dep = min(dep, it_wdv)
        dep = round(dep, 2)
        it_wdv = round(it_wdv - dep, 2)
        it_schedule.append({'year': y, 'dep': dep, 'closing_wdv': max(0, it_wdv)})
        if it_wdv <= 1:
            break

    # ── Companies Act Depreciation ──
    co_schedule = []
    co_wdv = cost
    dep_base = cost - residual
    if co_method == 'SLM':
        annual_dep = dep_base / co_life if co_life > 0 else 0
    else:
        co_rate = (1 - pow(max(residual, 1) / max(cost, 1), 1 / max(co_life, 1))) * 100
        annual_dep = 0  # computed per year for WDV

    for y in range(1, co_life + 1):
        if co_method == 'SLM':
            dep = annual_dep
        else:
            dep = co_wdv * (co_rate / 100)

        if y == 1 and half_year:
            dep = dep / 2
        dep = min(dep, max(0, co_wdv - residual))
        dep = max(0, round(dep, 2))
        co_wdv = round(co_wdv - dep, 2)
        co_schedule.append({'year': y, 'dep': dep, 'closing_wdv': max(0, co_wdv)})
        if co_wdv <= residual:
            break

    it_dep_fy = it_schedule[0]['dep'] if it_schedule else 0
    co_dep_fy = co_schedule[0]['dep'] if co_schedule else 0
    diff = it_dep_fy - co_dep_fy
    deferred_tax = round(abs(diff) * 0.2517, 2)

    return {
        'it_dep_fy': it_dep_fy,
        'co_dep_fy': co_dep_fy,
        'difference': round(diff, 2),
        'deferred_tax': deferred_tax,
        'dt_type': 'DTL' if diff >= 0 else 'DTA',
        'it_schedule': it_schedule,
        'co_schedule': co_schedule,
    }


# ═══════════════════════════════════════════════════════
#  SAVE (Upsert) Asset
# ═══════════════════════════════════════════════════════

@router.post("/save")
async def save_depreciation_asset(
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Save or update a depreciation asset. Computes depreciation server-side."""
    client_id = body.get("client_id")
    asset_id = body.get("id")
    if not client_id:
        raise HTTPException(400, "client_id required")

    # Compute depreciation
    results = compute_depreciation(body)

    group = body.get("group_name", body.get("group", "Plant & Machinery"))
    rates = RATE_MAP.get(group, RATE_MAP['Plant & Machinery'])

    if asset_id:
        existing = await db.execute(
            select(DepreciationAsset).where(DepreciationAsset.id == asset_id)
        )
        asset = existing.scalars().first()
        if asset:
            asset.name = body.get("name", asset.name)
            asset.group_name = group
            asset.date_acquired = body.get("date_acquired", asset.date_acquired)
            asset.cost = body.get("cost", asset.cost)
            asset.residual_value = body.get("residual_value", round(body.get("cost", asset.cost) * 0.05))
            asset.it_rate = body.get("it_rate", rates['it_rate'])
            asset.it_method = body.get("it_method", rates['it_method'])
            asset.co_life = body.get("co_life", rates['co_life'])
            asset.co_method = body.get("co_method", rates['co_method'])
            asset.it_dep_fy = results['it_dep_fy']
            asset.co_dep_fy = results['co_dep_fy']
            asset.results = results
            asset.financial_year = body.get("financial_year", "2025-26")
            asset.updated_at = datetime.utcnow()
            await db.commit()
            await db.refresh(asset)
            return _asset_to_dict(asset)

    # Create new
    residual = body.get("residual_value")
    if residual is None:
        residual = round(body.get("cost", 0) * 0.05)

    asset = DepreciationAsset(
        client_id=client_id,
        user_id=current_user.id,
        firm_id=current_user.firm_id,
        name=body.get("name", "Unnamed Asset"),
        group_name=group,
        date_acquired=body.get("date_acquired", "2025-04-01"),
        cost=body.get("cost", 0),
        residual_value=residual,
        financial_year=body.get("financial_year", "2025-26"),
        it_rate=body.get("it_rate", rates['it_rate']),
        it_method=body.get("it_method", rates['it_method']),
        co_life=body.get("co_life", rates['co_life']),
        co_method=body.get("co_method", rates['co_method']),
        it_dep_fy=results['it_dep_fy'],
        co_dep_fy=results['co_dep_fy'],
        results=results,
        source=body.get("source", "manual"),
        tally_ledger_name=body.get("tally_ledger_name"),
    )
    db.add(asset)
    await db.commit()
    await db.refresh(asset)
    return _asset_to_dict(asset)


# ═══════════════════════════════════════════════════════
#  LIST — all assets for client
# ═══════════════════════════════════════════════════════

@router.get("/list")
async def list_depreciation_assets(
    client_id: UUID = Query(...),
    financial_year: str = Query("2025-26"),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    result = await db.execute(
        select(DepreciationAsset)
        .where(
            DepreciationAsset.client_id == client_id,
            DepreciationAsset.financial_year == financial_year,
            DepreciationAsset.status != "deleted",
        )
        .order_by(DepreciationAsset.group_name, DepreciationAsset.name)
    )
    assets = result.scalars().all()
    return [_asset_to_dict(a) for a in assets]


# ═══════════════════════════════════════════════════════
#  DELETE
# ═══════════════════════════════════════════════════════

@router.delete("/{asset_id}")
async def delete_depreciation_asset(
    asset_id: UUID,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    result = await db.execute(
        select(DepreciationAsset).where(DepreciationAsset.id == asset_id)
    )
    asset = result.scalars().first()
    if not asset:
        raise HTTPException(404, "Asset not found")
    asset.status = "deleted"
    await db.commit()
    return {"deleted": True, "id": str(asset_id)}


# ═══════════════════════════════════════════════════════
#  SYNC FROM TALLY (Load from synced data in DB)
# ═══════════════════════════════════════════════════════

@router.post("/sync-from-tally")
async def sync_from_tally(
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Extract fixed assets from Tally ledgers already synced in DB.
    Looks under 'Fixed Assets' parent group and sub-groups.
    """
    client_id = body.get("client_id")
    company_name = body.get("company_name")
    financial_year = body.get("financial_year", "2025-26")

    if not company_name:
        raise HTTPException(400, "company_name required")

    # Query ledgers under Fixed Assets and related groups
    q = select(Ledger).where(
        Ledger.company_name == company_name,
        or_(
            Ledger.parent.ilike('%Fixed Assets%'),
            Ledger.parent.ilike('%Plant%'),
            Ledger.parent.ilike('%Machinery%'),
            Ledger.parent.ilike('%Furniture%'),
            Ledger.parent.ilike('%Vehicle%'),
            Ledger.parent.ilike('%Computer%'),
            Ledger.parent.ilike('%Building%'),
            Ledger.parent.ilike('%Office Equipment%'),
            Ledger.parent.ilike('%Intangible%'),
        )
    )
    result = await db.execute(q)
    ledgers = result.scalars().all()

    saved = 0
    assets_data = []

    for led in ledgers:
        bal = abs(float(led.closing_balance or led.opening_balance or 0))
        if bal <= 0:
            continue

        group = _classify_group(led.parent or led.name)
        rates = RATE_MAP.get(group, RATE_MAP['Plant & Machinery'])

        # Check if already exists
        if client_id:
            existing = await db.execute(
                select(DepreciationAsset).where(
                    DepreciationAsset.client_id == client_id,
                    DepreciationAsset.tally_ledger_name == led.name,
                    DepreciationAsset.financial_year == financial_year,
                )
            )
            if existing.scalars().first():
                continue

        asset_data = {
            'name': led.name,
            'group_name': group,
            'date_acquired': '2025-04-01',  # Default if not known
            'cost': bal,
            'residual_value': round(bal * 0.05),
            'it_rate': rates['it_rate'],
            'it_method': rates['it_method'],
            'co_life': rates['co_life'],
            'co_method': rates['co_method'],
        }

        results = compute_depreciation(asset_data)

        if client_id:
            db_asset = DepreciationAsset(
                client_id=client_id,
                user_id=current_user.id,
                firm_id=current_user.firm_id,
                name=led.name,
                group_name=group,
                date_acquired='2025-04-01',
                cost=bal,
                residual_value=round(bal * 0.05),
                financial_year=financial_year,
                it_rate=rates['it_rate'],
                it_method=rates['it_method'],
                co_life=rates['co_life'],
                co_method=rates['co_method'],
                it_dep_fy=results['it_dep_fy'],
                co_dep_fy=results['co_dep_fy'],
                results=results,
                source='ledger',
                tally_ledger_name=led.name,
            )
            db.add(db_asset)
            saved += 1

        asset_data['results'] = results
        asset_data['it_dep_fy'] = results['it_dep_fy']
        asset_data['co_dep_fy'] = results['co_dep_fy']
        asset_data['source'] = 'ledger'
        asset_data['tally_ledger_name'] = led.name
        assets_data.append(asset_data)

    if client_id and saved > 0:
        await db.commit()

    logger.info(f"Depreciation sync: company={company_name}, ledgers={len(ledgers)}, saved={saved}")

    return JSONResponse(content={
        "assets": assets_data,
        "total": len(assets_data),
        "saved": saved,
        "company_name": company_name,
    })


# ═══════════════════════════════════════════════════════
#  COMPUTE ALL — recompute all assets for a client
# ═══════════════════════════════════════════════════════

@router.post("/compute-all")
async def compute_all_assets(
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    client_id = body.get("client_id")
    financial_year = body.get("financial_year", "2025-26")

    result = await db.execute(
        select(DepreciationAsset).where(
            DepreciationAsset.client_id == client_id,
            DepreciationAsset.financial_year == financial_year,
            DepreciationAsset.status != "deleted",
        )
    )
    assets = result.scalars().all()

    for a in assets:
        res = compute_depreciation({
            'cost': a.cost, 'residual_value': a.residual_value,
            'it_rate': a.it_rate, 'it_method': a.it_method,
            'co_life': a.co_life, 'co_method': a.co_method,
            'date_acquired': a.date_acquired,
        })
        a.it_dep_fy = res['it_dep_fy']
        a.co_dep_fy = res['co_dep_fy']
        a.results = res
        a.updated_at = datetime.utcnow()

    await db.commit()
    return {"computed": len(assets)}


# ═══════════════════════════════════════════════════════
#  SUMMARY — deferred tax summary by group
# ═══════════════════════════════════════════════════════

@router.get("/summary")
async def depreciation_summary(
    client_id: UUID = Query(...),
    financial_year: str = Query("2025-26"),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    result = await db.execute(
        select(DepreciationAsset).where(
            DepreciationAsset.client_id == client_id,
            DepreciationAsset.financial_year == financial_year,
            DepreciationAsset.status != "deleted",
        )
    )
    assets = result.scalars().all()

    groups = {}
    for a in assets:
        g = groups.setdefault(a.group_name, {
            'count': 0, 'cost': 0, 'it_dep': 0, 'co_dep': 0
        })
        g['count'] += 1
        g['cost'] += a.cost
        g['it_dep'] += a.it_dep_fy or 0
        g['co_dep'] += a.co_dep_fy or 0

    summary = []
    for group, g in groups.items():
        diff = g['it_dep'] - g['co_dep']
        summary.append({
            'group': group,
            'count': g['count'],
            'cost': g['cost'],
            'it_dep': round(g['it_dep'], 2),
            'co_dep': round(g['co_dep'], 2),
            'difference': round(diff, 2),
            'deferred_tax': round(abs(diff) * 0.2517, 2),
            'dt_type': 'DTL' if diff >= 0 else 'DTA',
        })

    total_it = sum(g['it_dep'] for g in summary)
    total_co = sum(g['co_dep'] for g in summary)
    total_diff = total_it - total_co

    return {
        'groups': summary,
        'total_assets': len(assets),
        'total_cost': sum(a.cost for a in assets),
        'total_it_dep': round(total_it, 2),
        'total_co_dep': round(total_co, 2),
        'total_difference': round(total_diff, 2),
        'total_deferred_tax': round(abs(total_diff) * 0.2517, 2),
        'dt_type': 'DTL' if total_diff >= 0 else 'DTA',
    }


def _asset_to_dict(a: DepreciationAsset) -> dict:
    return {
        'id': str(a.id),
        'name': a.name,
        'group_name': a.group_name,
        'date_acquired': a.date_acquired,
        'cost': a.cost,
        'residual_value': a.residual_value,
        'it_rate': a.it_rate,
        'it_method': a.it_method,
        'co_life': a.co_life,
        'co_method': a.co_method,
        'it_dep_fy': a.it_dep_fy,
        'co_dep_fy': a.co_dep_fy,
        'results': a.results,
        'source': a.source,
        'status': a.status,
        'financial_year': a.financial_year,
    }
