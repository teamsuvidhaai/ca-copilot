"""
Financial Statements API
────────────────────────
Upload previous-year BS + Notes + current-year Trial Balance.
AI maps accounts (VLOOKUP-style), generates comparative Balance Sheet,
P&L Statement, and detailed Schedules.

Persistent PostgreSQL storage — survives restarts, supports queries.
"""

import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any

from app.services.fs_rule_parser import parse_trial_balance, parse_balance_sheet, map_tb_to_schedule_iii
from app.services.schedule_iii_mapping import TALLY_TO_SCHEDULE_III, match_tally_group as _match_group
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import deps
from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.models import FinancialStatementJob, Ledger, User

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 25 * 1024 * 1024  # 25 MB

# ─── Prompts ───────────────────────────────────────────

TB_EXTRACTION_PROMPT = """You are an expert Indian Chartered Accountant AI specialising in Trial Balance analysis.

Given raw text extracted from a Trial Balance document, extract EVERY account with its debit/credit balance.

**Rules:**
1. Extract ALL accounts — do not skip any row.
2. Amounts must be plain numbers (no commas, no ₹) or null.
3. Classify each account into a schedule group:
   - Share Capital, Reserves & Surplus, Long Term Borrowings, Short Term Borrowings,
   - Trade Payables, Other Current Liabilities, Short Term Provisions, Long Term Provisions,
   - Fixed Assets (Tangible), Fixed Assets (Intangible), Capital WIP,
   - Non-Current Investments, Long Term Loans & Advances,
   - Current Investments, Inventories, Trade Receivables, Cash & Bank Balances,
   - Short Term Loans & Advances, Other Current Assets,
   - Revenue from Operations, Other Income,
   - Cost of Materials, Employee Benefit Expense, Finance Costs, Depreciation,
   - Other Expenses, Tax Expense
4. Determine if each account is: Asset, Liability, Equity, Income, or Expense.

Return this JSON:
{
    "company_name": "string or null",
    "period": "string or null",
    "as_on_date": "YYYY-MM-DD or null",
    "accounts": [
        {
            "account_name": "string",
            "debit": number or null,
            "credit": number or null,
            "net_balance": number,
            "category": "Asset/Liability/Equity/Income/Expense",
            "schedule_group": "one of the groups above",
            "sub_group": "string or null"
        }
    ],
    "total_debit": number,
    "total_credit": number
}"""

BS_EXTRACTION_PROMPT = """You are an expert Indian Chartered Accountant AI.

Given raw text from a previous-year Balance Sheet document, extract the complete structure
including all line items, schedule references, and amounts.

**Rules:**
1. Extract EVERY line item with its amount.
2. Preserve the schedule numbering (Note 1, Note 2, etc.).
3. Separate Assets side and Liabilities & Equity side.
4. Amounts: plain numbers, no commas/₹.

Return this JSON:
{
    "company_name": "string or null",
    "as_on_date": "YYYY-MM-DD or null",
    "previous_year_date": "YYYY-MM-DD or null",
    "equity_and_liabilities": {
        "shareholders_funds": {
            "share_capital": {"amount": number, "note_ref": "string or null", "prev_year": number or null},
            "reserves_and_surplus": {"amount": number, "note_ref": "string or null", "prev_year": number or null}
        },
        "non_current_liabilities": {
            "long_term_borrowings": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "deferred_tax_liabilities": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "long_term_provisions": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null}
        },
        "current_liabilities": {
            "short_term_borrowings": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "trade_payables": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "other_current_liabilities": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "short_term_provisions": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null}
        },
        "total": number
    },
    "assets": {
        "non_current_assets": {
            "fixed_assets": {
                "tangible": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
                "intangible": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
                "capital_wip": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null}
            },
            "non_current_investments": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "long_term_loans_advances": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null}
        },
        "current_assets": {
            "current_investments": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "inventories": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "trade_receivables": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "cash_and_bank": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "short_term_loans_advances": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null},
            "other_current_assets": {"amount": number or null, "note_ref": "string or null", "prev_year": number or null}
        },
        "total": number
    }
}"""

MAPPING_AND_GENERATION_PROMPT = """You are an expert Indian Chartered Accountant preparing financial statements
under Schedule III of the Companies Act, 2013.

You are given:
1. CURRENT YEAR TRIAL BALANCE (structured accounts with balances)
2. PREVIOUS YEAR BALANCE SHEET (structured line items with amounts)
3. NOTES TO ACCOUNTS (if available)

Your task:
1. **MAP** each Trial Balance account to the correct Balance Sheet / P&L line item
   (VLOOKUP-style matching by account name, group, and accounting conventions).
2. **GENERATE** a comparative Balance Sheet with current year and previous year columns.
3. **GENERATE** a Profit & Loss Statement with current year figures.
4. **GENERATE** detailed Schedules/Notes for each line item.

**Indian Accounting Rules:**
- Follow Schedule III format strictly
- Assets = Liabilities + Equity (must balance)
- All amounts in INR (plain numbers)
- Income/Expense accounts go to P&L, then net profit flows to Reserves
- Depreciation reduces Fixed Asset values
- Closing stock adjustments for inventory

Return this JSON:
{
    "company_name": "string",
    "current_year_date": "YYYY-MM-DD",
    "previous_year_date": "YYYY-MM-DD or null",
    "account_mappings": [
        {
            "tb_account": "string",
            "mapped_to": "string (BS/PL line item)",
            "schedule": "string (Note reference)",
            "confidence": "High/Medium/Low"
        }
    ],
    "balance_sheet": {
        "equity_and_liabilities": [
            {
                "heading": "string",
                "items": [
                    {"name": "string", "note_ref": "string or null", "current_year": number, "previous_year": number or null}
                ],
                "sub_total": {"current_year": number, "previous_year": number or null}
            }
        ],
        "assets": [
            {
                "heading": "string",
                "items": [
                    {"name": "string", "note_ref": "string or null", "current_year": number, "previous_year": number or null}
                ],
                "sub_total": {"current_year": number, "previous_year": number or null}
            }
        ],
        "total_equity_liabilities": {"current_year": number, "previous_year": number or null},
        "total_assets": {"current_year": number, "previous_year": number or null},
        "is_balanced": true
    },
    "profit_and_loss": {
        "income": [
            {"name": "string", "note_ref": "string or null", "current_year": number, "previous_year": number or null}
        ],
        "total_income": {"current_year": number, "previous_year": number or null},
        "expenses": [
            {"name": "string", "note_ref": "string or null", "current_year": number, "previous_year": number or null}
        ],
        "total_expenses": {"current_year": number, "previous_year": number or null},
        "profit_before_tax": {"current_year": number, "previous_year": number or null},
        "tax_expense": {"current_year": number or null, "previous_year": number or null},
        "profit_after_tax": {"current_year": number, "previous_year": number or null}
    },
    "schedules": [
        {
            "note_ref": "string",
            "title": "string",
            "items": [
                {"description": "string", "current_year": number, "previous_year": number or null}
            ],
            "total": {"current_year": number, "previous_year": number or null}
        }
    ],
    "warnings": ["string"]
}"""


# ─── Text extraction helper ───────────────────────────
async def _extract_text(file_bytes: bytes, filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext in ("xlsx", "xls", "csv"):
        try:
            return file_bytes.decode("utf-8", errors="replace")
        except Exception:
            return file_bytes.decode("latin-1", errors="replace")

    if not settings.LLAMA_CLOUD_API_KEY:
        raise ValueError("LLAMA_CLOUD_API_KEY not configured")
    try:
        from llama_cloud import AsyncLlamaCloud
    except ImportError:
        raise ImportError("llama_cloud not installed. pip install llama_cloud>=1.0")

    import tempfile
    client = AsyncLlamaCloud(api_key=settings.LLAMA_CLOUD_API_KEY)
    with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    file_obj = await client.files.create(file=tmp_path, purpose="parse")
    result = await client.parsing.parse(
        file_id=file_obj.id, tier="agentic", version="latest",
        output_options={"markdown": {"tables": {"output_tables_as_markdown": True}}},
        expand=["text", "markdown"],
    )
    pages = []
    if result.markdown and result.markdown.pages:
        pages = [p.markdown or "" for p in result.markdown.pages]
    elif result.text and result.text.pages:
        pages = [p.text or "" for p in result.text.pages]
    return "\n\n--- PAGE BREAK ---\n\n".join(pages)


async def _parse_document(prompt: str, text: str, max_tokens: int = 16000) -> dict:
    """Rule-based parser — routes to appropriate parser based on prompt content.
    Deterministic, no AI/LLM calls."""
    prompt_lower = prompt.lower()[:200]
    if 'trial balance' in prompt_lower:
        return parse_trial_balance(text)
    elif 'balance sheet' in prompt_lower:
        return parse_balance_sheet(text)
    elif 'mapping' in prompt_lower or 'generate' in prompt_lower:
        # TB → BS/PL mapping
        # text contains combined TB + prev BS data
        tb_data = parse_trial_balance(text)
        return map_tb_to_schedule_iii(tb_data)
    else:
        return parse_trial_balance(text)


# ─── Supabase upload ──────────────────────────────────
def _upload_to_supabase(file_bytes: bytes, job_id: str, filename: str) -> str:
    try:
        from supabase import create_client
        sb = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        bucket = "financial-statements"
        try:
            sb.storage.create_bucket(bucket, options={"public": False})
        except Exception:
            pass
        path = f"{job_id}/{filename}"
        ct = "application/pdf" if filename.lower().endswith(".pdf") else "application/octet-stream"
        sb.storage.from_(bucket).upload(path=path, file=file_bytes, file_options={"content-type": ct, "upsert": "true"})
        return path
    except Exception as e:
        logger.warning(f"Supabase upload failed (non-fatal): {e}")
        return ""


# ─── Tally TB → structured data ───────────────────────
def _tally_ledgers_to_tb(ledgers: list) -> dict:
    """Convert raw Tally ledger rows into the same structured TB format
    that the rule-based parser produces, so the downstream generation
    receives identical input regardless of source.
    Uses the canonical TALLY_TO_SCHEDULE_III mapping from the tally module."""
    # Tally convention: positive closing_balance = Credit, negative = Debit
    accounts = []
    total_dr = 0
    total_cr = 0
    for l in ledgers:
        cb = float(l.get('closing_balance', 0) or 0)
        ob = float(l.get('opening_balance', 0) or 0)
        parent = (l.get('parent') or '').strip()
        category, schedule_group, note_ref = _match_group(parent)
        dr = abs(cb) if cb < 0 else None
        cr = cb if cb > 0 else None
        total_dr += dr or 0
        total_cr += cr or 0
        accounts.append({
            'account_name': l.get('name', ''),
            'tally_group': l.get('parent', ''),
            'debit': dr,
            'credit': cr,
            'net_balance': cb,
            'opening_balance': ob,
            'category': category,
            'schedule_group': schedule_group,
            'sub_group': None,
        })
    return {
        'company_name': ledgers[0].get('company_name') if ledgers else None,
        'period': None,
        'as_on_date': None,
        'accounts': accounts,
        'total_debit': total_dr,
        'total_credit': total_cr,
        'source': 'tally',
    }


# ─── Background processor (DB-backed) ─────────────────
async def _process_job(job_id: str, files_data: dict):
    """Background: extract all files → structure TB → structure prev BS → generate statements.
    All progress written to PostgreSQL — survives restarts."""
    async with AsyncSessionLocal() as db:
        try:
            row = (await db.execute(
                select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
            )).scalar_one()

            # 1. Extract text from each uploaded file
            row.status = "extracting"
            await db.commit()

            extracted = {}
            file_paths = {}
            for role, (file_bytes, filename) in files_data.items():
                path = _upload_to_supabase(file_bytes, job_id, filename)
                if path:
                    file_paths[role] = path
                text = await _extract_text(file_bytes, filename)
                extracted[role] = text
                logger.info(f"Extracted {role}: {len(text)} chars from {filename}")

            row.raw_texts = {k: v[:3000] for k, v in extracted.items()}  # Store preview
            row.file_paths = file_paths
            await db.commit()

            # 2. Structure Trial Balance
            row.status = "parsing_tb"
            await db.commit()
            tb_structured = await _parse_document(TB_EXTRACTION_PROMPT, extracted.get("trial_balance", ""))
            row.trial_balance_data = tb_structured
            if tb_structured.get("company_name"):
                row.company_name = tb_structured["company_name"]
            await db.commit()

            # 3. Structure Previous Year BS (if provided)
            prev_bs = None
            if "prev_balance_sheet" in extracted and extracted["prev_balance_sheet"].strip():
                row.status = "parsing_bs"
                await db.commit()
                prev_bs = await _parse_document(BS_EXTRACTION_PROMPT, extracted["prev_balance_sheet"])
                row.prev_bs_data = prev_bs
                await db.commit()

            # 4. Generate mapped financial statements
            row.status = "generating"
            await db.commit()
            combined_input = "=== CURRENT YEAR TRIAL BALANCE ===\n"
            combined_input += json.dumps(tb_structured, default=str) + "\n\n"
            if prev_bs:
                combined_input += "=== PREVIOUS YEAR BALANCE SHEET ===\n"
                combined_input += json.dumps(prev_bs, default=str) + "\n\n"
            if "notes" in extracted and extracted["notes"].strip():
                combined_input += "=== NOTES TO ACCOUNTS ===\n"
                combined_input += extracted["notes"] + "\n"

            result = await _call_claude(MAPPING_AND_GENERATION_PROMPT, combined_input, max_tokens=16000)

            # Extract metadata from result
            is_balanced = None
            if result.get("balance_sheet"):
                is_balanced = result["balance_sheet"].get("is_balanced")

            row.result = result
            row.status = "completed"
            row.is_balanced = is_balanced
            if result.get("company_name"):
                row.company_name = result["company_name"]
            if result.get("current_year_date"):
                row.financial_year = result["current_year_date"][:4] if result["current_year_date"] else None
            await db.commit()
            logger.info(f"✅ FS Job {job_id} completed")

        except Exception as e:
            logger.error(f"❌ FS Job {job_id} failed: {e}")
            row = (await db.execute(
                select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
            )).scalar_one_or_none()
            if row:
                row.status = "failed"
                row.error_message = str(e)[:2000]
                await db.commit()


# ─── Background processor for Tally-sourced TB ────────
async def _process_tally_job(job_id: str, tally_tb: dict, files_data: dict):
    """Background: use pre-structured tally TB, optionally parse uploaded BS/Notes,
    then generate statements. Same output format as upload path."""
    async with AsyncSessionLocal() as db:
        try:
            row = (await db.execute(
                select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
            )).scalar_one()

            # TB already structured from Tally
            row.status = "parsing_tb"
            row.trial_balance_data = tally_tb
            if tally_tb.get('company_name'):
                row.company_name = tally_tb['company_name']
            await db.commit()

            # Extract uploaded files if any (prev BS, notes)
            prev_bs = None
            extracted_notes = ""
            if files_data:
                row.status = "extracting"
                await db.commit()
                file_paths = {}
                for role, (file_bytes, filename) in files_data.items():
                    path = _upload_to_supabase(file_bytes, job_id, filename)
                    if path:
                        file_paths[role] = path
                    text = await _extract_text(file_bytes, filename)
                    if role == 'prev_balance_sheet':
                        row.status = "parsing_bs"
                        await db.commit()
                        prev_bs = await _parse_document(BS_EXTRACTION_PROMPT, text)
                        row.prev_bs_data = prev_bs
                    elif role == 'notes':
                        extracted_notes = text
                    logger.info(f"Extracted {role}: {len(text)} chars from {filename}")
                row.file_paths = file_paths
                await db.commit()

            # Generate statements
            row.status = "generating"
            await db.commit()
            combined_input = "=== CURRENT YEAR TRIAL BALANCE (from Tally) ===\n"
            combined_input += json.dumps(tally_tb, default=str) + "\n\n"
            if prev_bs:
                combined_input += "=== PREVIOUS YEAR BALANCE SHEET ===\n"
                combined_input += json.dumps(prev_bs, default=str) + "\n\n"
            if extracted_notes:
                combined_input += "=== NOTES TO ACCOUNTS ===\n"
                combined_input += extracted_notes + "\n"

            result = await _parse_document(MAPPING_AND_GENERATION_PROMPT, combined_input, max_tokens=16000)

            is_balanced = None
            if result.get("balance_sheet"):
                is_balanced = result["balance_sheet"].get("is_balanced")

            row.result = result
            row.status = "completed"
            row.is_balanced = is_balanced
            if result.get("company_name"):
                row.company_name = result["company_name"]
            if result.get("current_year_date"):
                row.financial_year = result["current_year_date"][:4] if result["current_year_date"] else None
            await db.commit()
            logger.info(f"✅ FS Tally Job {job_id} completed")

        except Exception as e:
            logger.error(f"❌ FS Tally Job {job_id} failed: {e}")
            row = (await db.execute(
                select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
            )).scalar_one_or_none()
            if row:
                row.status = "failed"
                row.error_message = str(e)[:2000]
                await db.commit()


# ─── Tally TB Auto-Pull Endpoint ──────────────────────
@router.get("/tally-tb")
async def get_tally_trial_balance(
    company_name: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Pull Trial Balance directly from synced Tally ledgers.
    Returns structured TB in the same format as AI extraction."""
    q = select(Ledger).where(Ledger.company_name == company_name).order_by(Ledger.parent, Ledger.name)
    result = await db.execute(q)
    ledgers = result.scalars().all()
    if not ledgers:
        raise HTTPException(404, f"No Tally ledgers found for company '{company_name}'")

    # Convert to dict list for the conversion function
    ledger_dicts = [{
        'name': l.name,
        'parent': l.parent or '',
        'opening_balance': float(l.opening_balance) if l.opening_balance else 0,
        'closing_balance': float(l.closing_balance) if l.closing_balance else 0,
        'company_name': l.company_name,
    } for l in ledgers]

    tb = _tally_ledgers_to_tb(ledger_dicts)
    count_q = select(func.count(Ledger.id)).where(Ledger.company_name == company_name)
    total = (await db.execute(count_q)).scalar() or 0
    last_sync = max((l.synced_at for l in ledgers if l.synced_at), default=None)

    return {
        **tb,
        'ledger_count': total,
        'last_synced': last_sync.isoformat() if last_sync else None,
    }


# ─── Generate from Tally TB (JSON body) ───────────────
class TallyGenerateRequest(BaseModel):
    client_id: str
    company_name: str


@router.post("/generate-from-tally")
async def generate_from_tally(
    req: TallyGenerateRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Generate financial statements from Tally-synced ledgers. No file upload needed."""
    # Pull ledgers
    q = select(Ledger).where(Ledger.company_name == req.company_name).order_by(Ledger.parent, Ledger.name)
    result = await db.execute(q)
    ledgers = result.scalars().all()
    if not ledgers:
        raise HTTPException(404, f"No Tally ledgers found for '{req.company_name}'")

    ledger_dicts = [{
        'name': l.name,
        'parent': l.parent or '',
        'opening_balance': float(l.opening_balance) if l.opening_balance else 0,
        'closing_balance': float(l.closing_balance) if l.closing_balance else 0,
        'company_name': l.company_name,
    } for l in ledgers]

    tally_tb = _tally_ledgers_to_tb(ledger_dicts)

    # Create job
    job_id = str(uuid.uuid4())
    row = FinancialStatementJob(
        id=job_id,
        client_id=req.client_id,
        user_id=str(current_user.id),
        filenames={'source': 'tally', 'company': req.company_name},
        file_hash=hashlib.sha256(json.dumps(ledger_dicts, default=str).encode()).hexdigest(),
        status="processing",
        company_name=req.company_name,
    )
    db.add(row)
    await db.commit()

    background_tasks.add_task(_process_tally_job, job_id, tally_tb, {})

    return {
        'id': job_id,
        'status': 'processing',
        'message': f'Generating from {len(ledgers)} Tally ledgers. ~30-60 seconds.',
        'ledger_count': len(ledgers),
    }


# ─── Upload Endpoint ──────────────────────────────────
@router.post("/generate")
async def generate_financial_statements(
    background_tasks: BackgroundTasks,
    trial_balance: UploadFile = File(...),
    client_id: str = Form(...),
    prev_balance_sheet: UploadFile = File(None),
    notes: UploadFile = File(None),
    prev_trial_balance: UploadFile = File(None),
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Upload TB (required) + prev BS + Notes + prev TB (optional) to generate financial statements."""

    if not trial_balance.filename:
        raise HTTPException(400, "Trial Balance file is required")

    # Read all files
    files_data = {}
    tb_bytes = await trial_balance.read()
    if len(tb_bytes) > MAX_FILE_SIZE:
        raise HTTPException(400, "Trial Balance file too large (max 25 MB)")
    if len(tb_bytes) == 0:
        raise HTTPException(400, "Trial Balance file is empty")
    files_data["trial_balance"] = (tb_bytes, trial_balance.filename)

    if prev_balance_sheet and prev_balance_sheet.filename:
        bs_bytes = await prev_balance_sheet.read()
        if len(bs_bytes) > 0:
            files_data["prev_balance_sheet"] = (bs_bytes, prev_balance_sheet.filename)

    if notes and notes.filename:
        notes_bytes = await notes.read()
        if len(notes_bytes) > 0:
            files_data["notes"] = (notes_bytes, notes.filename)

    if prev_trial_balance and prev_trial_balance.filename:
        ptb_bytes = await prev_trial_balance.read()
        if len(ptb_bytes) > 0:
            files_data["prev_trial_balance"] = (ptb_bytes, prev_trial_balance.filename)

    # Duplicate detection via TB hash
    file_hash = hashlib.sha256(tb_bytes).hexdigest()
    existing = (await db.execute(
        select(FinancialStatementJob).where(
            FinancialStatementJob.client_id == client_id,
            FinancialStatementJob.file_hash == file_hash,
            FinancialStatementJob.status == "completed",
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(
            409,
            f"This Trial Balance was already processed on "
            f"{existing.created_at.strftime('%d %b %Y') if existing.created_at else 'unknown date'}. "
            f"View the existing result or delete it to re-process."
        )

    job_id = str(uuid.uuid4())
    filenames = {k: v[1] for k, v in files_data.items()}

    row = FinancialStatementJob(
        id=job_id,
        client_id=client_id,
        user_id=str(current_user.id),
        filenames=filenames,
        file_hash=file_hash,
        status="processing",
    )
    db.add(row)
    await db.commit()

    background_tasks.add_task(_process_job, job_id, files_data)

    return {
        "id": job_id,
        "status": "processing",
        "message": f"Processing {len(files_data)} file(s). This may take 60-90 seconds.",
    }


# ─── Status ───────────────────────────────────────────
@router.get("/status/{job_id}")
async def get_job_status(
    job_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    row = (await db.execute(
        select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Job not found")
    return {
        "id": str(row.id),
        "status": row.status,
        "filenames": row.filenames,
        "error": row.error_message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "has_result": row.result is not None,
        "company_name": row.company_name,
        "is_balanced": row.is_balanced,
    }


# ─── Get Result ───────────────────────────────────────
@router.get("/result/{job_id}")
async def get_result(
    job_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    row = (await db.execute(
        select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Job not found")
    if row.status != "completed":
        raise HTTPException(400, f"Not ready. Status: {row.status}")
    r = row.result or {}
    return {
        "balance_sheet": r.get("balance_sheet"),
        "profit_and_loss": r.get("profit_and_loss"),
        "schedules": r.get("schedules"),
        "account_mappings": r.get("account_mappings"),
        "warnings": r.get("warnings", []),
        "company_name": r.get("company_name") or row.company_name,
        "current_year_date": r.get("current_year_date"),
        "previous_year_date": r.get("previous_year_date"),
    }


# ─── Get Trial Balance ────────────────────────────────
@router.get("/trial-balance/{job_id}")
async def get_trial_balance(
    job_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    row = (await db.execute(
        select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Job not found")
    if not row.trial_balance_data:
        raise HTTPException(400, "Trial balance not parsed yet")
    return row.trial_balance_data


# ─── List Jobs ────────────────────────────────────────
@router.get("/")
async def list_jobs(
    client_id: str = None,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    query = select(FinancialStatementJob)
    if client_id:
        query = query.where(FinancialStatementJob.client_id == client_id)
    query = query.order_by(FinancialStatementJob.created_at.desc())

    result = await db.execute(query)
    rows = result.scalars().all()
    return [
        {
            "id": str(r.id),
            "client_id": str(r.client_id),
            "filenames": r.filenames,
            "status": r.status,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "has_result": r.result is not None,
            "company_name": r.company_name,
            "financial_year": r.financial_year,
            "is_balanced": r.is_balanced,
        }
        for r in rows
    ]


# ─── Delete Job ───────────────────────────────────────
@router.delete("/{job_id}")
async def delete_job(
    job_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    row = (await db.execute(
        select(FinancialStatementJob).where(FinancialStatementJob.id == job_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Job not found")

    # Clean up Supabase storage
    if row.filenames:
        try:
            from supabase import create_client
            client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
            paths = [f"{job_id}/{fn}" for fn in row.filenames.values()]
            client.storage.from_("financial-statements").remove(paths)
            logger.info(f"🗑️ Deleted {len(paths)} Supabase files for job {job_id}")
        except Exception as e:
            logger.warning(f"Supabase cleanup failed (non-fatal): {e}")

    await db.delete(row)
    await db.commit()
    return {"message": "Deleted"}
