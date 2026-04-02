"""
Financial Instruments API
─────────────────────────
Endpoints for uploading and parsing Demat, Mutual Fund, and PMS statements.
AI extracts holdings, transactions, dividends, capital gains → journal entries.
"""

import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import openai
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.api import deps
from app.core.config import settings
from app.models.models import User

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 25 * 1024 * 1024  # 25 MB


# ─── OpenAI Structuring Prompts ────────────────────────

DEMAT_PROMPT = """You are a financial data extraction AI specialising in Indian Demat account statements (CDSL/NSDL).

Given the raw text from a Demat statement, extract ALL holdings and transactions into strict JSON.

**Rules:**
1. Extract EVERY holding and transaction — do not skip any row.
2. Dates must be ISO format: "YYYY-MM-DD"
3. Amounts must be plain numbers (no commas, no ₹) or null.
4. Classify each transaction: Buy, Sell, Bonus, Split, Rights, Transfer In, Transfer Out, IPO Allotment, Dividend
5. For capital gains, compute buy_value, sell_value, gain_loss where possible.

Return this exact JSON:
{
    "dp_id": "string or null",
    "client_id": "string or null",
    "depository": "CDSL or NSDL or null",
    "statement_date": "YYYY-MM-DD or null",
    "holdings": [
        {
            "isin": "string",
            "scrip_name": "string",
            "quantity": number,
            "avg_cost": number or null,
            "market_value": number or null,
            "market_price": number or null
        }
    ],
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "isin": "string or null",
            "scrip_name": "string",
            "type": "Buy/Sell/Bonus/Dividend/etc",
            "quantity": number or null,
            "price": number or null,
            "amount": number or null,
            "buy_value": number or null,
            "sell_value": number or null,
            "gain_loss": number or null,
            "holding_period": "Short Term or Long Term or null"
        }
    ],
    "dividends": [
        {
            "date": "YYYY-MM-DD",
            "scrip_name": "string",
            "amount": number,
            "tds_deducted": number or null
        }
    ],
    "capital_gains_summary": {
        "short_term_gain": number or null,
        "long_term_gain": number or null,
        "total_gain": number or null
    }
}"""

MF_PROMPT = """You are a financial data extraction AI specialising in Indian Mutual Fund statements (CAS / AMC statements).

Given the raw text from a Mutual Fund statement, extract ALL fund holdings and transactions.

**Rules:**
1. Extract EVERY fund, transaction, and dividend — do not skip any.
2. Dates: "YYYY-MM-DD". Amounts: plain numbers, no commas/₹.
3. Classify transactions: Purchase, Redemption, SIP, STP, Switch In, Switch Out, Dividend Payout, Dividend Reinvestment
4. Compute capital gains where possible.

Return this exact JSON:
{
    "investor_name": "string or null",
    "pan": "string or null",
    "statement_period_start": "YYYY-MM-DD or null",
    "statement_period_end": "YYYY-MM-DD or null",
    "funds": [
        {
            "fund_name": "string",
            "amc": "string or null",
            "folio_number": "string or null",
            "scheme_type": "Equity/Debt/Hybrid/ELSS/Liquid/etc",
            "units": number or null,
            "nav": number or null,
            "market_value": number or null,
            "cost_value": number or null
        }
    ],
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "fund_name": "string",
            "type": "Purchase/Redemption/SIP/STP/Switch/Dividend",
            "units": number or null,
            "nav": number or null,
            "amount": number or null,
            "stamp_duty": number or null,
            "stt": number or null
        }
    ],
    "dividends": [
        {
            "date": "YYYY-MM-DD",
            "fund_name": "string",
            "amount": number,
            "tds_deducted": number or null
        }
    ],
    "capital_gains_summary": {
        "short_term_gain": number or null,
        "long_term_gain": number or null,
        "total_gain": number or null
    }
}"""

PMS_PROMPT = """You are a financial data extraction AI specialising in Indian Portfolio Management Service (PMS) statements.

Given the raw text from a PMS statement, extract ALL holdings, transactions, fees, and performance data.

**Rules:**
1. Extract EVERY holding, transaction, fee entry — do not skip any.
2. Dates: "YYYY-MM-DD". Amounts: plain numbers only.
3. Classify transactions: Buy, Sell, Dividend, Corporate Action, Fee Deduction
4. Track PMS-specific fields: management fees, performance fees, brokerage.

Return this exact JSON:
{
    "portfolio_name": "string or null",
    "pms_provider": "string or null",
    "client_name": "string or null",
    "statement_date": "YYYY-MM-DD or null",
    "portfolio_value": number or null,
    "invested_value": number or null,
    "holdings": [
        {
            "scrip_name": "string",
            "isin": "string or null",
            "quantity": number,
            "avg_cost": number or null,
            "market_price": number or null,
            "market_value": number or null,
            "weight_pct": number or null,
            "unrealised_gain": number or null
        }
    ],
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "scrip_name": "string",
            "type": "Buy/Sell/Dividend/Corporate Action/Fee",
            "quantity": number or null,
            "price": number or null,
            "amount": number or null,
            "brokerage": number or null,
            "gain_loss": number or null
        }
    ],
    "fees": [
        {
            "date": "YYYY-MM-DD",
            "type": "Management Fee/Performance Fee/Brokerage/Custodian Fee/Other",
            "amount": number,
            "gst_on_fee": number or null
        }
    ],
    "capital_gains_summary": {
        "short_term_gain": number or null,
        "long_term_gain": number or null,
        "total_gain": number or null
    }
}"""

JOURNAL_ENTRY_PROMPT = """You are an expert Indian Chartered Accountant. Given the structured financial instrument data below,
generate proper double-entry journal entries following Indian accounting standards.

**Rules:**
1. Use standard Indian ledger names (e.g., "Investment in Equity Shares", "Dividend Income", "STCG on Shares", "LTCG on Equity MF")
2. Each entry must balance (total Dr = total Cr)
3. Include narration for each entry
4. Handle TDS on dividends (TDS Receivable Dr, Dividend Income Cr)
5. For capital gains, separate STCG and LTCG
6. For PMS fees, debit "PMS Management Fees" / "PMS Performance Fees" and credit the PMS account
7. Assign the correct Tally voucher_type:
   - Share/MF purchases → "Purchase"
   - Share/MF sales → "Sales"
   - Dividend/interest received → "Receipt"
   - Brokerage/DP charges/fees paid → "Payment"
   - Capital gain/loss entries, transfers, adjustments → "Journal"

Return JSON:
{
    "journal_entries": [
        {
            "date": "YYYY-MM-DD",
            "voucher_type": "Purchase" or "Sales" or "Receipt" or "Payment" or "Journal",
            "narration": "string",
            "ledger_entries": [
                {"ledger_name": "string", "amount": number, "side": "Dr" or "Cr"}
            ]
        }
    ]
}"""


# ─── Helper: Extract text from uploaded file ───────────
async def _extract_text(file_bytes: bytes, filename: str) -> str:
    """Extract text from PDF/Excel using LlamaParse."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext in ("xlsx", "xls", "csv"):
        # For spreadsheets, decode directly
        try:
            return file_bytes.decode("utf-8", errors="replace")
        except Exception:
            return file_bytes.decode("latin-1", errors="replace")

    # PDF — use LlamaParse
    if not settings.LLAMA_CLOUD_API_KEY:
        raise ValueError("LLAMA_CLOUD_API_KEY not configured")

    try:
        from llama_cloud import AsyncLlamaCloud
    except ImportError:
        raise ImportError("llama_cloud package not installed. Run: pip install llama_cloud>=1.0")

    import tempfile
    client = AsyncLlamaCloud(api_key=settings.LLAMA_CLOUD_API_KEY)

    with tempfile.NamedTemporaryFile(suffix=f"_{filename}", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    logger.info(f"Uploading {filename} ({len(file_bytes)} bytes) to LlamaParse...")

    try:
        file_obj = await client.files.create(file=tmp_path, purpose="parse")
        result = await client.parsing.parse(
            file_id=file_obj.id, tier="agentic", version="latest",
            output_options={"markdown": {"tables": {"output_tables_as_markdown": True}}},
            processing_options={"ocr_parameters": {"languages": ["en"]}},
            expand=["text", "markdown"],
        )

        pages_text = []
        if result.markdown and result.markdown.pages:
            pages_text = [p.markdown or "" for p in result.markdown.pages]
        elif result.text and result.text.pages:
            pages_text = [p.text or "" for p in result.text.pages]

        full_text = "\n\n--- PAGE BREAK ---\n\n".join(pages_text)
        logger.info(f"LlamaParse: {len(full_text)} chars from {len(pages_text)} pages")
        return full_text
    except Exception as e:
        logger.error(f"LlamaParse extraction failed: {e}")
        raise


async def _structure_with_openai(text: str, instrument_type: str) -> dict:
    """Use OpenAI to structure extracted text based on instrument type."""
    prompts = {"demat": DEMAT_PROMPT, "mutual_fund": MF_PROMPT, "pms": PMS_PROMPT}
    prompt = prompts.get(instrument_type, DEMAT_PROMPT)

    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=16000,
    )
    return json.loads(response.choices[0].message.content)


async def _generate_journal_entries(structured_data: dict) -> dict:
    """Generate journal entries from structured instrument data."""
    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": JOURNAL_ENTRY_PROMPT},
            {"role": "user", "content": json.dumps(structured_data, default=str)},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=8000,
    )
    result = json.loads(response.choices[0].message.content)
    # Validate entries before returning
    result["journal_entries"] = _validate_journal_entries(result.get("journal_entries", []))
    return result


def _validate_journal_entries(entries: list) -> list:
    """Validate AI-generated journal entries: balance check, amount sanity, date format."""
    valid_voucher_types = {"Purchase", "Sales", "Receipt", "Payment", "Journal"}
    validated = []
    for je in entries:
        ledger_entries = je.get("ledger_entries", [])
        if not ledger_entries:
            continue

        # Clean up amounts — force numeric
        for le in ledger_entries:
            try:
                le["amount"] = round(float(le.get("amount", 0)), 2)
            except (ValueError, TypeError):
                le["amount"] = 0
            if le.get("side") not in ("Dr", "Cr"):
                le["side"] = "Dr"  # default

        # Remove zero-amount entries
        ledger_entries = [le for le in ledger_entries if abs(le["amount"]) > 0.01]
        if len(ledger_entries) < 2:
            continue  # need at least one Dr and one Cr

        je["ledger_entries"] = ledger_entries

        # Balance check: total Dr must equal total Cr
        total_dr = sum(le["amount"] for le in ledger_entries if le["side"] == "Dr")
        total_cr = sum(le["amount"] for le in ledger_entries if le["side"] == "Cr")
        if abs(total_dr - total_cr) > 1.0:  # Allow ₹1 rounding tolerance
            je["narration"] = f"⚠ UNBALANCED (Dr={total_dr:.2f}, Cr={total_cr:.2f}) — " + (je.get("narration") or "")
            logger.warning(f"Unbalanced journal entry: Dr={total_dr}, Cr={total_cr}")

        # Validate voucher_type
        if je.get("voucher_type") not in valid_voucher_types:
            je["voucher_type"] = "Journal"  # safe default

        validated.append(je)

    return validated


# ─── Upload PDF to Supabase ────────────────────────────
def _upload_to_supabase(file_bytes: bytes, upload_id: str, filename: str) -> str:
    try:
        from supabase import create_client
        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        bucket = "financial-instruments"
        try:
            client.storage.create_bucket(bucket, options={"public": False})
        except Exception:
            pass
        storage_path = f"{upload_id}/{filename}"
        content_type = "application/pdf" if filename.lower().endswith(".pdf") else "application/octet-stream"
        client.storage.from_(bucket).upload(
            path=storage_path, file=file_bytes,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        return storage_path
    except Exception as e:
        logger.warning(f"Supabase upload failed (non-fatal): {e}")
        return ""


# ─── DB-backed storage ─────────────────────────────────
# Uses FinancialInstrumentUpload model — survives restarts, supports queries

from app.db.session import AsyncSessionLocal
from app.models.models import FinancialInstrumentUpload
import hashlib


# ─── Background processor ─────────────────────────────
async def _process_upload(upload_id: str, file_bytes: bytes, filename: str, instrument_type: str):
    """Background: extract → structure → journal entries. Writes progress to DB."""
    async with AsyncSessionLocal() as db:
        try:
            # Update status: extracting
            row = (await db.execute(select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id))).scalar_one()
            row.status = "extracting"
            await db.commit()

            _upload_to_supabase(file_bytes, upload_id, filename)

            raw_text = await _extract_text(file_bytes, filename)

            # Update status: structuring
            row.status = "structuring"
            row.raw_text = raw_text[:2000]
            await db.commit()

            structured = await _structure_with_openai(raw_text, instrument_type)
            row.structured_data = structured
            row.status = "generating_entries"
            await db.commit()

            journal = await _generate_journal_entries(structured)
            entries = journal.get("journal_entries", [])
            row.journal_entries = entries
            row.journal_entry_count = len(entries)
            row.status = "completed"
            await db.commit()

            logger.info(f"✅ FI Upload {upload_id}: {len(entries)} journal entries")

        except Exception as e:
            logger.error(f"❌ FI Upload {upload_id} failed: {e}")
            row = (await db.execute(select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id))).scalar_one_or_none()
            if row:
                row.status = "failed"
                row.error_message = str(e)[:2000]
                await db.commit()


# ─── Upload Endpoint ──────────────────────────────────
@router.post("/upload")
async def upload_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_id: str = Form(...),
    instrument_type: str = Form(...),  # demat, mutual_fund, pms
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Upload a Demat/MF/PMS statement for AI processing."""

    if instrument_type not in ("demat", "mutual_fund", "pms"):
        raise HTTPException(400, "instrument_type must be: demat, mutual_fund, or pms")

    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("pdf", "xlsx", "xls", "csv"):
        raise HTTPException(400, f"Supported formats: PDF, XLSX, XLS, CSV. Got .{ext}")

    file_bytes = await file.read()
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large ({len(file_bytes)/(1024*1024):.1f} MB). Max 25 MB.")
    if len(file_bytes) == 0:
        raise HTTPException(400, "File is empty")

    # Compute file hash for duplicate detection
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # Check for duplicate: same file already uploaded for this client
    existing = (await db.execute(
        select(FinancialInstrumentUpload).where(
            FinancialInstrumentUpload.client_id == client_id,
            FinancialInstrumentUpload.file_hash == file_hash,
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(
            409,
            f"Duplicate file — this statement was already uploaded as \"{existing.filename}\" "
            f"on {existing.created_at.strftime('%d %b %Y') if existing.created_at else 'unknown date'}."
        )

    upload_id = str(uuid.uuid4())
    row = FinancialInstrumentUpload(
        id=upload_id,
        client_id=client_id,
        user_id=str(current_user.id),
        instrument_type=instrument_type,
        filename=file.filename,
        file_hash=file_hash,
        status="processing",
    )
    db.add(row)
    await db.commit()

    background_tasks.add_task(_process_upload, upload_id, file_bytes, file.filename, instrument_type)

    return {
        "id": upload_id,
        "status": "processing",
        "message": f"Processing {file.filename} as {instrument_type}. This may take 30-60 seconds.",
    }


# ─── Status Endpoint ──────────────────────────────────
@router.get("/status/{upload_id}")
async def get_upload_status(
    upload_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Check the status of a financial instrument upload."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Upload not found")
    return {
        "id": str(row.id),
        "status": row.status,
        "instrument_type": row.instrument_type,
        "filename": row.filename,
        "error": row.error_message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "has_data": row.structured_data is not None,
        "journal_entry_count": row.journal_entry_count or 0,
    }


# ─── Get Structured Data ─────────────────────────────
@router.get("/data/{upload_id}")
async def get_structured_data(
    upload_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get the AI-parsed structured data from an upload."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Upload not found")
    if row.status not in ("completed", "generating_entries"):
        raise HTTPException(400, f"Data not ready. Current status: {row.status}")
    return {
        "instrument_type": row.instrument_type,
        "structured_data": row.structured_data,
    }


# ─── Get Journal Entries ──────────────────────────────
@router.get("/journal-entries/{upload_id}")
async def get_journal_entries(
    upload_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get generated journal entries for an upload."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Upload not found")
    if row.status != "completed":
        raise HTTPException(400, f"Entries not ready. Status: {row.status}")
    return {"journal_entries": row.journal_entries or []}


# ─── List Uploads ─────────────────────────────────────
@router.get("/")
async def list_uploads(
    client_id: str = None,
    instrument_type: str = None,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """List all financial instrument uploads."""
    query = select(FinancialInstrumentUpload)
    if client_id:
        query = query.where(FinancialInstrumentUpload.client_id == client_id)
    if instrument_type:
        query = query.where(FinancialInstrumentUpload.instrument_type == instrument_type)
    query = query.order_by(FinancialInstrumentUpload.created_at.desc())

    result = await db.execute(query)
    rows = result.scalars().all()
    return [
        {
            "id": str(r.id),
            "client_id": str(r.client_id),
            "instrument_type": r.instrument_type,
            "filename": r.filename,
            "status": r.status,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "journal_entry_count": r.journal_entry_count or 0,
        }
        for r in rows
    ]


# ─── Delete Upload (with Supabase cleanup) ────────────
@router.delete("/{upload_id}")
async def delete_upload(
    upload_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Delete a financial instrument upload and its Supabase file."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Upload not found")

    # Clean up Supabase storage
    storage_path = f"{upload_id}/{row.filename}"
    try:
        from supabase import create_client
        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        client.storage.from_("financial-instruments").remove([storage_path])
        logger.info(f"🗑️ Deleted Supabase file: {storage_path}")
    except Exception as e:
        logger.warning(f"Supabase cleanup failed (non-fatal): {e}")

    await db.delete(row)
    await db.commit()
    return {"message": "Deleted"}


# ─── Get PDF URL ──────────────────────────────────
@router.get("/pdf/{upload_id}")
async def get_upload_pdf(
    upload_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get a signed URL for the uploaded statement PDF."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Upload not found")

    storage_path = f"{upload_id}/{row.filename}"
    try:
        from supabase import create_client
        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        signed = client.storage.from_("financial-instruments").create_signed_url(
            path=storage_path, expires_in=3600
        )
        url = signed.get("signedURL") or signed.get("signedUrl") or ""
        if not url:
            raise ValueError("No signed URL returned")
        return {"url": url, "filename": row.filename}
    except Exception as e:
        logger.warning(f"PDF URL generation failed: {e}")
        raise HTTPException(400, f"Could not generate PDF URL: {str(e)}")

