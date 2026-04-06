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

    if ext == "csv":
        try:
            return file_bytes.decode("utf-8", errors="replace")
        except Exception:
            return file_bytes.decode("latin-1", errors="replace")

    if ext in ("xlsx", "xls"):
        # Parse Excel properly using openpyxl
        import io
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
            all_text = []
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                rows = []
                for row in ws.iter_rows(values_only=True):
                    cells = [str(c) if c is not None else "" for c in row]
                    rows.append(",".join(cells))
                all_text.append(f"--- Sheet: {sheet_name} ---\n" + "\n".join(rows))
            wb.close()
            result = "\n\n".join(all_text)
            logger.info(f"Excel extraction: {len(result)} chars from {len(wb.sheetnames)} sheets")
            return result
        except Exception as e:
            logger.error(f"openpyxl failed: {e}, falling back to raw decode")
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

    # Check if text is too large and needs chunking
    estimated_tokens = len(text) // 4  # rough: 1 token ≈ 4 chars
    if estimated_tokens > 20000:
        logger.info(f"Large file detected (~{estimated_tokens} tokens). Using chunked processing.")
        return await _structure_chunked(text, instrument_type, prompt)

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


async def _structure_chunked(text: str, instrument_type: str, prompt: str) -> dict:
    """Process large files by splitting into row-based chunks and merging results."""
    import asyncio

    lines = text.split("\n")

    # Find the header (first non-empty line or sheet header)
    header_lines = []
    data_lines = []
    in_header = True
    for line in lines:
        stripped = line.strip()
        if in_header:
            header_lines.append(line)
            # Once we hit a line with commas (data row), switch to data mode
            if stripped.count(",") >= 2 and not stripped.startswith("---"):
                in_header = False
                # The last header line is actually the first data row's header
                # Keep it as header for each chunk
        else:
            data_lines.append(line)

    # If no clear header/data split, just split by lines
    if len(data_lines) == 0:
        header_lines = lines[:2]  # first 2 lines as header
        data_lines = lines[2:]

    header_text = "\n".join(header_lines)
    CHUNK_SIZE = 300  # rows per chunk (keeps each ~12K tokens, well under 30K TPM)

    chunks = []
    for i in range(0, len(data_lines), CHUNK_SIZE):
        chunk_rows = data_lines[i:i + CHUNK_SIZE]
        chunk_text = header_text + "\n" + "\n".join(chunk_rows)
        chunks.append(chunk_text)

    logger.info(f"Split into {len(chunks)} chunks ({len(data_lines)} data rows, {CHUNK_SIZE} per chunk)")

    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    async def process_chunk(chunk_text: str, chunk_idx: int) -> dict:
        chunk_prompt = prompt + f"\n\nNOTE: This is chunk {chunk_idx + 1} of {len(chunks)}. Extract all data from this chunk."
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": chunk_prompt},
                        {"role": "user", "content": chunk_text},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                    max_tokens=16000,
                )
                result = json.loads(response.choices[0].message.content)
                logger.info(f"  Chunk {chunk_idx + 1}/{len(chunks)}: OK")
                return result
            except Exception as e:
                wait_time = 30 * (attempt + 1)  # 30s, 60s, 90s
                logger.warning(f"  Chunk {chunk_idx + 1} attempt {attempt + 1} failed: {e}. Retry in {wait_time}s")
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"  Chunk {chunk_idx + 1}/{len(chunks)} failed after {max_retries} retries")
                    return {}

    # Process chunks sequentially (1 at a time) with 30s gap to respect TPM limits
    all_results = []
    for i, chunk_text in enumerate(chunks):
        result = await process_chunk(chunk_text, i)
        all_results.append(result)
        if i < len(chunks) - 1:
            await asyncio.sleep(30)  # 30s gap between chunks to stay under 30K TPM

    # Merge all chunk results
    merged = _merge_structured_results(all_results, instrument_type)
    logger.info(f"Merged {len(all_results)} chunks → {sum(len(v) for v in merged.values() if isinstance(v, list))} total items")
    return merged


def _merge_structured_results(results: list, instrument_type: str) -> dict:
    """Merge structured data from multiple chunks into one."""
    merged = {}
    list_keys = {"holdings", "transactions", "dividends", "fees", "funds"}

    for r in results:
        if not r:
            continue
        for key, value in r.items():
            if key in list_keys and isinstance(value, list):
                merged.setdefault(key, []).extend(value)
            elif key == "capital_gains_summary" and isinstance(value, dict):
                existing = merged.get("capital_gains_summary", {})
                for k, v in value.items():
                    if v is not None:
                        existing[k] = (existing.get(k) or 0) + (v or 0)
                merged["capital_gains_summary"] = existing
            elif key not in merged and value is not None:
                merged[key] = value  # keep first non-null scalar (e.g., dp_id, client_name)

    return merged


async def _generate_journal_entries(structured_data: dict) -> dict:
    """Generate journal entries from structured instrument data. Chunks large datasets."""
    import asyncio

    data_str = json.dumps(structured_data, default=str)
    estimated_tokens = len(data_str) // 4

    # If small enough, process in one shot
    if estimated_tokens <= 20000:
        return await _generate_journal_entries_single(data_str)

    # Large dataset — chunk by transactions
    logger.info(f"Large structured data (~{estimated_tokens} tokens). Chunking journal generation.")
    transactions = structured_data.get("transactions", [])
    BATCH = 100
    all_entries = []

    for i in range(0, max(len(transactions), 1), BATCH):
        chunk_data = {k: v for k, v in structured_data.items() if k != "transactions"}
        chunk_data["transactions"] = transactions[i:i + BATCH]
        chunk_str = json.dumps(chunk_data, default=str)

        try:
            result = await _generate_journal_entries_single(chunk_str)
            entries = result.get("journal_entries", [])
            all_entries.extend(entries)
            logger.info(f"  JE batch {i // BATCH + 1}: {len(entries)} entries")
        except Exception as e:
            logger.error(f"  JE batch {i // BATCH + 1} failed: {e}")

        if i + BATCH < len(transactions):
            await asyncio.sleep(2)

    validated = _validate_journal_entries(all_entries)
    return {"journal_entries": validated}


async def _generate_journal_entries_single(data_str: str) -> dict:
    """Generate journal entries for a single chunk."""
    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": JOURNAL_ENTRY_PROMPT},
            {"role": "user", "content": data_str},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
        max_tokens=16000,
    )
    result = json.loads(response.choices[0].message.content)
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
            row = (await db.execute(select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id))).scalar_one()
            row.status = "extracting"
            await db.commit()

            _upload_to_supabase(file_bytes, upload_id, filename)

            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            is_demat_excel = instrument_type.startswith("demat_") and ext in ("xlsx", "xls")

            if is_demat_excel:
                # ═══ DIRECT PARSING — No AI needed ═══
                from app.api.endpoints.demat_parser import parse_demat_excel
                row.status = "structuring"
                await db.commit()

                structured, entries = parse_demat_excel(file_bytes, filename, instrument_type)
                row.structured_data = structured
                row.raw_text = f"[Direct parsed — {len(structured.get('transactions', structured.get('holdings', [])))} items]"
                row.status = "generating_entries"
                await db.commit()

                row.journal_entries = entries
                row.journal_entry_count = len(entries)
                row.status = "completed"
                await db.commit()

                logger.info(f"✅ FI Upload {upload_id} (direct): {len(entries)} journal entries")
            else:
                # ═══ AI PIPELINE — for PDFs and other formats ═══
                raw_text = await _extract_text(file_bytes, filename)
                row.status = "structuring"
                row.raw_text = raw_text[:2000]
                await db.commit()

                base_type = instrument_type.split('_')[0] if instrument_type.startswith('demat_') else instrument_type
                structured = await _structure_with_openai(raw_text, base_type)
                row.structured_data = structured
                row.status = "generating_entries"
                await db.commit()

                journal = await _generate_journal_entries(structured)
                entries = journal.get("journal_entries", [])
                row.journal_entries = entries
                row.journal_entry_count = len(entries)
                row.status = "completed"
                await db.commit()

                logger.info(f"✅ FI Upload {upload_id} (AI): {len(entries)} journal entries")

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

    valid_types = ("demat", "mutual_fund", "pms", "demat_holdings", "demat_taxpnl", "demat_tradebook")
    if instrument_type not in valid_types:
        raise HTTPException(400, f"instrument_type must be one of: {', '.join(valid_types)}")

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
            "pms_account_id": str(r.pms_account_id) if r.pms_account_id else None,
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


# ═══════════════════════════════════════════════════════
# 26AS / AIS — Upload, Extract, Auto-Match
# ═══════════════════════════════════════════════════════

TDS_26AS_PROMPT = """You are an expert Indian tax data extraction AI specialising in Form 26AS and AIS (Annual Information Statement).

Given the raw text from a 26AS or AIS document, extract ALL TDS entries organised by section.

**Rules:**
1. Extract EVERY entry from Part A (TDS) and Part A1 (TDS 15G/15H). Do NOT skip any.
2. Dates must be ISO format: "YYYY-MM-DD"
3. Amounts must be plain numbers (no commas, no ₹) or null.
4. Group entries by TDS section code (194, 194A, 194K, 194DA, 194B, etc.)
5. Also extract Part B (TCS), Part C (Tax Paid), and SFT entries if present.

Return this exact JSON:
{
    "pan": "string or null",
    "assessment_year": "string or null",
    "financial_year": "string or null",
    "tds_entries": [
        {
            "section": "194 or 194A or 194K or 194DA or 194B etc",
            "section_description": "Dividend / Interest / MF Income etc",
            "tan_of_deductor": "string or null",
            "deductor_name": "string",
            "transaction_date": "YYYY-MM-DD or null",
            "amount_paid_credited": number,
            "tds_deducted": number,
            "tds_deposited": number or null
        }
    ],
    "tcs_entries": [
        {
            "section": "string",
            "collector_name": "string",
            "amount": number,
            "tcs_collected": number
        }
    ],
    "tax_paid": [
        {
            "type": "Advance Tax or Self Assessment or TDS",
            "bsr_code": "string or null",
            "date": "YYYY-MM-DD",
            "amount": number,
            "challan_serial": "string or null"
        }
    ],
    "sft_entries": [
        {
            "transaction_type": "Purchase of shares / Sale of shares / MF Purchase / MF Redemption etc",
            "reported_by": "string",
            "amount": number,
            "count_of_transactions": number or null
        }
    ],
    "summary": {
        "total_tds": number,
        "total_tcs": number,
        "total_tax_paid": number,
        "total_income_reported": number
    }
}"""


async def _process_26as_upload(upload_id: str, file_bytes: bytes, filename: str, client_id: str):
    """Background: extract 26AS → structure → auto-match with existing statements."""
    async with AsyncSessionLocal() as db:
        try:
            row = (await db.execute(
                select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
            )).scalar_one()
            row.status = "extracting"
            await db.commit()

            _upload_to_supabase(file_bytes, upload_id, filename)

            raw_text = await _extract_text(file_bytes, filename)
            row.status = "structuring"
            row.raw_text = raw_text[:2000]
            await db.commit()

            # AI extraction
            client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
            estimated_tokens = len(raw_text) // 4

            if estimated_tokens > 20000:
                # Chunked processing for large 26AS
                structured = await _structure_chunked(raw_text, "26as", TDS_26AS_PROMPT)
            else:
                response = await client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": TDS_26AS_PROMPT},
                        {"role": "user", "content": raw_text},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                    max_tokens=16000,
                )
                structured = json.loads(response.choices[0].message.content)

            # Run auto-matching
            row.status = "generating_entries"
            await db.commit()

            match_results = await _match_26as_with_statements(db, structured, client_id)
            structured["match_results"] = match_results

            row.structured_data = structured
            row.journal_entries = []
            row.journal_entry_count = 0
            row.status = "completed"
            await db.commit()

            logger.info(f"✅ 26AS Upload {upload_id}: {len(structured.get('tds_entries', []))} TDS entries, "
                        f"{match_results.get('matched_count', 0)} matched, "
                        f"{match_results.get('unmatched_count', 0)} unmatched")

        except Exception as e:
            logger.error(f"❌ 26AS Upload {upload_id} failed: {e}")
            row = (await db.execute(
                select(FinancialInstrumentUpload).where(FinancialInstrumentUpload.id == upload_id)
            )).scalar_one_or_none()
            if row:
                row.status = "failed"
                row.error_message = str(e)[:2000]
                await db.commit()


async def _match_26as_with_statements(db: AsyncSession, data_26as: dict, client_id: str) -> dict:
    """Match 26AS TDS entries against uploaded Demat/MF/PMS statements."""

    tds_entries = data_26as.get("tds_entries", [])
    if not tds_entries:
        return {"matched": [], "unmatched_26as": [], "unmatched_stmts": [],
                "mismatched": [], "matched_count": 0, "unmatched_count": 0,
                "mismatch_count": 0, "section_summary": {}}

    # Fetch all completed FI uploads for this client (non-26AS)
    stmt_rows = (await db.execute(
        select(FinancialInstrumentUpload).where(
            FinancialInstrumentUpload.client_id == client_id,
            FinancialInstrumentUpload.status == "completed",
            FinancialInstrumentUpload.instrument_type != "26as",
        )
    )).scalars().all()

    # Collect all dividends & TDS from statements
    stmt_dividends = []  # {"source": "demat/mf/pms", "name": "...", "amount": X, "tds": Y}
    stmt_cg = {"stcg": 0, "ltcg": 0}

    for sr in stmt_rows:
        sd = sr.structured_data or {}
        utype = sr.instrument_type

        # Dividends
        for div in sd.get("dividends", []):
            amt = float(div.get("amount", 0) or 0)
            tds = float(div.get("tds_deducted", div.get("tds", 0)) or 0)
            name = div.get("scrip_name", div.get("fund_name", div.get("security_name", "Unknown")))
            if amt > 0:
                stmt_dividends.append({
                    "source": utype, "name": name, "amount": amt,
                    "tds": tds, "date": div.get("date", div.get("ex_date", ""))
                })

        # Capital gains summary
        cg = sd.get("capital_gains_summary", {})
        stmt_cg["stcg"] += float(cg.get("short_term_gain", 0) or 0)
        stmt_cg["ltcg"] += float(cg.get("long_term_gain", 0) or 0)

    # Group 26AS entries by section
    section_groups = {}
    for entry in tds_entries:
        sec = entry.get("section", "unknown")
        section_groups.setdefault(sec, []).append(entry)

    matched = []
    unmatched_26as = []
    mismatched = []

    # Process each section
    remaining_stmt_divs = list(stmt_dividends)

    for entry in tds_entries:
        sec = entry.get("section", "")
        deductor = entry.get("deductor_name", "")
        amt_26as = float(entry.get("amount_paid_credited", 0) or 0)
        tds_26as = float(entry.get("tds_deducted", 0) or 0)

        # Try to match dividend entries (Sec 194, 194K, 194DA)
        if sec in ("194", "194K", "194DA"):
            best_match = None
            best_idx = -1
            best_diff = float("inf")

            for i, sd in enumerate(remaining_stmt_divs):
                # Match by amount (within 10% tolerance) and fuzzy name
                if sd["amount"] > 0:
                    diff = abs(sd["amount"] - amt_26as)
                    pct_diff = diff / max(amt_26as, 1) * 100

                    if pct_diff < 15 and diff < best_diff:
                        best_match = sd
                        best_idx = i
                        best_diff = diff

            if best_match is not None:
                tds_diff = abs(best_match["tds"] - tds_26as)
                status = "matched" if tds_diff < 10 else "tds_mismatch"

                result_entry = {
                    "section": sec,
                    "deductor": deductor,
                    "amount_26as": amt_26as,
                    "tds_26as": tds_26as,
                    "amount_stmt": best_match["amount"],
                    "tds_stmt": best_match["tds"],
                    "stmt_source": best_match["source"],
                    "stmt_name": best_match["name"],
                    "amount_diff": round(amt_26as - best_match["amount"], 2),
                    "tds_diff": round(tds_26as - best_match["tds"], 2),
                }

                if status == "matched":
                    matched.append(result_entry)
                else:
                    result_entry["issue"] = f"TDS mismatch: 26AS ₹{tds_26as:.0f} vs Stmt ₹{best_match['tds']:.0f}"
                    mismatched.append(result_entry)

                remaining_stmt_divs.pop(best_idx)
            else:
                unmatched_26as.append({
                    "section": sec,
                    "deductor": deductor,
                    "amount_26as": amt_26as,
                    "tds_26as": tds_26as,
                    "issue": "No matching dividend found in uploaded statements"
                })
        else:
            # Other sections (194A interest, etc.) — report as informational
            unmatched_26as.append({
                "section": sec,
                "deductor": deductor,
                "amount_26as": amt_26as,
                "tds_26as": tds_26as,
                "issue": f"Section {sec} — not auto-matched (manual verification needed)"
            })

    # Remaining statement dividends that weren't matched
    unmatched_stmts = [{
        "source": sd["source"], "name": sd["name"],
        "amount": sd["amount"], "tds": sd["tds"],
        "issue": "Dividend in statement but not found in 26AS"
    } for sd in remaining_stmt_divs]

    # Build section summary
    section_summary = {}
    for sec, entries in section_groups.items():
        total_amount = sum(float(e.get("amount_paid_credited", 0) or 0) for e in entries)
        total_tds = sum(float(e.get("tds_deducted", 0) or 0) for e in entries)
        section_summary[sec] = {
            "count": len(entries),
            "total_amount": round(total_amount, 2),
            "total_tds": round(total_tds, 2),
        }

    return {
        "matched": matched,
        "unmatched_26as": unmatched_26as,
        "unmatched_stmts": unmatched_stmts,
        "mismatched": mismatched,
        "matched_count": len(matched),
        "unmatched_count": len(unmatched_26as) + len(unmatched_stmts),
        "mismatch_count": len(mismatched),
        "section_summary": section_summary,
    }


# ─── 26AS Upload Endpoint ────────────────────────────
@router.post("/26as-upload")
async def upload_26as(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_id: str = Form(...),
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Upload a 26AS or AIS PDF for TDS reconciliation."""
    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("pdf", "xlsx", "xls", "csv"):
        raise HTTPException(400, f"Supported: PDF, Excel, CSV. Got .{ext}")

    file_bytes = await file.read()
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large ({len(file_bytes)/(1024*1024):.1f} MB). Max 25 MB.")
    if len(file_bytes) == 0:
        raise HTTPException(400, "File is empty")

    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # Check duplicate
    existing = (await db.execute(
        select(FinancialInstrumentUpload).where(
            FinancialInstrumentUpload.client_id == client_id,
            FinancialInstrumentUpload.file_hash == file_hash,
        )
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(409, f"Duplicate — already uploaded as \"{existing.filename}\"")

    upload_id = str(uuid.uuid4())
    row = FinancialInstrumentUpload(
        id=upload_id,
        client_id=client_id,
        user_id=str(current_user.id),
        instrument_type="26as",
        filename=file.filename,
        file_hash=file_hash,
        status="processing",
    )
    db.add(row)
    await db.commit()

    background_tasks.add_task(_process_26as_upload, upload_id, file_bytes, file.filename, client_id)

    return {
        "id": upload_id,
        "status": "processing",
        "message": f"Processing 26AS: {file.filename}. Extraction + auto-matching may take 30-60 seconds.",
    }


# ─── 26AS Match Results ──────────────────────────────
@router.get("/26as-match")
async def get_26as_match(
    client_id: str,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(deps.get_db),
) -> Any:
    """Get auto-match results from the latest 26AS upload for a client."""
    row = (await db.execute(
        select(FinancialInstrumentUpload).where(
            FinancialInstrumentUpload.client_id == client_id,
            FinancialInstrumentUpload.instrument_type == "26as",
        ).order_by(FinancialInstrumentUpload.created_at.desc())
    )).scalars().first()

    if not row:
        return {"status": "not_uploaded", "message": "No 26AS uploaded for this client"}
    if row.status != "completed":
        return {"status": row.status, "message": f"26AS processing: {row.status}"}

    sd = row.structured_data or {}
    return {
        "status": "completed",
        "upload_id": str(row.id),
        "filename": row.filename,
        "uploaded_at": row.created_at.isoformat() if row.created_at else None,
        "pan": sd.get("pan"),
        "assessment_year": sd.get("assessment_year"),
        "financial_year": sd.get("financial_year"),
        "tds_entries_count": len(sd.get("tds_entries", [])),
        "summary": sd.get("summary", {}),
        "match_results": sd.get("match_results", {}),
        "section_summary": sd.get("match_results", {}).get("section_summary", {}),
    }


