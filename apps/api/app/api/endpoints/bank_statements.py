"""
Bank Statements API
───────────────────
Endpoints for uploading, listing, and viewing bank statement data.
"""

import logging
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api import deps
from app.models.models import BankStatement, BankTransaction, User
from app.services.banking.statement_parser import (
    extract_text_llamaparse,
    structure_with_openai,
    safe_date,
    safe_decimal,
)

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB


# ─── Upload PDF to Supabase ────────────────────────────
def _upload_pdf_to_supabase(file_bytes: bytes, statement_id: str, filename: str) -> str:
    """Upload the PDF to Supabase storage. Returns the storage path."""
    try:
        from supabase import create_client
        from app.core.config import settings

        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        bucket = "bank-statements"

        # Ensure bucket exists
        try:
            client.storage.create_bucket(bucket, options={"public": False})
        except Exception:
            pass  # bucket already exists

        storage_path = f"{statement_id}/{filename}"
        client.storage.from_(bucket).upload(
            path=storage_path,
            file=file_bytes,
            file_options={"content-type": "application/pdf", "upsert": "true"}
        )
        logger.info(f"PDF uploaded to Supabase: {storage_path}")
        return storage_path
    except Exception as e:
        logger.warning(f"PDF upload to Supabase failed (non-fatal): {e}")
        return ""


# ─── Background processor ──────────────────────────────
async def _process_statement(statement_id: str, file_bytes: bytes, filename: str):
    """Background task: Upload PDF → LlamaParse → OpenAI → save to DB."""
    from app.db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        stmt = await db.get(BankStatement, uuid.UUID(statement_id))
        if not stmt:
            logger.error(f"Statement {statement_id} not found")
            return

        try:
            # Step 0 — Upload PDF to Supabase for later viewing
            storage_path = _upload_pdf_to_supabase(file_bytes, statement_id, filename)
            if storage_path:
                stmt.file_path = storage_path
                await db.commit()

            # Step 1 — Extract text with LlamaParse
            stmt.status = "extracting"
            await db.commit()

            raw_text = await extract_text_llamaparse(file_bytes, filename)
            stmt.raw_text = raw_text

            # Step 2 — Structure with OpenAI
            stmt.status = "structuring"
            await db.commit()

            structured = await structure_with_openai(raw_text)

            # Step 3 — Save structured data
            stmt.bank_name = structured.get("bank_name")
            stmt.account_number = structured.get("account_number")
            stmt.statement_period_start = safe_date(structured.get("period_start"))
            stmt.statement_period_end = safe_date(structured.get("period_end"))
            stmt.opening_balance = safe_decimal(structured.get("opening_balance"))
            stmt.closing_balance = safe_decimal(structured.get("closing_balance"))

            transactions = structured.get("transactions", [])
            total_credits = 0
            total_debits = 0

            for txn in transactions:
                debit = safe_decimal(txn.get("debit"))
                credit = safe_decimal(txn.get("credit"))

                if debit:
                    total_debits += float(debit)
                if credit:
                    total_credits += float(credit)

                db.add(BankTransaction(
                    statement_id=stmt.id,
                    transaction_date=safe_date(txn.get("date")),
                    value_date=safe_date(txn.get("value_date")),
                    description=txn.get("description"),
                    reference_no=txn.get("reference_no"),
                    debit=debit,
                    credit=credit,
                    balance=safe_decimal(txn.get("balance")),
                    category=txn.get("category"),
                    party_name=txn.get("party_name"),
                ))

            stmt.total_credits = safe_decimal(total_credits)
            stmt.total_debits = safe_decimal(total_debits)
            stmt.transaction_count = len(transactions)
            stmt.status = "completed"
            stmt.error_message = None

            await db.commit()
            logger.info(f"✅ Statement {statement_id}: {len(transactions)} transactions saved")

        except Exception as e:
            logger.error(f"❌ Statement {statement_id} processing failed: {e}")
            stmt.status = "failed"
            stmt.error_message = str(e)[:2000]
            await db.commit()


# ─── Upload Endpoint ───────────────────────────────────
@router.post("/upload")
async def upload_bank_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_id: str = Form(...),
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Upload a bank statement PDF for processing."""

    # Validate file type
    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("pdf", "PDF"):
        raise HTTPException(400, f"Only PDF files are supported, got .{ext}")

    # Read file
    file_bytes = await file.read()
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large ({len(file_bytes)/(1024*1024):.1f} MB). Max is 20 MB.")
    if len(file_bytes) == 0:
        raise HTTPException(400, "File is empty")

    # Create statement record
    stmt = BankStatement(
        client_id=uuid.UUID(client_id),
        user_id=current_user.id,
        original_filename=file.filename,
        status="processing",
        created_at=datetime.utcnow(),
    )
    db.add(stmt)
    await db.commit()
    await db.refresh(stmt)

    # Process in background
    background_tasks.add_task(_process_statement, str(stmt.id), file_bytes, file.filename)

    return {
        "id": str(stmt.id),
        "status": "processing",
        "message": f"Processing {file.filename}. This will take 15-30 seconds."
    }


# ─── List Statements ──────────────────────────────────
@router.get("/")
async def list_bank_statements(
    client_id: str = None,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """List all bank statements, optionally filtered by client."""

    query = select(BankStatement).order_by(BankStatement.created_at.desc())
    if client_id:
        query = query.where(BankStatement.client_id == uuid.UUID(client_id))

    result = await db.execute(query)
    statements = result.scalars().all()

    return [
        {
            "id": str(s.id),
            "client_id": str(s.client_id),
            "bank_name": s.bank_name,
            "account_number": s.account_number,
            "period_start": str(s.statement_period_start) if s.statement_period_start else None,
            "period_end": str(s.statement_period_end) if s.statement_period_end else None,
            "opening_balance": float(s.opening_balance) if s.opening_balance else None,
            "closing_balance": float(s.closing_balance) if s.closing_balance else None,
            "total_credits": float(s.total_credits) if s.total_credits else None,
            "total_debits": float(s.total_debits) if s.total_debits else None,
            "transaction_count": s.transaction_count,
            "original_filename": s.original_filename,
            "status": s.status,
            "error_message": s.error_message,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in statements
    ]


# ─── Get Statement Detail ─────────────────────────────
@router.get("/{statement_id}")
async def get_bank_statement(
    statement_id: str,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Get a single statement with its status."""
    stmt = await db.get(BankStatement, uuid.UUID(statement_id))
    if not stmt:
        raise HTTPException(404, "Statement not found")

    return {
        "id": str(stmt.id),
        "bank_name": stmt.bank_name,
        "account_number": stmt.account_number,
        "period_start": str(stmt.statement_period_start) if stmt.statement_period_start else None,
        "period_end": str(stmt.statement_period_end) if stmt.statement_period_end else None,
        "opening_balance": float(stmt.opening_balance) if stmt.opening_balance else None,
        "closing_balance": float(stmt.closing_balance) if stmt.closing_balance else None,
        "total_credits": float(stmt.total_credits) if stmt.total_credits else None,
        "total_debits": float(stmt.total_debits) if stmt.total_debits else None,
        "transaction_count": stmt.transaction_count,
        "original_filename": stmt.original_filename,
        "file_path": stmt.file_path,
        "status": stmt.status,
        "error_message": stmt.error_message,
        "created_at": stmt.created_at.isoformat() if stmt.created_at else None,
    }


# ─── Get PDF Signed URL ───────────────────────────────
@router.get("/{statement_id}/pdf")
async def get_bank_statement_pdf(
    statement_id: str,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Get a signed URL to view the bank statement PDF."""
    stmt = await db.get(BankStatement, uuid.UUID(statement_id))
    if not stmt:
        raise HTTPException(404, "Statement not found")

    if not stmt.file_path:
        raise HTTPException(404, "PDF not available for this statement")

    try:
        from supabase import create_client
        from app.core.config import settings

        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        bucket = "bank-statements"

        # Generate a signed URL valid for 1 hour
        signed = client.storage.from_(bucket).create_signed_url(
            stmt.file_path, 3600
        )
        url = signed.get("signedURL") or signed.get("signedUrl") or signed.get("signed_url")

        if not url:
            raise HTTPException(500, "Could not generate signed URL")

        return {"url": url}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF URL generation failed: {e}")
        raise HTTPException(500, f"Failed to get PDF URL: {str(e)}")


# ─── Get Transactions ─────────────────────────────────
@router.get("/{statement_id}/transactions")
async def get_bank_transactions(
    statement_id: str,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Get all transactions for a specific bank statement."""
    stmt = await db.get(BankStatement, uuid.UUID(statement_id))
    if not stmt:
        raise HTTPException(404, "Statement not found")

    query = (
        select(BankTransaction)
        .where(BankTransaction.statement_id == uuid.UUID(statement_id))
        .order_by(BankTransaction.transaction_date.asc(), BankTransaction.created_at.asc())
    )
    result = await db.execute(query)
    txns = result.scalars().all()

    return [
        {
            "id": str(t.id),
            "date": str(t.transaction_date) if t.transaction_date else None,
            "value_date": str(t.value_date) if t.value_date else None,
            "description": t.description,
            "reference_no": t.reference_no,
            "debit": float(t.debit) if t.debit else None,
            "credit": float(t.credit) if t.credit else None,
            "balance": float(t.balance) if t.balance else None,
            "category": t.category,
            "party_name": t.party_name,
        }
        for t in txns
    ]


# ─── Delete Statement ─────────────────────────────────
@router.delete("/{statement_id}")
async def delete_bank_statement(
    statement_id: str,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Delete a bank statement and all its transactions."""
    stmt = await db.get(BankStatement, uuid.UUID(statement_id))
    if not stmt:
        raise HTTPException(404, "Statement not found")

    await db.delete(stmt)
    await db.commit()
    return {"message": "Statement deleted"}


# ─── Update Transaction ───────────────────────────────
@router.patch("/transactions/{transaction_id}")
async def update_bank_transaction(
    transaction_id: str,
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Update a single bank transaction."""
    txn = await db.get(BankTransaction, uuid.UUID(transaction_id))
    if not txn:
        raise HTTPException(404, "Transaction not found")

    if "description" in body:
        txn.description = body["description"]
    if "reference_no" in body:
        txn.reference_no = body["reference_no"]
    if "debit" in body:
        txn.debit = safe_decimal(body["debit"]) if body["debit"] else None
    if "credit" in body:
        txn.credit = safe_decimal(body["credit"]) if body["credit"] else None
    if "balance" in body:
        txn.balance = safe_decimal(body["balance"]) if body["balance"] else None
    if "category" in body:
        txn.category = body["category"]
    if "party_name" in body:
        txn.party_name = body["party_name"]
    if "date" in body:
        txn.transaction_date = safe_date(body["date"])

    await db.commit()
    return {"message": "Updated"}


# ─── Bulk Save Transactions ───────────────────────────
@router.put("/{statement_id}/transactions")
async def bulk_save_bank_transactions(
    statement_id: str,
    body: dict,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """Bulk update all transactions for a statement."""
    stmt = await db.get(BankStatement, uuid.UUID(statement_id))
    if not stmt:
        raise HTTPException(404, "Statement not found")

    updates = body.get("transactions", [])
    updated = 0

    for item in updates:
        txn_id = item.get("id")
        if not txn_id:
            continue
        txn = await db.get(BankTransaction, uuid.UUID(txn_id))
        if not txn or str(txn.statement_id) != statement_id:
            continue

        if "description" in item:
            txn.description = item["description"]
        if "reference_no" in item:
            txn.reference_no = item["reference_no"]
        if "debit" in item:
            txn.debit = safe_decimal(item["debit"]) if item["debit"] else None
        if "credit" in item:
            txn.credit = safe_decimal(item["credit"]) if item["credit"] else None
        if "balance" in item:
            txn.balance = safe_decimal(item["balance"]) if item["balance"] else None
        if "category" in item:
            txn.category = item["category"]
        if "party_name" in item:
            txn.party_name = item["party_name"]
        if "date" in item:
            txn.transaction_date = safe_date(item["date"])
        updated += 1

    await db.commit()
    return {"message": f"Updated {updated} transactions"}
