from typing import Any, List, Optional
from uuid import UUID as PyUUID
import asyncio
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.api import deps
from app.models.models import GetInvoice, InvoiceLineItem, User
from app.schemas import invoice as invoice_schemas

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══ UPLOAD & AI PROCESSING ═══

async def _process_invoice_background(
    file_bytes: bytes,
    filename: str,
    client_id: str,
    voucher_type: str,
    content_type: str,
):
    """Background task: parse invoice and insert into DB."""
    from app.db.session import AsyncSessionLocal
    from app.services.invoice_parser import extract_invoice_data
    from app.services.storage import storage_service
    from datetime import datetime

    try:
        # 1. Extract structured data
        data = await extract_invoice_data(file_bytes, filename, voucher_type)

        # 2. Store file in Supabase (invoices bucket)
        safe_name = filename.replace(" ", "_").replace("/", "_")
        object_path = f"{client_id}/{safe_name}"
        stored_path = None
        try:
            from supabase import create_client
            from app.core.config import settings
            supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
            # Ensure bucket exists
            try:
                supabase.storage.create_bucket("invoices", options={"public": False})
            except Exception:
                pass
            supabase.storage.from_("invoices").upload(
                path=object_path,
                file=file_bytes,
                file_options={"content-type": content_type or "application/pdf", "upsert": "true"}
            )
            stored_path = f"invoices/{object_path}"
            logger.info(f"📁 File uploaded to Supabase: {stored_path}")
        except Exception as e:
            logger.warning(f"⚠️ Supabase upload failed, using local: {e}")
            from app.services.storage import storage_service
            local_path = f"invoices/{client_id}/{safe_name}"
            storage_service.upload_file(file_content=file_bytes, path=local_path, content_type=content_type or "application/pdf")
            stored_path = local_path

        # 3. Insert invoice into DB
        async with AsyncSessionLocal() as db:
            invoice = GetInvoice(
                vendor_name=data.get("vendor_name", "Unknown"),
                gst_number=data.get("gst_number"),
                invoice_number=data.get("invoice_number", "N/A"),
                invoice_date=_parse_date(data.get("invoice_date")),
                currency=data.get("currency", "INR"),
                amount=str(data.get("amount", "0") or "0"),
                gst_amount=str(data.get("gst_amount", "0") or "0"),
                total_amount=str(data.get("total_amount", "0") or "0"),
                expenses_type=voucher_type if voucher_type else data.get("expenses_type", "Uncategorized"),
                source="web",
                file_path=stored_path or file_path,
                synced_to_tally=False,
                status="pending",
                client_id=PyUUID(client_id) if client_id else None,
            )
            db.add(invoice)
            await db.flush()  # Get the ID

            # 4. Insert line items
            for item in data.get("line_items", []):
                line = InvoiceLineItem(
                    invoice_id=invoice.id,
                    description=item.get("description", "Item"),
                    service_code=item.get("hsn_sac", ""),
                    quantity=str(item.get("quantity", "1")),
                    price=str(item.get("unit_price", "0") or "0"),
                    amount=str(item.get("amount", "0") or "0"),
                )
                db.add(line)

            await db.commit()
            logger.info(f"✅ Invoice saved: ID={invoice.id}, "
                        f"Vendor={invoice.vendor_name}, "
                        f"Total={invoice.total_amount}, "
                        f"Items={len(data.get('line_items', []))}")

    except Exception as e:
        logger.error(f"❌ Invoice processing failed for {filename}: {e}")
        import traceback
        traceback.print_exc()


def _parse_date(val):
    """Parse date string safely."""
    if not val:
        return None
    try:
        from datetime import datetime
        return datetime.strptime(str(val), "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


@router.post("/upload")
async def upload_invoice(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_id: str = Form(""),
    clientId: str = Form(""),  # frontend sends both
    voucher_type: str = Form(""),
    vendor_name_preference: str = Form(""),
    current_user: User = Depends(deps.get_current_active_user),
):
    """
    Upload an invoice file for AI processing.
    Replaces the n8n webhook pipeline.
    """
    # Accept either client_id or clientId
    cid = client_id or clientId
    if not cid:
        raise HTTPException(status_code=400, detail="client_id is required")

    file_bytes = await file.read()
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    if len(file_bytes) > 20 * 1024 * 1024:  # 20MB limit
        raise HTTPException(status_code=413, detail="File too large (max 20MB)")

    logger.info(f"📤 Invoice upload: {file.filename} ({len(file_bytes)} bytes) "
                f"| client={cid} | type={voucher_type}")

    # Process in background so upload returns immediately
    background_tasks.add_task(
        _process_invoice_background,
        file_bytes=file_bytes,
        filename=file.filename,
        client_id=cid,
        voucher_type=voucher_type,
        content_type=file.content_type,
    )

    return {
        "status": "processing",
        "message": f"Invoice {file.filename} is being processed by AI. It will appear in your transactions within 30-60 seconds.",
        "filename": file.filename,
    }

@router.get("/", response_model=List[invoice_schemas.Invoice])
async def read_invoices(
    db: AsyncSession = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    client_email: Optional[str] = None,
    client_id: Optional[str] = None,
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Retrieve invoices, optionally filtered by client_id.
    """
    query = select(GetInvoice).offset(skip).limit(limit)
    if client_id:
        from uuid import UUID as PyUUID
        try:
            query = query.where(GetInvoice.client_id == PyUUID(client_id))
        except ValueError:
            pass
    if client_email:
        query = query.where(GetInvoice.client_email_id == client_email)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/stats")
async def invoice_stats(
    db: AsyncSession = Depends(deps.get_db),
    client_id: Optional[str] = None,
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Dashboard stats for invoices, optionally filtered by client_id.
    """
    from sqlalchemy import func
    from uuid import UUID as PyUUID

    base_filter = []
    if client_id:
        try:
            base_filter.append(GetInvoice.client_id == PyUUID(client_id))
        except ValueError:
            pass

    def apply_filters(q):
        for f in base_filter:
            q = q.where(f)
        return q

    total = (await db.execute(apply_filters(
        select(func.count(GetInvoice.id))
    ))).scalar() or 0

    pending = (await db.execute(apply_filters(
        select(func.count(GetInvoice.id)).where(GetInvoice.synced_to_tally == False)
    ))).scalar() or 0

    synced = (await db.execute(apply_filters(
        select(func.count(GetInvoice.id)).where(GetInvoice.synced_to_tally == True)
    ))).scalar() or 0

    with_file = (await db.execute(apply_filters(
        select(func.count(GetInvoice.id)).where(
            GetInvoice.file_path != None, GetInvoice.file_path != ""
        )
    ))).scalar() or 0

    vendor_count = (await db.execute(apply_filters(
        select(func.count(func.distinct(GetInvoice.vendor_name)))
    ))).scalar() or 0

    # Top 5 vendors by count
    q_top = apply_filters(
        select(
            GetInvoice.vendor_name,
            func.count(GetInvoice.id).label("cnt")
        ).group_by(GetInvoice.vendor_name)
        .order_by(func.count(GetInvoice.id).desc())
        .limit(5)
    )
    top_result = await db.execute(q_top)
    top_vendors = [
        {"name": row[0] or "Unknown", "count": row[1]}
        for row in top_result.fetchall()
    ]

    # Get all invoices to compute total amount in Python (amounts are strings)
    all_q = apply_filters(select(GetInvoice.total_amount))
    all_result = await db.execute(all_q)
    total_amount = 0.0
    for row in all_result.fetchall():
        try:
            total_amount += float(str(row[0] or "0").replace(",", ""))
        except (ValueError, TypeError):
            pass

    return {
        "total": total,
        "pending": pending,
        "synced": synced,
        "with_file": with_file,
        "vendor_count": vendor_count,
        "total_amount": total_amount,
        "top_vendors": top_vendors,
    }

@router.post("/", response_model=invoice_schemas.Invoice)
async def create_invoice(
    *,
    db: AsyncSession = Depends(deps.get_db),
    invoice_in: invoice_schemas.InvoiceCreate,
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Create new invoice.
    """
    invoice = GetInvoice(**invoice_in.model_dump())
    db.add(invoice)
    await db.commit()
    await db.refresh(invoice)
    return invoice

@router.get("/{id}", response_model=invoice_schemas.Invoice)
async def read_invoice(
    id: int,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Get invoice by ID.
    """
    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return invoice


@router.get("/{id}/items")
async def read_invoice_items(
    id: int,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Get line items for a given invoice (from get_invoice_items table).
    These are populated by n8n after OCR processing.
    """
    query = select(InvoiceLineItem).where(InvoiceLineItem.invoice_id == id)
    result = await db.execute(query)
    items = result.scalars().all()
    return [
        {
            "id": str(item.id),
            "invoice_id": item.invoice_id,
            "description": item.description,
            "service_code": item.service_code or "",
            "quantity": item.quantity or "1",
            "price": item.price or "0",
            "amount": item.amount or "0",
        }
        for item in items
    ]


@router.put("/{id}/items")
async def replace_invoice_items(
    id: int,
    items: List[dict],
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Replace all line items for a given invoice.
    Deletes existing items and creates new ones from the request body.
    """
    from sqlalchemy import delete

    # Verify invoice exists
    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")

    # Delete existing items
    await db.execute(
        delete(InvoiceLineItem).where(InvoiceLineItem.invoice_id == id)
    )

    # Insert new items
    new_items = []
    for item_data in items:
        new_item = InvoiceLineItem(
            invoice_id=id,
            description=item_data.get("description", "Item"),
            service_code=item_data.get("service_code", ""),
            quantity=str(item_data.get("quantity", "1")),
            price=str(item_data.get("price", "0")),
            amount=str(item_data.get("amount", "0")),
        )
        db.add(new_item)
        new_items.append(new_item)

    await db.commit()

    return [
        {
            "id": str(item.id),
            "invoice_id": item.invoice_id,
            "description": item.description,
            "service_code": item.service_code or "",
            "quantity": item.quantity or "1",
            "price": item.price or "0",
            "amount": item.amount or "0",
        }
        for item in new_items
    ]


@router.patch("/{id}", response_model=invoice_schemas.Invoice)
async def update_invoice(
    id: int,
    invoice_in: invoice_schemas.InvoiceUpdate,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Update an invoice.
    """
    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")

    update_data = invoice_in.model_dump(exclude_unset=True)

    # Strip timezone from invoice_date — DB column is TIMESTAMP WITHOUT TIME ZONE
    if 'invoice_date' in update_data and update_data['invoice_date'] is not None:
        dt = update_data['invoice_date']
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            update_data['invoice_date'] = dt.replace(tzinfo=None)

    for field, value in update_data.items():
        setattr(invoice, field, value)

    try:
        await db.commit()
        await db.refresh(invoice)
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return invoice


@router.get("/{id}/file-url")
async def get_invoice_file_url(
    id: int,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Get a signed storage URL for the invoice's source document.
    The invoices bucket is private, so we generate a time-limited signed URL.
    """
    from app.core.config import settings
    from urllib.parse import quote
    import requests as http_requests

    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")

    if not invoice.file_path:
        raise HTTPException(status_code=404, detail="No file associated with this invoice")

    supabase_url = settings.SUPABASE_URL
    supabase_key = settings.SUPABASE_KEY
    if not supabase_url or not supabase_key:
        raise HTTPException(status_code=500, detail="Storage not configured")

    # file_path format: "invoices/filename.pdf" where "invoices" is the bucket name
    parts = invoice.file_path.split("/", 1)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid file path format")

    bucket_name = parts[0]  # "invoices"
    object_path = parts[1]  # "filename.pdf"

    # Generate signed URL (valid for 1 hour = 3600 seconds)
    sign_url = f"{supabase_url}/storage/v1/object/sign/{bucket_name}/{quote(object_path, safe='/')}"
    resp = http_requests.post(
        sign_url,
        headers={
            "Authorization": f"Bearer {supabase_key}",
            "apikey": supabase_key,
            "Content-Type": "application/json",
        },
        json={"expiresIn": 3600},
    )

    if resp.status_code == 200:
        data = resp.json()
        signed_path = data.get("signedURL", "")
        file_url = f"{supabase_url}/storage/v1{signed_path}"
    else:
        # Fallback to direct public URL attempt
        file_url = f"{supabase_url}/storage/v1/object/public/{quote(invoice.file_path, safe='/')}"

    return {
        "id": invoice.id,
        "file_path": invoice.file_path,
        "file_url": file_url,
        "file_type": invoice.file_path.rsplit('.', 1)[-1].lower() if '.' in invoice.file_path else "unknown",
    }


@router.get("/{id}/file")
async def get_invoice_file(
    id: int,
    db: AsyncSession = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Serve the invoice file. Supports both local storage and Supabase.
    """
    from app.core.config import settings
    from app.services.storage import storage_service
    from fastapi.responses import FileResponse
    import os

    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")

    if not invoice.file_path:
        raise HTTPException(status_code=404, detail="No file associated")

    # Determine content type from extension
    ext = invoice.file_path.rsplit('.', 1)[-1].lower() if '.' in invoice.file_path else ''
    content_type_map = {
        'pdf': 'application/pdf',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif',
        'webp': 'image/webp',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'csv': 'text/csv',
    }
    ct = content_type_map.get(ext, 'application/octet-stream')

    # Try local storage first
    if hasattr(storage_service, 'provider') and storage_service.provider == 'local':
        local_path = storage_service.download_to_temp(invoice.file_path)
        if local_path and os.path.exists(local_path):
            return FileResponse(
                local_path,
                media_type=ct,
                headers={"Content-Disposition": f'inline; filename="{os.path.basename(invoice.file_path)}"'},
            )

    # Fallback: try Supabase storage proxy
    from urllib.parse import quote
    import requests as http_requests

    supabase_url = settings.SUPABASE_URL
    supabase_key = settings.SUPABASE_KEY

    if not supabase_url or not supabase_key:
        raise HTTPException(status_code=404, detail="File not found (storage not configured)")

    parts = invoice.file_path.split("/", 1)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid file path")

    bucket_name = parts[0]
    object_path = parts[1]

    # Generate signed URL
    sign_url = f"{supabase_url}/storage/v1/object/sign/{bucket_name}/{quote(object_path, safe='/')}"
    sign_resp = http_requests.post(
        sign_url,
        headers={
            "Authorization": f"Bearer {supabase_key}",
            "apikey": supabase_key,
            "Content-Type": "application/json",
        },
        json={"expiresIn": 3600},
    )

    if sign_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Could not sign file URL")

    signed_path = sign_resp.json().get("signedURL", "")
    file_url = f"{supabase_url}/storage/v1{signed_path}"

    # Fetch the actual file
    file_resp = http_requests.get(file_url)
    if file_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Could not fetch file")

    return Response(
        content=file_resp.content,
        media_type=ct,
        headers={"Content-Disposition": f'inline; filename="{object_path}"'},
    )

