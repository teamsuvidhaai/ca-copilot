from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.api import deps
from app.models.models import GetInvoice, InvoiceLineItem, User
from app.schemas import invoice as invoice_schemas

router = APIRouter()

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
    Update an invoice (specifically expenses_type).
    """
    query = select(GetInvoice).where(GetInvoice.id == id)
    result = await db.execute(query)
    invoice = result.scalars().first()
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")

    update_data = invoice_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(invoice, field, value)

    await db.commit()
    await db.refresh(invoice)
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
    Proxy the actual file content from Supabase storage.
    This avoids CORS issues by serving the file through our backend.
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
        raise HTTPException(status_code=404, detail="No file associated")

    supabase_url = settings.SUPABASE_URL
    supabase_key = settings.SUPABASE_KEY

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

    # Determine content type from extension
    ext = object_path.rsplit('.', 1)[-1].lower() if '.' in object_path else ''
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

    return Response(
        content=file_resp.content,
        media_type=ct,
        headers={"Content-Disposition": f"inline; filename=\"{object_path}\""},
    )
