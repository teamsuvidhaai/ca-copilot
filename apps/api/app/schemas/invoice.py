from typing import Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

class InvoiceBase(BaseModel):
    vendor_name: Optional[str] = None
    gst_number: Optional[str] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[datetime] = None
    currency: Optional[str] = "INR"
    amount: Optional[str] = None
    gst_amount: Optional[str] = None
    total_amount: Optional[str] = None
    expenses_type: Optional[str] = None
    source: Optional[str] = None
    client_email_id: Optional[str] = None
    file_path: Optional[str] = None
    synced_to_tally: Optional[bool] = False
    status: Optional[str] = "pending"
    client_id: Optional[UUID] = None

class InvoiceCreate(InvoiceBase):
    pass

class InvoiceUpdate(BaseModel):
    vendor_name: Optional[str] = None
    gst_number: Optional[str] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[datetime] = None
    currency: Optional[str] = None
    amount: Optional[str] = None
    gst_amount: Optional[str] = None
    total_amount: Optional[str] = None
    expenses_type: Optional[str] = None
    synced_to_tally: Optional[bool] = None
    status: Optional[str] = None
    client_id: Optional[UUID] = None

class InvoiceInDBBase(InvoiceBase):
    id: int
    received_at: datetime

    class Config:
        from_attributes = True

class Invoice(InvoiceInDBBase):
    pass

