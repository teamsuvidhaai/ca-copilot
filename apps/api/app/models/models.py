import uuid
from datetime import datetime
from enum import Enum as PyEnum
from typing import List, Optional
import sqlalchemy as sa

from sqlalchemy import Column, DateTime, ForeignKey, String, Text, Integer, Boolean, Float, Table, Enum, CheckConstraint, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship, Mapped, mapped_column
from pgvector.sqlalchemy import Vector

from app.db.base import Base

# Enums
class UserRole(str, PyEnum):
    OWNER = "owner"
    ADMIN = "admin"
    STAFF = "staff"

class SignupMethod(str, PyEnum):
    EMAIL = "email"
    PHONE = "phone"
    GOOGLE = "google"

class MessageRole(str, PyEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"

class Scope(str, PyEnum):
    FIRM = "FIRM"
    KIT = "KIT"
    CLIENT = "CLIENT"

class DocumentStatus(str, PyEnum):
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"

# Association Tables
conversation_kits = Table(
    "conversation_kits",
    Base.metadata,
    Column("conversation_id", UUID(as_uuid=True), ForeignKey("conversations.id"), primary_key=True),
    Column("kit_id", UUID(as_uuid=True), ForeignKey("kits.id"), primary_key=True),
)

service_kits = Table(
    "service_kits",
    Base.metadata,
    Column("service_id", UUID(as_uuid=True), ForeignKey("services.id"), primary_key=True),
    Column("kit_id", UUID(as_uuid=True), ForeignKey("kits.id"), primary_key=True),
)

client_services = Table(
    "client_services",
    Base.metadata,
    Column("client_id", UUID(as_uuid=True), ForeignKey("clients.id"), primary_key=True),
    Column("service_id", UUID(as_uuid=True), ForeignKey("services.id"), primary_key=True),
)

class Firm(Base):
    __tablename__ = "firms"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    users = relationship("User", back_populates="firm")
    clients = relationship("Client", back_populates="firm")

class User(Base):
    __tablename__ = "users"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True, index=True, nullable=True)  # Nullable for phone-only signups
    hashed_password = Column(String, nullable=True)  # Nullable for Google OAuth signups
    full_name = Column(String)
    phone_number = Column(String, unique=True, nullable=True, index=True)  # Unique for phone-based login
    job_title = Column(String, nullable=True)
    subscription_plan = Column(String, default="free")
    role = Column(Enum(UserRole), default=UserRole.STAFF)
    signup_method = Column(String, default="email", nullable=False)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    trial_started_at = Column(DateTime, default=datetime.utcnow)  # 30-day free trial starts at signup

    firm = relationship("Firm", back_populates="users")

class Client(Base):
    __tablename__ = "clients"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    email = Column(String(255), nullable=False, server_default="admin@example.com") # Default for existing rows
    client_id = Column(String, unique=True, nullable=True)  # E.g. CL-001
    gstins = Column(JSONB, default=[])
    pan = Column(String, nullable=True)
    cin = Column(String, nullable=True)
    tan = Column(String, nullable=True)
    iec = Column(String, nullable=True)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    firm = relationship("Firm", back_populates="clients")
    conversations = relationship("Conversation", back_populates="client")
    services = relationship("Service", secondary=client_services, back_populates="clients")
    # documents relationship handled via query usually, but can be added

class Kit(Base):
    __tablename__ = "kits"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    # Kits are global in this design, or could be firm specific. 
    # Requirement: "Topic Kits (attachable): specialist knowledge packs... attachable per conversation"
    created_at = Column(DateTime, default=datetime.utcnow)
    services = relationship("Service", secondary=service_kits, back_populates="kits")

class Service(Base):
    __tablename__ = "services"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    kits = relationship("Kit", secondary=service_kits, back_populates="services")
    clients = relationship("Client", secondary=client_services, back_populates="services")

class Conversation(Base):
    __tablename__ = "conversations"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    client = relationship("Client", back_populates="conversations")
    messages = relationship("Message", back_populates="conversation")
    attached_kits = relationship("Kit", secondary=conversation_kits)

class Message(Base):
    __tablename__ = "messages"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("conversations.id"), nullable=False)
    role = Column(Enum(MessageRole), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    conversation = relationship("Conversation", back_populates="messages")
    retrieval_logs = relationship("RetrievalLog", back_populates="message")

class Document(Base):
    __tablename__ = "documents"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String, nullable=False)
    scope = Column(Enum(Scope), nullable=False)
    
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=True) # Global usage might leave this null or specialized
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True)
    kit_id = Column(UUID(as_uuid=True), ForeignKey("kits.id"), nullable=True)
    
    status = Column(Enum(DocumentStatus), default=DocumentStatus.UPLOADED)
    file_path = Column(String, nullable=True) 
    metadata_ = Column("metadata", JSONB, default={})
    
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        CheckConstraint(
            "(scope = 'FIRM' AND kit_id IS NULL AND client_id IS NULL) OR "
            "(scope = 'KIT' AND kit_id IS NOT NULL AND client_id IS NULL) OR "
            "(scope = 'CLIENT' AND client_id IS NOT NULL AND kit_id IS NULL)",
            name="check_scope_constraints"
        ),
    )
    
    embeddings = relationship("DocEmbedding", back_populates="document", cascade="all, delete-orphan")

class DocEmbedding(Base):
    __tablename__ = "doc_embeddings"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id"), nullable=False)
    chunk_text = Column(Text, nullable=False)
    chunk_index = Column(Integer)
    embedding = Column(Vector(1536))
    metadata_ = Column("metadata", JSONB, default={})

    document = relationship("Document", back_populates="embeddings")

class RetrievalLog(Base):
    __tablename__ = "retrieval_logs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    message_id = Column(UUID(as_uuid=True), ForeignKey("messages.id"), nullable=False)
    cited_chunks = Column(JSONB) # Store list of chunk IDs or content snapshots
    created_at = Column(DateTime, default=datetime.utcnow)

    message = relationship("Message", back_populates="retrieval_logs")

class GetInvoice(Base):
    __tablename__ = "get_invoice"
    id = Column(Integer, primary_key=True, autoincrement=True)
    vendor_name = Column(String(255))
    gst_number = Column(String(50))
    invoice_number = Column(String(100))
    invoice_date = Column(DateTime)
    currency = Column(String(10))
    amount = Column(String)
    gst_amount = Column(String)
    total_amount = Column(String)
    expenses_type = Column(String(100), nullable=True)
    source = Column(String(100))
    client_email_id = Column(String(255), nullable=True)
    file_path = Column(String(1000), nullable=True) # Storage path or URL to the original document
    received_at = Column(DateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    synced_to_tally = Column(Boolean, default=False)
    status = Column(String(50), default="pending")  # pending, approved
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True)

    line_items = relationship("InvoiceLineItem", back_populates="invoice", cascade="all, delete-orphan")


class InvoiceLineItem(Base):
    """Line items extracted by OCR for a given invoice.
    n8n writes directly to this table after processing."""
    __tablename__ = "get_invoice_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    invoice_id = Column(Integer, ForeignKey("get_invoice.id", ondelete="CASCADE"), nullable=False, index=True)
    description = Column(String(500), nullable=False)
    service_code = Column(String(100), nullable=True)   # HSN / SAC code
    quantity = Column(String(50), default="1")
    price = Column(String(50), nullable=True)           # unit price
    amount = Column(String(50), nullable=True)          # line total

    invoice = relationship("GetInvoice", back_populates="line_items")


class AccountingVoucher(Base):
    __tablename__ = "accounting_vouchers"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    voucher_type = Column(String(50), nullable=False) # e.g., Purchase, Sales, Journal
    supplier_invoice_no = Column(String(100), nullable=True)
    voucher_date = Column(DateTime, nullable=True)
    party_name = Column(String(255), nullable=False)
    gst_number = Column(String(50), nullable=True)
    narration = Column(Text, nullable=True)
    sub_total = Column(String(50), default="0") # Storing as string to avoid precision loss, similar to GetInvoice
    tax_amount = Column(String(50), default="0")
    total_amount = Column(String(50), default="0")
    sync_status = Column(String(50), default="NOT_SYNCED") # e.g. NOT_SYNCED, SYNCED
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    ledger_items = relationship("VoucherLedgerItem", back_populates="voucher", cascade="all, delete-orphan")
    tax_items = relationship("VoucherTaxItem", back_populates="voucher", cascade="all, delete-orphan")

class VoucherLedgerItem(Base):
    __tablename__ = "voucher_ledger_items"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    voucher_id = Column(UUID(as_uuid=True), ForeignKey("accounting_vouchers.id"), nullable=False)
    ledger_name = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    amount = Column(String(50), nullable=False)

    voucher = relationship("AccountingVoucher", back_populates="ledger_items")

class VoucherTaxItem(Base):
    __tablename__ = "voucher_tax_items"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    voucher_id = Column(UUID(as_uuid=True), ForeignKey("accounting_vouchers.id"), nullable=False)
    ledger_name = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    amount = Column(String(50), nullable=False)

    voucher = relationship("AccountingVoucher", back_populates="tax_items")


# ══════════════════════════════════════════════════════════════
# TALLY CONNECTOR TABLES
# Synced from Tally via /tally/sync-ledgers and /tally/sync-vouchers
# ══════════════════════════════════════════════════════════════

class Ledger(Base):
    """Tally master ledger data, synced via /tally/sync-ledgers.
    One row per ledger per company. Upserted on every sync."""
    __tablename__ = "ledgers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    name = Column(Text, nullable=False)
    parent = Column(Text, nullable=True)
    opening_balance = Column(sa.Numeric, nullable=True)     # positive = Cr, negative = Dr
    closing_balance = Column(sa.Numeric, nullable=True)
    party_gstin = Column(Text, nullable=True, index=True)
    gst_registration_type = Column(Text, nullable=True)
    state = Column(Text, nullable=True)
    pin_code = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    mobile = Column(Text, nullable=True)
    address = Column(Text, nullable=True)
    mailing_name = Column(Text, nullable=True)
    synced_at = Column(DateTime(timezone=True), server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint('company_name', 'name', name='uq_ledgers_company_name'),
        Index('idx_ledgers_parent', 'company_name', 'parent'),
    )


class Voucher(Base):
    """Tally voucher headers, synced via /tally/sync-vouchers.
    One row per voucher. GUID is Tally's permanent unique ID."""
    __tablename__ = "vouchers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    date = Column(Text, nullable=True)                       # Tally returns YYYYMMDD
    voucher_type = Column(Text, nullable=True)               # Sales, Purchase, Payment, Receipt, etc.
    voucher_number = Column(Text, nullable=True)
    party_name = Column(Text, nullable=True)                 # primary party ledger name
    amount = Column(sa.Numeric, nullable=True)               # header-level net amount
    narration = Column(Text, nullable=True)
    guid = Column(Text, nullable=False)                      # Tally permanent GUID — never changes
    alter_id = Column(Text, default='')                      # increments on every Tally modification
    synced_at = Column(DateTime(timezone=True), server_default=sa.func.now())

    entries = relationship("VoucherEntry", back_populates="voucher",
                           foreign_keys="VoucherEntry.voucher_guid",
                           primaryjoin="Voucher.guid == foreign(VoucherEntry.voucher_guid)",
                           cascade="all, delete-orphan")

    inventory_entries = relationship("VoucherInventoryEntry", back_populates="voucher",
                                     foreign_keys="VoucherInventoryEntry.voucher_guid",
                                     primaryjoin="Voucher.guid == foreign(VoucherInventoryEntry.voucher_guid)",
                                     cascade="all, delete-orphan")

    __table_args__ = (
        sa.UniqueConstraint('company_name', 'guid', name='uq_vouchers_company_guid'),
        Index('idx_vouchers_date', 'company_name', 'date'),
        Index('idx_vouchers_type', 'company_name', 'voucher_type'),
        Index('idx_vouchers_party', 'company_name', 'party_name'),
        Index('idx_vouchers_guid', 'guid'),
    )


class VoucherEntry(Base):
    """Individual debit/credit ledger lines from ALLLEDGERENTRIES.LIST.
    A single ₹10,000 sales voucher produces 2 rows:
      Sundry Debtors   is_debit=true   amount=10000
      Sales Account    is_debit=false  amount=-10000"""
    __tablename__ = "voucher_entries"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    voucher_guid = Column(Text, nullable=False, index=True)  # links to vouchers.guid
    voucher_date = Column(Text, nullable=True)               # denormalised for fast date filtering
    voucher_type = Column(Text, nullable=True)               # denormalised for fast type filtering
    ledger_name = Column(Text, nullable=False)
    amount = Column(sa.Numeric, nullable=True)               # positive = Cr, negative = Dr
    is_debit = Column(Boolean, default=False)
    synced_at = Column(DateTime(timezone=True), server_default=sa.func.now())

    voucher = relationship("Voucher", back_populates="entries",
                           foreign_keys=[voucher_guid],
                           primaryjoin="VoucherEntry.voucher_guid == Voucher.guid")

    __table_args__ = (
        sa.UniqueConstraint('company_name', 'voucher_guid', 'ledger_name', 'amount',
                            name='uq_ventry_company_guid_ledger_amount'),
        Index('idx_ventry_ledger', 'company_name', 'ledger_name'),
        Index('idx_ventry_date', 'company_name', 'voucher_date'),
    )


class VoucherInventoryEntry(Base):
    """Individual stock item lines from ALLINVENTORYENTRIES.LIST.
    A Purchase voucher for 2 items produces 2 rows:
      Laptop Dell 15   qty=1   rate=50000   amount=50000   hsn=84713010
      Mouse Logitech   qty=5   rate=500     amount=2500    hsn=84716060"""
    __tablename__ = "voucher_inventory_entries"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    voucher_guid = Column(Text, nullable=False, index=True)  # links to vouchers.guid
    voucher_date = Column(Text, nullable=True)               # denormalised for fast date filtering
    voucher_type = Column(Text, nullable=True)               # denormalised for fast type filtering
    stock_item_name = Column(Text, nullable=False)           # Tally stock item name
    quantity = Column(sa.Numeric, nullable=True)             # qty sold/purchased
    rate = Column(sa.Numeric, nullable=True)                 # per-unit rate
    amount = Column(sa.Numeric, nullable=True)               # line total (qty × rate)
    uom = Column(Text, nullable=True)                        # unit of measure: Nos, Kg, Pcs, Ltr
    hsn_code = Column(Text, nullable=True)                   # HSN/SAC code
    gst_rate = Column(sa.Numeric, nullable=True)             # applicable GST rate %
    godown = Column(Text, nullable=True)                     # godown/warehouse name
    batch = Column(Text, nullable=True)                      # batch number
    discount = Column(sa.Numeric, nullable=True)             # discount amount/percentage
    synced_at = Column(DateTime(timezone=True), server_default=sa.func.now())

    voucher = relationship("Voucher", back_populates="inventory_entries",
                           foreign_keys=[voucher_guid],
                           primaryjoin="VoucherInventoryEntry.voucher_guid == Voucher.guid")

    __table_args__ = (
        sa.UniqueConstraint('company_name', 'voucher_guid', 'stock_item_name', 'quantity', 'amount',
                            name='uq_vinv_company_guid_item_qty_amount'),
        Index('idx_vinv_item', 'company_name', 'stock_item_name'),
        Index('idx_vinv_date', 'company_name', 'voucher_date'),
        Index('idx_vinv_hsn', 'company_name', 'hsn_code'),
        Index('idx_vinv_godown', 'company_name', 'godown'),
    )


# ══════════════════════════════════════════════════════════════
# CLIENT DRIVE TABLES
# ══════════════════════════════════════════════════════════════

class DriveFolder(Base):
    """A folder inside a client's drive. Supports nesting via parent_id."""
    __tablename__ = "drive_folders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False, index=True)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False, index=True)
    parent_id = Column(UUID(as_uuid=True), ForeignKey("drive_folders.id", ondelete="CASCADE"), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    icon = Column(String(10), default="📁")
    color = Column(String(20), default="#3b82f6")
    bg = Column(String(20), default="#eff6ff")
    created_at = Column(DateTime, default=datetime.utcnow)

    files = relationship("DriveFile", back_populates="folder", cascade="all, delete-orphan")
    children = relationship("DriveFolder", back_populates="parent", cascade="all, delete-orphan")
    parent = relationship("DriveFolder", back_populates="children", remote_side=[id])

    __table_args__ = (
        sa.UniqueConstraint('client_id', 'firm_id', 'name', 'parent_id', name='uq_drive_folder_client_name'),
        Index('idx_drive_folder_client', 'client_id', 'firm_id'),
        Index('idx_drive_folder_parent', 'parent_id'),
    )


class DriveFile(Base):
    """A file inside a drive folder."""
    __tablename__ = "drive_files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    folder_id = Column(UUID(as_uuid=True), ForeignKey("drive_folders.id", ondelete="CASCADE"), nullable=False, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False, index=True)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False, index=True)
    name = Column(String(500), nullable=False)          # display name
    original_name = Column(String(500), nullable=False)  # original upload name
    file_type = Column(String(50), nullable=False)       # pdf, image, doc, spreadsheet, etc.
    size_bytes = Column(Integer, default=0)
    storage_path = Column(String(1000), nullable=False)  # path in local/cloud storage
    created_at = Column(DateTime, default=datetime.utcnow)

    folder = relationship("DriveFolder", back_populates="files")

    __table_args__ = (
        Index('idx_drive_file_folder', 'folder_id'),
        Index('idx_drive_file_client', 'client_id', 'firm_id'),
    )

