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
class AccountType(str, PyEnum):
    CA_FIRM = "ca_firm"
    CORPORATE = "corporate"

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
    account_type = Column(String, default="ca_firm", nullable=False, server_default="ca_firm")
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
    email_verified = Column(Boolean, default=False, nullable=False, server_default="false")
    phone_verified = Column(Boolean, default=False, nullable=False, server_default="false")
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    trial_started_at = Column(DateTime, default=datetime.utcnow)  # 30-day free trial starts at signup

    firm = relationship("Firm", back_populates="users")


class OTPVerification(Base):
    """Stores OTP codes for email/phone verification during signup and login."""
    __tablename__ = "otp_verifications"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    identifier = Column(String(255), nullable=False, index=True)   # email or phone number
    identifier_type = Column(String(10), nullable=False)           # 'email' or 'phone'
    otp_code = Column(String(6), nullable=False)                   # 6-digit code
    purpose = Column(String(20), nullable=False)                   # 'signup', 'login', 'reset'
    attempts = Column(Integer, default=0, nullable=False)          # wrong attempts counter
    is_verified = Column(Boolean, default=False, nullable=False)
    verification_token = Column(String(64), nullable=True, index=True)  # returned after verify
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)                  # 10 min from creation

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
    One row per ledger per company per fiscal year. Upserted on every sync."""
    __tablename__ = "ledgers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    name = Column(Text, nullable=False)
    fy_period = Column(Text, nullable=True)                  # e.g. '2025-26' — fiscal year this balance belongs to
    parent = Column(Text, nullable=True)                     # immediate parent group (e.g. 'WHITE OAK PIONEER EQUITY')
    primary_group = Column(Text, nullable=True)              # root BS group resolved from hierarchy (e.g. 'Investments')
    opening_balance = Column(sa.Numeric, nullable=True)      # positive = Cr, negative = Dr
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
        sa.UniqueConstraint('company_name', 'name', 'fy_period', name='uq_ledgers_company_name_fy'),
        Index('idx_ledgers_parent', 'company_name', 'parent'),
        Index('idx_ledgers_primary_group', 'company_name', 'primary_group'),
        Index('idx_ledgers_fy', 'company_name', 'fy_period'),
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
    entry_index = Column(Integer, nullable=True)              # position within voucher (used by TallyConnector upsert)
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
        sa.UniqueConstraint('company_name', 'voucher_guid', 'entry_index',
                            name='uq_ventry_company_guid_entry_index'),
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


class StockItem(Base):
    """Tally master stock item data, synced via /tally/sync-ledgers (master sync).
    One row per stock item per company. Contains item details like UOM, HSN, group."""
    __tablename__ = "stock_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name = Column(Text, nullable=False, index=True)
    name = Column(Text, nullable=False)
    parent = Column(Text, nullable=True)                     # stock group
    category = Column(Text, nullable=True)                   # stock category
    uom = Column(Text, nullable=True)                        # base unit of measure
    opening_balance_qty = Column(sa.Numeric, nullable=True)
    opening_balance_rate = Column(sa.Numeric, nullable=True)
    opening_balance_value = Column(sa.Numeric, nullable=True)
    hsn_code = Column(Text, nullable=True)
    gst_rate = Column(sa.Numeric, nullable=True)
    description = Column(Text, nullable=True)
    synced_at = Column(DateTime(timezone=True), server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint('company_name', 'name', name='uq_stock_items_company_name'),
        Index('idx_stock_items_parent', 'company_name', 'parent'),
        Index('idx_stock_items_hsn', 'company_name', 'hsn_code'),
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


# ═══════════════════════════════════════════════════════
# Banking — Bank statement upload + transaction extraction
# ═══════════════════════════════════════════════════════

class BankStatement(Base):
    """One row per uploaded bank statement PDF."""
    __tablename__ = "bank_statements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    bank_name = Column(String(200), nullable=True)
    account_number = Column(String(50), nullable=True)          # masked in display
    statement_period_start = Column(sa.Date, nullable=True)
    statement_period_end = Column(sa.Date, nullable=True)
    opening_balance = Column(sa.Numeric(15, 2), nullable=True)
    closing_balance = Column(sa.Numeric(15, 2), nullable=True)
    total_credits = Column(sa.Numeric(15, 2), nullable=True)
    total_debits = Column(sa.Numeric(15, 2), nullable=True)
    transaction_count = Column(Integer, default=0)
    file_path = Column(Text, nullable=True)                     # Supabase storage path
    original_filename = Column(String(500), nullable=True)
    status = Column(String(20), default="processing")           # processing | completed | failed
    error_message = Column(Text, nullable=True)
    raw_text = Column(Text, nullable=True)                      # LlamaParse extracted text
    created_at = Column(DateTime, default=datetime.utcnow)

    transactions = relationship("BankTransaction", back_populates="statement", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_bank_stmt_client', 'client_id'),
    )


class BankTransaction(Base):
    """Individual transaction lines extracted from a bank statement."""
    __tablename__ = "bank_transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    statement_id = Column(UUID(as_uuid=True), ForeignKey("bank_statements.id", ondelete="CASCADE"), nullable=False, index=True)
    transaction_date = Column(sa.Date, nullable=True)
    value_date = Column(sa.Date, nullable=True)
    description = Column(Text, nullable=True)
    reference_no = Column(String(100), nullable=True)           # Cheque/UTR/Ref
    debit = Column(sa.Numeric(15, 2), nullable=True)
    credit = Column(sa.Numeric(15, 2), nullable=True)
    balance = Column(sa.Numeric(15, 2), nullable=True)
    category = Column(String(50), nullable=True)                # Salary, Rent, GST, etc.
    party_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    statement = relationship("BankStatement", back_populates="transactions")

    __table_args__ = (
        Index('idx_bank_txn_stmt', 'statement_id'),
    )


# ═══════════════════════════════════════════════════════
# Financial Instruments — Upload + AI extraction
# ═══════════════════════════════════════════════════════

class FinancialInstrumentUpload(Base):
    """One row per uploaded Demat/MF/PMS statement.
    AI extracts holdings, transactions, capital gains → journal entries.
    Structured data and journal entries stored as JSONB."""
    __tablename__ = "fi_uploads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    instrument_type = Column(String(30), nullable=False)          # demat, mutual_fund, pms
    filename = Column(String(500), nullable=False)
    file_path = Column(Text, nullable=True)                       # Supabase storage path
    file_hash = Column(String(64), nullable=True, index=True)     # SHA-256 for duplicate detection
    status = Column(String(30), default="processing")             # processing, extracting, structuring, generating_entries, completed, failed
    error_message = Column(Text, nullable=True)
    raw_text = Column(Text, nullable=True)                        # first 2000 chars of extracted text
    structured_data = Column(JSONB, nullable=True)                # AI-extracted holdings/transactions/dividends
    journal_entries = Column(JSONB, nullable=True, default=[])    # AI-generated Dr/Cr entries
    journal_entry_count = Column(Integer, default=0)
    je_status = Column(String(20), default="pending")             # pending / approved / synced — tracks approval state of generated journal entries
    pms_account_id = Column(UUID(as_uuid=True), ForeignKey("pms_accounts.id", ondelete="SET NULL"), nullable=True)  # links PMS uploads to specific account
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fi_upload_client', 'client_id'),
        Index('idx_fi_upload_hash', 'client_id', 'file_hash'),
        Index('idx_fi_upload_type', 'client_id', 'instrument_type'),
    )


# ═══════════════════════════════════════════════════════
# PMS Accounting — FIFO Lot Tracking & Capital Gains
# ═══════════════════════════════════════════════════════

class PMSAccount(Base):
    """One row per PMS provider × strategy × client.
    Tracks provider details and accrual configuration."""
    __tablename__ = "pms_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    provider_name = Column(String(200), nullable=False)       # Abakkus, Marcellus, ASK, etc.
    strategy_name = Column(String(200), nullable=True)        # Emerging Opportunities, Value, etc.
    account_code = Column(String(100), nullable=True)         # PMS account number
    pms_start_date = Column(sa.Date, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    config = Column(JSONB, default={})                        # {accrual_mode: "daily"|"quarterly_actual"}
    created_at = Column(DateTime, default=datetime.utcnow)

    transactions = relationship("PMSTransaction", back_populates="pms_account", cascade="all, delete-orphan")
    dividends = relationship("PMSDividend", back_populates="pms_account", cascade="all, delete-orphan")
    expenses = relationship("PMSExpense", back_populates="pms_account", cascade="all, delete-orphan")
    lots = relationship("FIFOLot", back_populates="pms_account", cascade="all, delete-orphan")

    __table_args__ = (
        sa.UniqueConstraint('client_id', 'provider_name', 'strategy_name', name='uq_pms_account_client_provider_strategy'),
        Index('idx_pms_account_client', 'client_id'),
    )


class SecurityMaster(Base):
    """Canonical security names + ISIN. Shared across all clients.
    Aliases enable fuzzy matching across PMS providers."""
    __tablename__ = "security_master"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    isin = Column(String(12), unique=True, nullable=True, index=True)
    name = Column(String(500), nullable=False)                 # canonical name
    exchange = Column(String(20), nullable=True)               # NSE / BSE
    aliases = Column(JSONB, default=[])                        # fuzzy-matched alternate names from providers
    sector = Column(String(100), nullable=True)                # BSE/NSE sector: Banking, IT, Pharma, etc.
    market_cap_category = Column(String(20), nullable=True)    # large_cap, mid_cap, small_cap, micro_cap
    fmv_31jan2018 = Column(sa.Numeric(15, 4), nullable=True)   # for Section 112A grandfathering
    created_at = Column(DateTime, default=datetime.utcnow)


class PMSTransaction(Base):
    """Individual Buy/Sell/Dividend/TDS transactions parsed from PMS PDFs.
    Each row = one line from the Transaction Statement."""
    __tablename__ = "pms_transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pms_account_id = Column(UUID(as_uuid=True), ForeignKey("pms_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("fi_uploads.id", ondelete="SET NULL"), nullable=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("security_master.id"), nullable=True)
    tx_date = Column(sa.Date, nullable=False)
    tx_type = Column(String(30), nullable=False)               # BUY, SELL, DIVIDEND, TDS_TRANSFER, BONUS, SPLIT
    security_name = Column(String(500), nullable=False)        # raw name from PDF
    exchange = Column(String(20), nullable=True)
    quantity = Column(sa.Numeric(15, 6), nullable=True)        # supports fractional shares
    unit_price = Column(sa.Numeric(15, 4), nullable=True)
    brokerage = Column(sa.Numeric(15, 2), default=0)
    stt = Column(sa.Numeric(15, 2), default=0)
    stamp_duty = Column(sa.Numeric(15, 2), default=0)
    settlement_amt = Column(sa.Numeric(15, 2), nullable=True)  # net amount credited/debited
    narration = Column(Text, nullable=True)
    is_duplicate = Column(Boolean, default=False)              # for overlap period detection
    je_status = Column(String(20), default="pending")          # pending / approved / synced
    created_at = Column(DateTime, default=datetime.utcnow)

    pms_account = relationship("PMSAccount", back_populates="transactions")
    gain_matches = relationship("CapitalGainMatch", back_populates="sell_tx",
                                foreign_keys="CapitalGainMatch.sell_tx_id")

    __table_args__ = (
        Index('idx_pms_tx_account', 'pms_account_id'),
        Index('idx_pms_tx_date', 'pms_account_id', 'tx_date'),
        Index('idx_pms_tx_security', 'pms_account_id', 'security_name'),
    )


class FIFOLot(Base):
    """Each purchase creates a FIFO lot. Sales consume oldest lots first.
    remaining_qty tracks what's still held. Opening balances have is_opening=True."""
    __tablename__ = "fifo_lots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pms_account_id = Column(UUID(as_uuid=True), ForeignKey("pms_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("security_master.id"), nullable=True)
    security_name = Column(String(500), nullable=False)        # denormalized for fast display
    purchase_tx_id = Column(UUID(as_uuid=True), ForeignKey("pms_transactions.id", ondelete="SET NULL"), nullable=True)
    purchase_date = Column(sa.Date, nullable=False)
    original_qty = Column(sa.Numeric(15, 6), nullable=False)
    remaining_qty = Column(sa.Numeric(15, 6), nullable=False)
    cost_per_unit = Column(sa.Numeric(15, 4), nullable=False)
    total_cost = Column(sa.Numeric(15, 2), nullable=True)      # original_qty × cost_per_unit
    is_opening = Column(Boolean, default=False)                 # True for opening balance lots
    created_at = Column(DateTime, default=datetime.utcnow)

    pms_account = relationship("PMSAccount", back_populates="lots")
    gain_matches = relationship("CapitalGainMatch", back_populates="lot")

    __table_args__ = (
        Index('idx_fifo_lot_account', 'pms_account_id'),
        Index('idx_fifo_lot_security', 'pms_account_id', 'security_name'),
        Index('idx_fifo_lot_date', 'pms_account_id', 'purchase_date'),
    )


class CapitalGainMatch(Base):
    """FIFO lot consumption record. Each sale may consume multiple lots.
    Records cost basis, gain/loss, holding period, and grandfathering."""
    __tablename__ = "capital_gain_matches"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sell_tx_id = Column(UUID(as_uuid=True), ForeignKey("pms_transactions.id", ondelete="CASCADE"), nullable=False, index=True)
    lot_id = Column(UUID(as_uuid=True), ForeignKey("fifo_lots.id", ondelete="CASCADE"), nullable=False)
    qty_consumed = Column(sa.Numeric(15, 6), nullable=False)
    cost_basis = Column(sa.Numeric(15, 2), nullable=False)     # qty × effective_cost
    sale_proceeds = Column(sa.Numeric(15, 2), nullable=False)
    gain_loss = Column(sa.Numeric(15, 2), nullable=False)
    holding_days = Column(Integer, nullable=True)
    gain_type = Column(String(4), nullable=False)              # STCG / LTCG
    is_grandfathered = Column(Boolean, default=False)          # Section 112A applied
    effective_cost_per_unit = Column(sa.Numeric(15, 4), nullable=True)  # max(cost, FMV 31-Jan-2018)
    created_at = Column(DateTime, default=datetime.utcnow)

    sell_tx = relationship("PMSTransaction", back_populates="gain_matches",
                           foreign_keys=[sell_tx_id])
    lot = relationship("FIFOLot", back_populates="gain_matches")

    __table_args__ = (
        Index('idx_cg_match_sell', 'sell_tx_id'),
        Index('idx_cg_match_lot', 'lot_id'),
    )


class PMSDividend(Base):
    """Dividend records parsed from the Dividend Statement PDF.
    Tracks ex-date, TDS, gross/net for each security."""
    __tablename__ = "pms_dividends"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pms_account_id = Column(UUID(as_uuid=True), ForeignKey("pms_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("fi_uploads.id", ondelete="SET NULL"), nullable=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("security_master.id"), nullable=True)
    security_name = Column(String(500), nullable=False)
    ex_date = Column(sa.Date, nullable=True)
    received_date = Column(sa.Date, nullable=True)
    quantity = Column(sa.Numeric(15, 6), nullable=True)
    rate_per_share = Column(sa.Numeric(15, 4), nullable=True)
    gross_amount = Column(sa.Numeric(15, 2), nullable=False)
    tds_deducted = Column(sa.Numeric(15, 2), default=0)
    net_received = Column(sa.Numeric(15, 2), nullable=True)
    je_status = Column(String(20), default="pending")          # pending / approved / synced
    created_at = Column(DateTime, default=datetime.utcnow)

    pms_account = relationship("PMSAccount", back_populates="dividends")

    __table_args__ = (
        Index('idx_pms_div_account', 'pms_account_id'),
        Index('idx_pms_div_date', 'pms_account_id', 'ex_date'),
    )


class PMSExpense(Base):
    """Expenses parsed from Statement of Expenses PDF.
    Covers STT, management fees, custody fees, performance fees, etc.
    Distinguishes Paid vs Payable and Accrual vs Actual."""
    __tablename__ = "pms_expenses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pms_account_id = Column(UUID(as_uuid=True), ForeignKey("pms_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("fi_uploads.id", ondelete="SET NULL"), nullable=True)
    expense_type = Column(String(100), nullable=False)         # STT, Management Fee, Custody Fee, etc.
    expense_date = Column(sa.Date, nullable=True)
    period_from = Column(sa.Date, nullable=True)
    period_to = Column(sa.Date, nullable=True)
    amount = Column(sa.Numeric(15, 2), nullable=False)
    gst_amount = Column(sa.Numeric(15, 2), default=0)
    tds_applicable = Column(sa.Numeric(15, 2), default=0)
    net_payable = Column(sa.Numeric(15, 2), nullable=True)
    is_paid = Column(Boolean, nullable=True)                   # Paid vs Payable section
    is_accrual = Column(Boolean, default=False)                # Daily accrual vs actual charge
    is_stt_recon_only = Column(Boolean, default=False)         # STT from expense stmt = recon only, not booked
    narration = Column(Text, nullable=True)
    je_status = Column(String(20), default="pending")          # pending / approved / synced
    created_at = Column(DateTime, default=datetime.utcnow)

    pms_account = relationship("PMSAccount", back_populates="expenses")

    __table_args__ = (
        Index('idx_pms_exp_account', 'pms_account_id'),
        Index('idx_pms_exp_type', 'pms_account_id', 'expense_type'),
    )


# ═══════════════════════════════════════════════════════
# Financial Statements — BS/P&L/Schedules generation
# ═══════════════════════════════════════════════════════

class FinancialStatementJob(Base):
    """One row per Financial Statement generation job.
    Upload Trial Balance + optional prev-year BS + Notes →
    AI maps accounts → comparative Balance Sheet, P&L, Schedules.
    All structured data stored as JSONB for flexible querying."""
    __tablename__ = "fs_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    # Uploaded file metadata
    filenames = Column(JSONB, nullable=True, default={})            # {"trial_balance": "tb.xlsx", "prev_balance_sheet": "bs.pdf", ...}
    file_paths = Column(JSONB, nullable=True, default={})           # Supabase storage paths per role
    file_hash = Column(String(64), nullable=True, index=True)       # SHA-256 of TB for duplicate detection

    # Processing status
    status = Column(String(30), default="processing")               # processing, extracting, parsing_tb, parsing_bs, generating, completed, failed
    error_message = Column(Text, nullable=True)

    # Intermediate data
    raw_texts = Column(JSONB, nullable=True, default={})            # {"trial_balance": "...", "prev_balance_sheet": "...", "notes": "..."}
    trial_balance_data = Column(JSONB, nullable=True)               # Structured TB from AI
    prev_bs_data = Column(JSONB, nullable=True)                     # Structured prev-year BS from AI

    # Final result
    result = Column(JSONB, nullable=True)                           # Full result: balance_sheet, profit_and_loss, schedules, mappings, warnings

    # Metadata
    company_name = Column(String(500), nullable=True)
    financial_year = Column(String(20), nullable=True)              # e.g. "2024-25"
    is_balanced = Column(Boolean, nullable=True)                    # Balance Sheet tallied?

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fs_job_client', 'client_id'),
        Index('idx_fs_job_hash', 'client_id', 'file_hash'),
        Index('idx_fs_job_status', 'client_id', 'status'),
    )


# ═══════════════════════════════════════════════════════
# Rule 42 ITC Reversal — Persisted monthly computations
# ═══════════════════════════════════════════════════════

class Rule42Computation(Base):
    """One row per Rule 42 ITC reversal computation (client × period × tax_head).
    Stores both inputs and computed results as JSONB for flexibility.
    Monthly computations are provisional; annual true-up adjusts them."""
    __tablename__ = "rule42_computations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False, index=True)

    # Period identification
    period = Column(String(7), nullable=False)               # "2025-04" (YYYY-MM)
    financial_year = Column(String(9), nullable=False)        # "2025-2026"
    tax_head = Column(String(10), nullable=False, default="cgst")  # cgst / sgst / igst

    # Inputs & Results (JSONB for flexibility)
    inputs = Column(JSONB, nullable=False, default={})        # {T, T1, T2, T3, E, N, F}
    results = Column(JSONB, nullable=False, default={})       # {C1, D1, D2, C2, C3, C4, ratio, totalRev, ...}

    # Status workflow
    status = Column(String(20), nullable=False, default="draft")  # draft / final / annual_adjusted

    # Working notes
    notes = Column(Text, nullable=True)

    # Metadata
    auto_filled_fields = Column(JSONB, nullable=True, default=[])  # e.g. ["T", "E", "F"] if from GSTR-3B

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        sa.UniqueConstraint('client_id', 'period', 'tax_head', name='uq_rule42_client_period_taxhead'),
        Index('idx_rule42_client', 'client_id'),
        Index('idx_rule42_firm', 'firm_id'),
        Index('idx_rule42_fy', 'client_id', 'financial_year'),
        Index('idx_rule42_period', 'client_id', 'period'),
    )


class DepreciationAsset(Base):
    """Fixed asset with IT Act + Companies Act depreciation parameters.
    Stores computed year-wise depreciation schedules as JSONB."""
    __tablename__ = "depreciation_assets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id"), nullable=False, index=True)

    # Asset info
    name = Column(String(255), nullable=False)
    group_name = Column(String(100), nullable=False)  # Plant & Machinery, Computers, etc.
    date_acquired = Column(String(10), nullable=False)  # YYYY-MM-DD
    cost = Column(Float, nullable=False, default=0)
    residual_value = Column(Float, nullable=False, default=0)
    financial_year = Column(String(9), nullable=False, default="2025-26")

    # IT Act parameters
    it_rate = Column(Float, nullable=False, default=15)
    it_method = Column(String(5), nullable=False, default="WDV")  # WDV or SLM

    # Companies Act parameters
    co_life = Column(Integer, nullable=False, default=15)  # Useful life in years
    co_method = Column(String(5), nullable=False, default="SLM")

    # Computed results (JSONB)
    it_dep_fy = Column(Float, default=0)     # Current FY IT dep
    co_dep_fy = Column(Float, default=0)     # Current FY Co dep
    results = Column(JSONB, nullable=True)   # {it_schedule:[], co_schedule:[], deferred_tax:...}

    # Source & status
    source = Column(String(20), default="manual")  # manual / tally / ledger
    tally_ledger_name = Column(String(255), nullable=True)
    status = Column(String(20), default="active")   # active / disposed / draft

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_dep_client', 'client_id'),
        Index('idx_dep_firm', 'firm_id'),
        Index('idx_dep_fy', 'client_id', 'financial_year'),
    )


class FirmConfig(Base):
    """Key-value configuration store per firm.
    Supports arbitrary config keys like 'gst_deadlines', 'defaults', etc.
    Actual config data stored as JSONB for maximum flexibility."""
    __tablename__ = "firm_configs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    firm_id = Column(UUID(as_uuid=True), ForeignKey("firms.id", ondelete="CASCADE"), nullable=False, index=True)
    config_key = Column(String(100), nullable=False)          # e.g. 'gst_deadlines', 'invoice_defaults'
    config_data = Column(JSONB, nullable=False, default={})   # full config payload
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        sa.UniqueConstraint('firm_id', 'config_key', name='uq_firm_config_key'),
        Index('idx_firm_config_firm', 'firm_id'),
    )
