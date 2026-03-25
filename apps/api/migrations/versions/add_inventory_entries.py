"""Add voucher_inventory_entries table for Tally stock item lines

Revision ID: add_inventory_entries
Revises: add_tally_connector_tables
Create Date: 2026-03-22
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = 'add_inventory_entries'
down_revision = 'tally_connector_001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── VOUCHER INVENTORY ENTRIES ──
    # Captures ALLINVENTORYENTRIES.LIST from Tally — stock item lines
    # in Sales/Purchase vouchers. Linked to vouchers via voucher_guid.
    op.create_table(
        'voucher_inventory_entries',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), primary_key=True),
        sa.Column('company_name', sa.Text(), nullable=False),
        sa.Column('voucher_guid', sa.Text(), nullable=False),
        sa.Column('voucher_date', sa.Text(), nullable=True),
        sa.Column('voucher_type', sa.Text(), nullable=True),
        sa.Column('stock_item_name', sa.Text(), nullable=False),
        sa.Column('quantity', sa.Numeric(), nullable=True),
        sa.Column('rate', sa.Numeric(), nullable=True),
        sa.Column('amount', sa.Numeric(), nullable=True),
        sa.Column('uom', sa.Text(), nullable=True),
        sa.Column('hsn_code', sa.Text(), nullable=True),
        sa.Column('gst_rate', sa.Numeric(), nullable=True),
        sa.Column('godown', sa.Text(), nullable=True),
        sa.Column('batch', sa.Text(), nullable=True),
        sa.Column('discount', sa.Numeric(), nullable=True),
        sa.Column('synced_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint('company_name', 'voucher_guid', 'stock_item_name', 'quantity', 'amount',
                            name='uq_vinv_company_guid_item_qty_amount'),
    )
    op.create_index('idx_vinv_company', 'voucher_inventory_entries', ['company_name'])
    op.create_index('idx_vinv_guid', 'voucher_inventory_entries', ['voucher_guid'])
    op.create_index('idx_vinv_item', 'voucher_inventory_entries', ['company_name', 'stock_item_name'])
    op.create_index('idx_vinv_date', 'voucher_inventory_entries', ['company_name', 'voucher_date'])
    op.create_index('idx_vinv_hsn', 'voucher_inventory_entries', ['company_name', 'hsn_code'])
    op.create_index('idx_vinv_godown', 'voucher_inventory_entries', ['company_name', 'godown'])


def downgrade() -> None:
    op.drop_table('voucher_inventory_entries')
