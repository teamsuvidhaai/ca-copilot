"""Add get_invoice_items table for OCR-extracted line items

Revision ID: add_invoice_line_items
Revises: add_inventory_entries
Create Date: 2026-03-25
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = 'add_invoice_line_items'
down_revision = 'add_inventory_entries'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'get_invoice_items',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), primary_key=True),
        sa.Column('invoice_id', sa.Integer(), sa.ForeignKey('get_invoice.id', ondelete='CASCADE'), nullable=False),
        sa.Column('description', sa.String(500), nullable=False),
        sa.Column('service_code', sa.String(100), nullable=True),
        sa.Column('quantity', sa.String(50), server_default='1'),
        sa.Column('price', sa.String(50), nullable=True),
        sa.Column('amount', sa.String(50), nullable=True),
    )
    op.create_index('idx_invoice_items_invoice_id', 'get_invoice_items', ['invoice_id'])


def downgrade() -> None:
    op.drop_table('get_invoice_items')
