"""Add status column to get_invoice for review workflow

Revision ID: add_invoice_status
Revises: add_invoice_line_items
Create Date: 2026-03-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = 'add_invoice_status'
down_revision = 'add_invoice_line_items'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('get_invoice', sa.Column('status', sa.String(50), server_default='pending'))


def downgrade() -> None:
    op.drop_column('get_invoice', 'status')
