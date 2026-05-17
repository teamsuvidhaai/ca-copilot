"""Add fi_entries table

Revision ID: 9c55fa03ed87
Revises: 930618c6d492
Create Date: 2026-05-16 20:36:59.794617

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '9c55fa03ed87'
down_revision: Union[str, None] = '930618c6d492'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'fi_entries',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('upload_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('fi_uploads.id', ondelete='CASCADE'), nullable=False),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('clients.id', ondelete='CASCADE'), nullable=False),
        sa.Column('date', sa.Date(), nullable=True),
        sa.Column('narration', sa.Text(), nullable=True),
        sa.Column('scrip', sa.String(length=255), nullable=True),
        sa.Column('trade_count', sa.Integer(), server_default='0'),
        sa.Column('cg_type', sa.String(length=20), nullable=True),
        sa.Column('voucher_type', sa.String(length=50), server_default='Journal'),
        sa.Column('status', sa.String(length=20), server_default='draft'),
        sa.Column('total_amount', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('entries', postgresql.JSONB, nullable=True, server_default='[]'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('idx_fi_entry_client', 'fi_entries', ['client_id'])
    op.create_index('idx_fi_entry_date', 'fi_entries', ['client_id', 'date'])
    op.create_index('idx_fi_entry_upload', 'fi_entries', ['upload_id'])


def downgrade() -> None:
    op.drop_index('idx_fi_entry_upload', table_name='fi_entries')
    op.drop_index('idx_fi_entry_date', table_name='fi_entries')
    op.drop_index('idx_fi_entry_client', table_name='fi_entries')
    op.drop_table('fi_entries')
