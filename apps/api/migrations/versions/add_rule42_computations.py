"""add rule42_computations table

Revision ID: add_rule42_computations
Revises: 0cb9c3c2a51f
Create Date: 2026-03-28
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = 'add_rule42_computations'
down_revision = '0cb9c3c2a51f'
branch_labels = None
depends_on = None


def upgrade():
    # Safe: table may already exist (created directly on Supabase)
    conn = op.get_bind()
    result = conn.execute(sa.text("SELECT to_regclass('public.rule42_computations')"))
    if result.scalar() is not None:
        return  # table already exists, skip

    op.create_table(
        'rule42_computations',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('client_id', UUID(as_uuid=True), sa.ForeignKey('clients.id', ondelete='CASCADE'), nullable=False),
        sa.Column('user_id', UUID(as_uuid=True), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('firm_id', UUID(as_uuid=True), sa.ForeignKey('firms.id'), nullable=False),
        sa.Column('period', sa.String(7), nullable=False),
        sa.Column('financial_year', sa.String(9), nullable=False),
        sa.Column('tax_head', sa.String(10), nullable=False, server_default='cgst'),
        sa.Column('inputs', JSONB, nullable=False, server_default='{}'),
        sa.Column('results', JSONB, nullable=False, server_default='{}'),
        sa.Column('status', sa.String(20), nullable=False, server_default='draft'),
        sa.Column('notes', sa.Text, nullable=True),
        sa.Column('auto_filled_fields', JSONB, nullable=True, server_default='[]'),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime, server_default=sa.text('now()')),
        sa.UniqueConstraint('client_id', 'period', 'tax_head', name='uq_rule42_client_period_taxhead'),
    )
    op.create_index('idx_rule42_client', 'rule42_computations', ['client_id'])
    op.create_index('idx_rule42_firm', 'rule42_computations', ['firm_id'])
    op.create_index('idx_rule42_fy', 'rule42_computations', ['client_id', 'financial_year'])
    op.create_index('idx_rule42_period', 'rule42_computations', ['client_id', 'period'])


def downgrade():
    op.drop_table('rule42_computations')
