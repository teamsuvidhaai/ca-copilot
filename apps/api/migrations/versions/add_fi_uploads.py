"""Add financial instrument uploads table

Revision ID: add_fi_uploads
Revises: None (will be auto-detected)
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'add_fi_uploads'
down_revision = None  # Alembic will resolve this
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'fi_uploads',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('client_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('clients.id', ondelete='CASCADE'), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id'), nullable=False),
        sa.Column('instrument_type', sa.String(30), nullable=False),
        sa.Column('filename', sa.String(500), nullable=False),
        sa.Column('file_path', sa.Text, nullable=True),
        sa.Column('file_hash', sa.String(64), nullable=True),
        sa.Column('status', sa.String(30), server_default='processing'),
        sa.Column('error_message', sa.Text, nullable=True),
        sa.Column('raw_text', sa.Text, nullable=True),
        sa.Column('structured_data', postgresql.JSONB, nullable=True),
        sa.Column('journal_entries', postgresql.JSONB, nullable=True, server_default='[]'),
        sa.Column('journal_entry_count', sa.Integer, server_default='0'),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('idx_fi_upload_client', 'fi_uploads', ['client_id'])
    op.create_index('idx_fi_upload_hash', 'fi_uploads', ['client_id', 'file_hash'])
    op.create_index('idx_fi_upload_type', 'fi_uploads', ['client_id', 'instrument_type'])


def downgrade() -> None:
    op.drop_index('idx_fi_upload_type', 'fi_uploads')
    op.drop_index('idx_fi_upload_hash', 'fi_uploads')
    op.drop_index('idx_fi_upload_client', 'fi_uploads')
    op.drop_table('fi_uploads')
