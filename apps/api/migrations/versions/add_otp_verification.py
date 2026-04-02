"""add otp_verifications table and email/phone verified flags

Revision ID: add_otp_verification
Revises: add_rule42_computations
Create Date: 2026-04-01
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = 'add_otp_verification'
down_revision = 'add_rule42_computations'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = inspector.get_table_names()

    # 1. Create otp_verifications table
    if 'otp_verifications' not in existing_tables:
        op.create_table(
            'otp_verifications',
            sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
            sa.Column('identifier', sa.String(255), nullable=False),
            sa.Column('identifier_type', sa.String(10), nullable=False),
            sa.Column('otp_code', sa.String(6), nullable=False),
            sa.Column('purpose', sa.String(20), nullable=False),
            sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('is_verified', sa.Boolean(), nullable=False, server_default='false'),
            sa.Column('verification_token', sa.String(64), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
            sa.Column('expires_at', sa.DateTime(), nullable=False),
        )
        op.create_index('ix_otp_identifier', 'otp_verifications', ['identifier', 'purpose'])
        op.create_index('ix_otp_token', 'otp_verifications', ['verification_token'])

    # 2. Add email_verified and phone_verified to users
    user_columns = [c['name'] for c in inspector.get_columns('users')]
    if 'email_verified' not in user_columns:
        op.add_column('users', sa.Column('email_verified', sa.Boolean(), nullable=False, server_default='false'))
    if 'phone_verified' not in user_columns:
        op.add_column('users', sa.Column('phone_verified', sa.Boolean(), nullable=False, server_default='false'))


def downgrade():
    op.drop_column('users', 'phone_verified')
    op.drop_column('users', 'email_verified')
    op.drop_index('ix_otp_token', table_name='otp_verifications')
    op.drop_index('ix_otp_identifier', table_name='otp_verifications')
    op.drop_table('otp_verifications')
