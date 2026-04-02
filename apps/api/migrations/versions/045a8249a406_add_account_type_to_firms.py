"""add_account_type_to_firms

Revision ID: 045a8249a406
Revises: add_otp_verification
Create Date: 2026-04-02 17:54:51.111365

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '045a8249a406'
down_revision: Union[str, None] = 'add_otp_verification'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('firms', sa.Column('account_type', sa.String(), server_default='ca_firm', nullable=False))


def downgrade() -> None:
    op.drop_column('firms', 'account_type')
