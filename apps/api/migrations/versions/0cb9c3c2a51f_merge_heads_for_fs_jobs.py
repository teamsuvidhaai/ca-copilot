"""merge_heads_for_fs_jobs

Revision ID: 0cb9c3c2a51f
Revises: a1b2c3d4e5f6, add_fi_uploads, add_invoice_status
Create Date: 2026-03-28 09:55:07.367976

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0cb9c3c2a51f'
down_revision: Union[str, None] = ('a1b2c3d4e5f6', 'add_fi_uploads', 'add_invoice_status')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
