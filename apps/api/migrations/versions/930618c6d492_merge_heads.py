"""Merge heads

Revision ID: 930618c6d492
Revises: 045a8249a406, add_firm_configs
Create Date: 2026-05-16 20:36:50.888840

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '930618c6d492'
down_revision: Union[str, None] = ('045a8249a406', 'add_firm_configs')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
