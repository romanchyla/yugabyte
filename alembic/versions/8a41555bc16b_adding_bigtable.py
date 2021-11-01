"""Adding bigtable

Revision ID: 8a41555bc16b
Revises: 0155d2dff74e
Create Date: 2021-10-06 19:39:14.220491

"""

# revision identifiers, used by Alembic.
revision = '8a41555bc16b'
down_revision = '0155d2dff74e'

from alembic import op
import sqlalchemy as sa

from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, Boolean, DateTime
from sqlalchemy.types import Enum, LargeBinary
                               


def upgrade():
    op.create_table('bigtable',
        Column('key', String(255), primary_key=True),
        Column('value', LargeBinary),
    )


def downgrade():
    op.drop_table('bigtable')
