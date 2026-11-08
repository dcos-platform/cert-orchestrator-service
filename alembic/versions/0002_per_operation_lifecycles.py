"""per-operation lifecycle records

Revision ID: 0002_per_operation_lifecycles
Revises: 0001_create_lifecycle_tables
Create Date: 2026-11-07 00:00:00

Adds ``operation`` and ``root_event_id`` to ``certificate_lifecycles`` so that each operation gets its own
record, keyed by the event that started it. The unique constraint on ``certificate_id`` is dropped; its
index is kept. No data is migrated: legacy rows keep NULL ``operation`` and ``root_event_id`` and are never
matched by new events.

Downgrade fails if any certificate has more than one row, because the unique constraint on
``certificate_id`` cannot be restored. That is intended: there is no lossless way back to one row per
certificate, and silently deleting operations would be worse.
"""

import sqlalchemy as sa

from alembic import op

revision = "0002_per_operation_lifecycles"
down_revision = "0001_create_lifecycle_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    lifecycle_operation = sa.Enum("CREATE", "RENEW", "REVOKE", "EXPIRE", name="lifecycleoperation")
    # add_column does not create a Postgres enum type (create_table does), so create it explicitly.
    lifecycle_operation.create(op.get_bind(), checkfirst=True)

    op.add_column("certificate_lifecycles", sa.Column("operation", lifecycle_operation, nullable=True))
    op.add_column("certificate_lifecycles", sa.Column("root_event_id", sa.String(length=255), nullable=True))
    op.drop_constraint("uq_certificate_lifecycles_certificate_id", "certificate_lifecycles", type_="unique")
    op.create_unique_constraint(
        "uq_certificate_lifecycles_root_event_id",
        "certificate_lifecycles",
        ["root_event_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_certificate_lifecycles_root_event_id", "certificate_lifecycles", type_="unique")
    op.create_unique_constraint(
        "uq_certificate_lifecycles_certificate_id",
        "certificate_lifecycles",
        ["certificate_id"],
    )
    op.drop_column("certificate_lifecycles", "root_event_id")
    op.drop_column("certificate_lifecycles", "operation")

    lifecycle_operation = sa.Enum("CREATE", "RENEW", "REVOKE", "EXPIRE", name="lifecycleoperation")
    lifecycle_operation.drop(op.get_bind(), checkfirst=True)
