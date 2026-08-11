"""create lifecycle tables

Revision ID: 0001_create_lifecycle_tables
Revises:
Create Date: 2026-08-11 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0001_create_lifecycle_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    lifecycle_state = sa.Enum("PENDING", "PROCESSING", "COMPLETED", "FAILED", name="lifecyclestate")
    lifecycle_state.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "certificate_lifecycles",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("certificate_id", sa.String(length=255), nullable=False),
        sa.Column("state", lifecycle_state, nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("certificate_id", name="uq_certificate_lifecycles_certificate_id"),
    )
    op.create_index("ix_certificate_lifecycles_certificate_id", "certificate_lifecycles", ["certificate_id"])

    op.create_table(
        "processed_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.String(length=255), nullable=False),
        sa.Column("certificate_id", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("event_id", name="uq_processed_events_event_id"),
    )
    op.create_index("ix_processed_events_certificate_id", "processed_events", ["certificate_id"])


def downgrade() -> None:
    op.drop_index("ix_processed_events_certificate_id", table_name="processed_events")
    op.drop_table("processed_events")

    op.drop_index("ix_certificate_lifecycles_certificate_id", table_name="certificate_lifecycles")
    op.drop_table("certificate_lifecycles")

    lifecycle_state = sa.Enum("PENDING", "PROCESSING", "COMPLETED", "FAILED", name="lifecyclestate")
    lifecycle_state.drop(op.get_bind(), checkfirst=True)
