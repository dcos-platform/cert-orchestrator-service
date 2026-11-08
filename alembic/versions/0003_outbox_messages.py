"""transactional outbox for completions and business retries

Revision ID: 0003_outbox_messages
Revises: 0002_per_operation_lifecycles
Create Date: 2026-11-07 00:00:00

Adds ``outbox_messages``. The handler writes one row per completion or business retry in the same transaction as
its lifecycle change; the relay publishes PENDING rows to the broker and marks them SENT. The destination queue is
not stored: it is derived at publish time from ``kind`` and ``delay_seconds``.

Downgrade drops the table and then both enum types.
"""

import sqlalchemy as sa

from alembic import op

revision = "0003_outbox_messages"
down_revision = "0002_per_operation_lifecycles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.String(length=255), nullable=False),
        sa.Column("kind", sa.Enum("COMPLETION", "RETRY", name="outboxkind"), nullable=False),
        sa.Column("state", sa.Enum("PENDING", "SENT", name="outboxstate"), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("headers", sa.JSON(), nullable=False),
        sa.Column("delay_seconds", sa.Integer(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("kind", "message_id", name="uq_outbox_messages_kind_message_id"),
    )
    op.create_index(
        "ix_outbox_messages_state_available_at",
        "outbox_messages",
        ["state", "available_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_messages_state_available_at", table_name="outbox_messages")
    op.drop_table("outbox_messages")

    bind = op.get_bind()
    sa.Enum("PENDING", "SENT", name="outboxstate").drop(bind, checkfirst=True)
    sa.Enum("COMPLETION", "RETRY", name="outboxkind").drop(bind, checkfirst=True)
