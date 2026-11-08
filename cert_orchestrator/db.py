from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cert_orchestrator.config import settings

# Error text names the statement but not its parameters, so a failed write never logs a message body (C-17).
engine = create_engine(settings.resolved_database_url, future=True, hide_parameters=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


@contextmanager
def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
