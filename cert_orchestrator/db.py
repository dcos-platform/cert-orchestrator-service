from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cert_orchestrator.config import settings

engine = create_engine(settings.database_url, future=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


def get_session() -> Session:
    return SessionLocal()
