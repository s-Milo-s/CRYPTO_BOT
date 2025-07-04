from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,scoped_session
import os
from sqlalchemy.pool import NullPool

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    poolclass=NullPool
)
SessionLocal = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
        bind=engine,
    )
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()