from sqlalchemy import Column, Integer, String, UniqueConstraint
from app.storage.base import Base

class TokenPair(Base):
    __tablename__ = "token_pairs"
    id = Column(Integer, primary_key=True)
    base_token = Column(String, nullable=False)
    quote_token = Column(String, nullable=False)
    symbol = Column(String, nullable=True)
    __table_args__ = (UniqueConstraint("base_token", "quote_token"),)