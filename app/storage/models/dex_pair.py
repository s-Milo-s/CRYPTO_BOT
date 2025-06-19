from sqlalchemy import Column, Integer, String, UniqueConstraint
from app.storage.base import Base

class DexPair(Base):
    __tablename__ = "dex_pairs"
    id = Column(Integer, primary_key=True)
    base_token = Column(String, nullable=False)
    quote_token = Column(String, nullable=False)
    dex = Column(String, nullable=False)
    chain = Column(String, nullable=False)
    pair_address = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint("base_token", "quote_token", "dex", "chain"),)