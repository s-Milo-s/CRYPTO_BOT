# models/pool.py
from sqlalchemy import Column, Integer, String, Boolean, Float, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Pool(Base):
    __tablename__ = "pools"

    id           = Column(Integer, primary_key=True)
    chain        = Column(String(32),  nullable=False)      # arbitrum / base …
    dex          = Column(String(32),  nullable=False)      # uniswap_v3 / camelot …
    pair         = Column(String(32),  nullable=False)      # “ARB/USDC” (UPPER‑case)
    address      = Column(String(42),  nullable=False, unique=True)  # 0x‑checksum
    active       = Column(Boolean,     nullable=False, default=True)
    last_started = Column(Float,       nullable=True)       # epoch‑seconds, NULL on first run

    # handy composite index if you query by chain + dex a lot
    __table_args__ = (
        Index("ix_pools_chain_dex", "chain", "dex"),
    )

    def __repr__(self) -> str:         # for nicer logs
        return f"<Pool {self.chain}.{self.dex} {self.pair} {self.address}>"
