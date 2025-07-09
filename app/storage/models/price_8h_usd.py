from sqlalchemy import Column, TIMESTAMP, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Price8hUSD(Base):
    __tablename__ = "price_8h_usd"

    bucket_start = Column(TIMESTAMP(timezone=True), primary_key=True)

    # USD prices (at bucket close time)
    eth = Column(Numeric(18, 8), nullable=True)
    btc = Column(Numeric(18, 8), nullable=True)
    # Add more assets as needed (e.g., op, sol, link...)

    created_at = Column(TIMESTAMP(timezone=True), nullable=False)