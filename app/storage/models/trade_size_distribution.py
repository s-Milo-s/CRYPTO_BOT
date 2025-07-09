from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class TradeSizeDistributionTable(Base):
    __tablename__ = "trade_size_distribution"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool_name = Column(String, index=True, nullable=False)

    # Buckets from 10^-2 to 10^6
    bucket_neg2 = Column(Integer, default=0)
    bucket_neg1 = Column(Integer, default=0)
    bucket_0 = Column(Integer, default=0)
    bucket_1 = Column(Integer, default=0)
    bucket_2 = Column(Integer, default=0)
    bucket_3 = Column(Integer, default=0)
    bucket_4 = Column(Integer, default=0)
    bucket_5 = Column(Integer, default=0)
    bucket_6 = Column(Integer, default=0)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
