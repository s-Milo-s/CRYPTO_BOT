import os
from decimal import Decimal
from unittest.mock import MagicMock
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Load environment variables from .env
load_dotenv()

# Import your actual class
from app.sources.dex_data_pipeline.utils.find_quote_usd_prices import FillQuoteUSDPrices


# @pytest.fixture(scope="module")
# def mock_session():
#     # Create a real session using env-provided DATABASE_URL
#     db_url = os.getenv("DATABASE_URL")
#     engine = create_engine(db_url)
#     Session = sessionmaker(bind=engine)
#     real_session = Session()

#     # Patch only inserts — allow SELECTs to go through
#     original_execute = real_session.execute

#     def side_effect_execute(stmt, *args, **kwargs):
#         if "INSERT INTO" in str(stmt):
#             print("❌ Blocked a real insert.")
#             return MagicMock()
#         return original_execute(stmt, *args, **kwargs)

#     real_session.execute = MagicMock(side_effect=side_effect_execute)
#     real_session.commit = MagicMock()
#     real_session.rollback = MagicMock()

#     return real_session


# def test_fill_price_gaps_with_real_api_and_mocked_insert(mock_session):
#     filler = FillQuoteUSDPrices(session=mock_session, symbol="ETH", days_back=2)

#     # Confirm that it tried to insert, but we blocked it
#     assert isinstance(filler.missing_price_intervals, list)
#     assert mock_session.execute.called
#     assert mock_session.commit.called

# def test_detects_missing_intervals_and_includes_today(mock_session):
#     filler = FillQuoteUSDPrices(session=mock_session, symbol="ETH", days_back=1)

#     gaps = filler._detect_missing_price_intervals()

#     assert isinstance(gaps, list), "Should return a list of gaps"
#     assert gaps, "Should have at least one missing gap (including today)"

#     # Pretty print the gap range as human time
#     for start_ms, end_ms in gaps:
#         start_dt = datetime.utcfromtimestamp(start_ms / 1000)
#         end_dt = datetime.utcfromtimestamp(end_ms / 1000)
#         print(f"🔍 Missing bucket: {start_dt} to {end_dt}")

#     # Extra: check that one of the buckets is today (rounded down to nearest 8h)
#     now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
#     today_bucket = now - timedelta(hours=now.hour % 8)
#     found = any(today_bucket <= datetime.utcfromtimestamp(end / 1000) for _, end in gaps)

#     assert found, f"Expected a missing bucket including today ({today_bucket})"

@pytest.mark.asyncio
async def test_fill_price_backfill_real_api_without_db_write():
    # Setup DB connection but mock all write methods
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    captured_rows = []

    def fake_execute(stmt, *args, **kwargs):
        # Only intercept INSERTs
        if getattr(stmt, "__visit_name__", "") == "insert":
            compiled = stmt.compile()
            captured_rows.append(compiled.params)
        return MagicMock()

    session.execute = MagicMock(side_effect=fake_execute)
    session.commit = MagicMock()
    session.rollback = MagicMock()

    # Run the full class on a real API call, but DB insert is mocked
    filler = FillQuoteUSDPrices(session=session, symbol="ETH", days_back=1)
    await filler.fill_missing_prices(interval="8h")

    # ✅ Ensure the DB insert was *attempted*
    assert session.execute.called
    assert session.commit.called

    # ✅ Validate at least one row was built from real API data
    assert captured_rows, "No rows were captured from insert"
    print(f"Captured {captured_rows} rows from mocked insert")
    sample = captured_rows[0]
    # assert "bucket_start" in sample
    assert "eth" in sample
    assert isinstance(sample["eth"], Decimal)
    # assert sample["eth"] > 0
    # assert isinstance(sample["bucket_start"], datetime)

    print(f"\n✅ Sample captured insert row: {sample}")