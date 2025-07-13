from sqlalchemy import Table, Column, Integer, Numeric, Text, TIMESTAMP, MetaData, func

# Define the table schema (can be placed in a separate models file)
metadata = MetaData()

extraction_metrics_table = Table(
    "extraction_metrics",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", TIMESTAMP(timezone=True), server_default=func.now()),
    Column("block_range", Text),
    Column("log_count", Integer, nullable=False),
    Column("duration_seconds", Numeric(10, 2))
)
