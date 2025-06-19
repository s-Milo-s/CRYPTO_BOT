#!/bin/bash

set -e  # Exit on any error

echo "🔧 Creating schema..."
docker-compose exec db psql -U user -d arbitrage -f /app/sql/schema.sql

echo "🌱 Seeding token pairs..."
docker-compose exec db psql -U user -d arbitrage -f /app/sql/seed_token_pairs.sql

echo "✅ DB initialization complete!"