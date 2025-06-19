#!/bin/bash

set -e

echo "⚠️ Dropping existing tables..."
docker-compose exec db psql -U user -d arbitrage -f /app/sql/reset.sql

echo "🔁 Reinitializing schema and seed data..."
./db_setup/scripts/init_db.sh