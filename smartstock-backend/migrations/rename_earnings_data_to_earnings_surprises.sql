-- Migration: Rename earnings_data table to earnings_surprises
-- Date: 2025-01-XX
-- Description: Rename table to better reflect its purpose (earnings surprises)

BEGIN;

-- Rename the table
ALTER TABLE earnings_data RENAME TO earnings_surprises;

-- Rename indexes
ALTER INDEX idx_earnings_data_ticker RENAME TO idx_earnings_surprises_ticker;
ALTER INDEX idx_earnings_data_date RENAME TO idx_earnings_surprises_date;

-- Update any foreign key constraints (if any exist)
-- Note: Check if there are any foreign keys referencing this table first

COMMIT;

-- Verification queries (run after migration):
-- SELECT COUNT(*) FROM earnings_surprises;
-- SELECT * FROM earnings_surprises LIMIT 5;

