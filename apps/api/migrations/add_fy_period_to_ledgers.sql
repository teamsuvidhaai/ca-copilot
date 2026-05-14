-- Migration: Add fy_period column to ledgers table for per-FY balance storage
-- Run this in Supabase SQL Editor

-- 1. Add the column
ALTER TABLE ledgers ADD COLUMN IF NOT EXISTS fy_period TEXT;

-- 2. Drop old unique constraint
ALTER TABLE ledgers DROP CONSTRAINT IF EXISTS uq_ledgers_company_name;

-- 3. Add new unique constraint including fy_period
ALTER TABLE ledgers ADD CONSTRAINT uq_ledgers_company_name_fy 
  UNIQUE (company_name, name, fy_period);

-- 4. Add index for FY filtering
CREATE INDEX IF NOT EXISTS idx_ledgers_fy ON ledgers (company_name, fy_period);

-- 5. Backfill existing rows with NULL fy_period (they represent the "full period" sync)
-- No action needed — NULL is the default
