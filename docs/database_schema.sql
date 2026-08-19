-- Companies House Screener - Database Schema
-- Run this in Supabase SQL Editor

-- ============================================================================
-- MAIN TABLES
-- ============================================================================

-- Main companies table
CREATE TABLE IF NOT EXISTS screened_companies (
    company_number TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    incorporation_date DATE NOT NULL,
    company_status TEXT DEFAULT 'active',
    sic_codes JSONB,
    company_url TEXT,
    screened_at TIMESTAMPTZ DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    source_type TEXT NOT NULL,
    review_status TEXT DEFAULT 'approved'
);

-- Stream checkpoint (tracks position in Companies House stream)
CREATE TABLE IF NOT EXISTS stream_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    timepoint BIGINT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Worker status (for monitoring)
CREATE TABLE IF NOT EXISTS worker_status (
    id INTEGER PRIMARY KEY DEFAULT 1,
    status TEXT,
    last_connected_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- User shortlists (Brad & James)
CREATE TABLE IF NOT EXISTS user_shortlists (
    company_number TEXT REFERENCES screened_companies(company_number) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    shortlisted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (company_number, user_name)
);

-- ============================================================================
-- INDEXES (for performance)
-- ============================================================================

-- Index for fast filtering by date and source type
CREATE INDEX IF NOT EXISTS idx_screened_companies_date_source 
ON screened_companies(incorporation_date, source_type);

-- Index for deduplication checks
CREATE INDEX IF NOT EXISTS idx_screened_companies_number 
ON screened_companies(company_number);

-- Index for user shortlist queries
CREATE INDEX IF NOT EXISTS idx_user_shortlists_user 
ON user_shortlists(user_name, shortlisted_at DESC);

-- Index for ordering by publication time
CREATE INDEX IF NOT EXISTS idx_screened_companies_published 
ON screened_companies(published_at DESC);

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert initial stream state (starting from timepoint 0)
INSERT INTO stream_state (id, timepoint) 
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check tables were created
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';

-- Check indexes were created
-- SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public';

-- View today's companies
-- SELECT company_number, company_name, source_type, published_at
-- FROM screened_companies
-- WHERE incorporation_date = CURRENT_DATE
-- ORDER BY published_at DESC;

-- Check worker status
-- SELECT status, last_event_at, last_error FROM worker_status WHERE id = 1;
