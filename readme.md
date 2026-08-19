Companies House Screener
Real-time streaming application that monitors newly incorporated UK companies from Companies House and displays them in a live dashboard.

Architecture
text
Companies House Stream API
         ↓
    Render Worker ($7/mo) ← 24/7 streaming
         ↓
    Supabase Database (Free) ← PostgreSQL storage
         ↓
    Streamlit UI (Free) ← Live dashboard
Features
✅ Real-time streaming - Companies appear within seconds of incorporation

✅ SIC code filtering - Target specific industries

✅ Name keyword matching - Catch companies with buzzwords (labs, capital, AI, etc.)

✅ Restricted SIC tracking - Flag companies in excluded industries

✅ Per-user shortlists - Brad and James can save companies

✅ CSV export - Download filtered lists

✅ Live dashboard - Auto-refreshes every 15 seconds

Repository Structure
text
companies-house-screener/
├── worker/                    # Render deployment (24/7 worker)
│   ├── worker.py             # Main streaming script
│   └── requirements.txt      # Worker dependencies
│
├── ui/                        # Streamlit deployment (dashboard)
│   ├── app.py                # Streamlit app
│   ├── requirements.txt      # UI dependencies
│   └── .streamlit/
│       └── secrets.toml      # Secrets (DO NOT COMMIT)
│
├── .gitignore
└── README.md
Quick Start
1. Supabase Setup

Create account: https://supabase.com/signup

Create new project

Get connection string (Session pooler, port 5432)

Run schema SQL (see docs/DATABASE_SCHEMA.md)

2. Deploy Worker to Render

Create account: https://render.com/signup

Create new Web Service

Connect this GitHub repo

Root Directory: worker

Build Command: pip install -r requirements.txt

Start Command: python worker.py

Instance Type: Starter ($7/month for 24/7)

Add environment variables:

DATABASE_URL - Your Supabase connection string

COMPANIES_HOUSE_STREAMING_API_KEY - Companies House streaming key

3. Deploy UI to Streamlit

Create account: https://streamlit.io/cloud

Create new app

Select this repo

File path: ui/app.py

Add secrets in ui/.streamlit/secrets.toml:

text
DATABASE_URL = "postgresql://..."
RESTRICTED_SIC_CODES = "10110,10130,..."
4. Get Companies House API Key

Create account: https://developer.company-information.service.gov.uk/

Create application

Copy streaming API key

Add to Render environment variables

Configuration
Worker Environment Variables (Render)

Variable	Description
DATABASE_URL	Supabase PostgreSQL connection string
COMPANIES_HOUSE_STREAMING_API_KEY	Companies House streaming API key
UI Secrets (Streamlit)

Variable	Description
DATABASE_URL	Supabase PostgreSQL connection string
RESTRICTED_SIC_CODES	Comma-separated list of restricted SIC codes
Target SIC Codes (hardcoded in worker.py)

python
TARGET_SIC_CODES = {"62012", "63110", "64209", "64301", "64999", "72110"}
Name Buzzwords (hardcoded in worker.py)

python
TARGET_NAME_KEYWORDS = [
    "labs", "global", "holdings", "capital", "ai", "technology", 
    "technologies", "uk", "london", "europe", "inc", "pty", "pvt", "group"
]
Database Schema
Run this SQL in Supabase SQL Editor:

sql
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

-- Stream checkpoint
CREATE TABLE IF NOT EXISTS stream_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    timepoint BIGINT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Worker status
CREATE TABLE IF NOT EXISTS worker_status (
    id INTEGER PRIMARY KEY DEFAULT 1,
    status TEXT,
    last_connected_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- User shortlists
CREATE TABLE IF NOT EXISTS user_shortlists (
    company_number TEXT REFERENCES screened_companies(company_number) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    shortlisted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (company_number, user_name)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_screened_companies_date_source 
ON screened_companies(incorporation_date, source_type);

CREATE INDEX IF NOT EXISTS idx_screened_companies_number 
ON screened_companies(company_number);

CREATE INDEX IF NOT EXISTS idx_user_shortlists_user 
ON user_shortlists(user_name, shortlisted_at DESC);

-- Initial checkpoint
INSERT INTO stream_state (id, timepoint) 
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
Monitoring
Check Worker Logs (Render)

Go to Render dashboard

Click your worker service

Click "Logs" tab

Look for:

"Starting Companies House worker..."

"Connected to Companies House stream"

"Inserted XXXXXXXX as target_sic"

Check Database (Supabase)

sql
-- View today's companies
SELECT company_number, company_name, source_type, published_at
FROM screened_companies
WHERE incorporation_date = CURRENT_DATE
ORDER BY published_at DESC;

-- Check worker status
SELECT status, last_event_at, last_error
FROM worker_status
WHERE id = 1;

-- Count by source type
SELECT source_type, COUNT(*)
FROM screened_companies
WHERE incorporation_date = CURRENT_DATE
GROUP BY source_type;
Troubleshooting
Worker not connecting

Check Render logs for errors

Verify DATABASE_URL is correct

Verify COMPANIES_HOUSE_STREAMING_API_KEY is correct

Ensure API key starts with stream_

No companies appearing

Check it's during UK business hours (9am-5pm GMT)

Verify worker logs show "Connected"

Check SIC codes and buzzwords match incoming companies

Verify timepoint is advancing (not stuck at 0)

Database connection errors

Verify connection string format

Ensure password is URL-encoded

Check sslmode=require is included

Verify database tables exist

Cost
Service	Plan	Cost
Render	Starter	$7/month
Supabase	Free	$0
Streamlit	Free	$0
GitHub	Free	$0
Companies House	Free	$0
Total		$7/month
Development
Local Testing

Worker:

bash
cd worker
export DATABASE_URL="postgresql://..."
export COMPANIES_HOUSE_STREAMING_API_KEY="stream_..."
python worker.py
UI:

bash
cd ui
streamlit run app.py
License
Private - All rights reserved

Support
Render Docs: https://docs.render.com/

Supabase Docs: https://supabase.com/docs

Streamlit Docs: https://docs.streamlit.io/

Companies House API: https://developer.company-information.service.gov.uk/docs
