"""
Companies House Streaming Worker - Render Deployment
Runs 24/7 streaming newly incorporated UK companies to Supabase
"""

import psycopg
import requests
import json
import time
import os
import logging
from datetime import date, datetime
from typing import Optional
from contextlib import contextmanager

# ============================================================================
# CONFIGURATION
# ============================================================================

TARGET_SIC_CODES = {"62012", "63110", "64209", "64301", "64999", "72110"}

TARGET_NAME_KEYWORDS = [
    "labs", "global", "holdings", "capital", "ai", "technology", 
    "technologies", "uk", "london", "europe", "inc", "pty", "pvt", "group"
]

RESTRICTED_SIC_CODES = {
    "10110", "10130", "10110", "10120", "10130", "10131", "10132",
    # Add your full restricted list here
}

# Environment variables (set in Render dashboard)
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CLASSIFICATION LOGIC
# ============================================================================

def matches_buzzword(company_name: str) -> bool:
    """Check if company name contains target buzzwords."""
    name_lower = company_name.lower()
    return any(keyword in name_lower for keyword in TARGET_NAME_KEYWORDS)

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """
    Classify company based on SIC codes and name.
    Returns: 'target_sic', 'buzzword', 'restricted_sic', or None
    """
    # Check target SIC codes
    if any(sic in TARGET_SIC_CODES for sic in sic_codes):
        return "target_sic"
    
    # Check buzzwords
    if matches_buzzword(company_name):
        return "buzzword"
    
    # Check restricted SIC codes
    if any(sic in RESTRICTED_SIC_CODES for sic in sic_codes):
        return "restricted_sic"
    
    return None

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

@contextmanager
def get_db_cursor():
    """Database connection context manager."""
    conn = psycopg.connect(DATABASE_URL)
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    finally:
        conn.close()

def get_checkpoint() -> int:
    """Get last processed timepoint from database."""
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT timepoint FROM stream_state WHERE id = 1")
            result = cur.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error getting checkpoint: {e}")
        return 0

def save_checkpoint(timepoint: int):
    """Save checkpoint to database."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO stream_state (id, timepoint, updated_at)
                VALUES (1, %s, NOW())
                ON CONFLICT (id) DO UPDATE
                SET timepoint = %s, updated_at = NOW()
            """, (timepoint, timepoint))
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

def update_worker_status(status: str, last_error: Optional[str] = None):
    """Update worker status in database."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO worker_status (id, status, last_connected_at, last_event_at, last_error, updated_at)
                VALUES (1, %s, NOW(), %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE
                SET status = %s, last_event_at = %s, last_error = %s, updated_at = NOW()
            """, (status, datetime.now(), last_error, status, datetime.now(), last_error))
    except Exception as e:
        logger.error(f"Error updating worker status: {e}")

def insert_company(company_data: dict, source_type: str):
    """Insert or update company in database."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO screened_companies (
                    company_number, company_name, incorporation_date, company_status,
                    sic_codes, company_url, screened_at, published_at, received_at,
                    source_type, review_status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, NOW(), %s, NOW(), %s, 'approved'
                )
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    company_status = EXCLUDED.company_status,
                    sic_codes = EXCLUDED.sic_codes,
                    screened_at = NOW(),
                    published_at = EXCLUDED.published_at,
                    received_at = NOW(),
                    source_type = EXCLUDED.source_type
                WHERE screened_companies.incorporation_date = %s
            """, (
                company_data["company_number"],
                company_data["company_name"],
                company_data["incorporation_date"],
                company_data.get("company_status", "active"),
                json.dumps(company_data["sic_codes"]),
                f"https://find-and-update.company-information.service.gov.uk/company/{company_data['company_number']}",
                company_data["published_at"],
                source_type,
                company_data["incorporation_date"]
            ))
            logger.info(f"Inserted {company_data['company_number']} as {source_type}")
    except Exception as e:
        logger.error(f"Error inserting company: {e}")

# ============================================================================
# MAIN WORKER LOOP
# ============================================================================

def process_event(event: dict):
    """Process a single streaming event."""
    try:
        company_number = event.get("company_number")
        if not company_number:
            return
        
        company_name = event.get("company_name", "")
        date_of_creation = event.get("date_of_creation")
        sic_codes = event.get("sic_codes", [])
        published_at = event.get("published_at")
        
        # Only process companies incorporated today
        if date_of_creation != date.today().isoformat():
            return
        
        # Classify the company
        source_type = classify_company(sic_codes, company_name)
        if not source_type:
            return
        
        # Insert into database
        company_data = {
            "company_number": company_number,
            "company_name": company_name,
            "incorporation_date": date_of_creation,
            "company_status": event.get("company_status", "active"),
            "sic_codes": sic_codes,
            "published_at": published_at
        }
        
        insert_company(company_data, source_type)
        update_worker_status("connected")
        
        # Save checkpoint
        timepoint = event.get("timepoint")
        if timepoint:
            save_checkpoint(timepoint)
            
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        update_worker_status("error", str(e))

def run_worker():
    """Main worker loop - runs indefinitely."""
    logger.info("Starting Companies House worker...")
    update_worker_status("starting")
    
    while True:
        try:
            # Get checkpoint
            checkpoint = get_checkpoint()
            update_worker_status("connecting")
            
            # Connect to stream
            url = "https://stream.companieshouse.gov.uk/companies"
            headers = {"Authorization": API_KEY}
            params = {"timepoint": checkpoint} if checkpoint else {}
            
            logger.info(f"Connecting to Companies House stream (checkpoint: {checkpoint})")
            
            with requests.get(url, headers=headers, params=params, stream=True, timeout=30) as response:
                response.raise_for_status()
                update_worker_status("connected")
                logger.info("Connected to Companies House stream")
                
                for line in response.iter_lines():
                    try:
                        event = json.loads(line)
                        process_event(event)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                        continue
                        
        except Exception as e:
            error_msg = f"Stream error: {e}"
            logger.error(error_msg)
            update_worker_status("error", error_msg)
            logger.info("Reconnecting in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        exit(1)
    
    if not API_KEY:
        logger.error("COMPANIES_HOUSE_STREAMING_API_KEY environment variable not set")
        exit(1)
    
    run_worker()
