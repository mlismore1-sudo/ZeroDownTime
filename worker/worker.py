"""
Companies House Streaming Worker - Render Deployment
DEBUG VERSION - Comprehensive logging to identify issues
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

# Logging - Set to DEBUG for detailed output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CLASSIFICATION LOGIC
# ============================================================================

def matches_buzzword(company_name: str) -> bool:
    """Check if company name contains target buzzwords."""
    if not company_name:
        return False
    name_lower = company_name.lower()
    return any(keyword in name_lower for keyword in TARGET_NAME_KEYWORDS)

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """
    Classify company based on SIC codes and name.
    Returns: 'target_sic', 'buzzword', 'restricted_sic', or None
    """
    logger.debug(f"Classifying: SIC={sic_codes}, Name={company_name}")
    
    # Check target SIC codes
    if sic_codes and any(str(sic) in TARGET_SIC_CODES for sic in sic_codes):
        logger.debug(f"Matched target SIC")
        return "target_sic"
    
    # Check buzzwords
    if matches_buzzword(company_name):
        logger.debug(f"Matched buzzword")
        return "buzzword"
    
    # Check restricted SIC codes
    if sic_codes and any(str(sic) in RESTRICTED_SIC_CODES for sic in sic_codes):
        logger.debug(f"Matched restricted SIC")
        return "restricted_sic"
    
    logger.debug(f"No match found")
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
            checkpoint = result[0] if result else 0
            logger.info(f"Got checkpoint: {checkpoint}")
            return checkpoint
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
            logger.debug(f"Saved checkpoint: {timepoint}")
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
            logger.debug(f"Updated worker status: {status}")
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
            logger.info(f"✅ INSERTED {company_data['company_number']} as {source_type}")
    except Exception as e:
        logger.error(f"❌ Error inserting company: {e}")

# ============================================================================
# EVENT PROCESSING
# ============================================================================

def process_event(event: dict):
    """Process a single streaming event with comprehensive logging."""
    try:
        logger.debug(f"📥 Event received: {json.dumps(event, default=str)[:300]}")
        
        # Extract company number
        company_number = event.get("company_number")
        if not company_number:
            logger.warning(f"⚠️ No company_number in event: {event}")
            return
        
        logger.info(f"🏢 Processing company: {company_number}")
        
        # Extract fields
        company_name = event.get("company_name", "")
        date_of_creation = event.get("date_of_creation")
        sic_codes = event.get("sic_codes", [])
        published_at = event.get("published_at")
        company_status = event.get("company_status", "active")
        
        logger.info(f"  Name: {company_name}")
        logger.info(f"  Date of creation: {date_of_creation}")
        logger.info(f"  SIC codes: {sic_codes}")
        logger.info(f"  Status: {company_status}")
        
        # Check if incorporated today
        today = date.today().isoformat()
        logger.info(f"  Today's date: {today}")
        
        if not date_of_creation:
            logger.warning(f"⚠️ No date_of_creation, skipping")
            return
        
        if date_of_creation != today:
            logger.info(f"⏭️ Skipping - not incorporated today (date: {date_of_creation})")
            return
        
        logger.info(f"✅ Date match - incorporated today")
        
        # Ensure sic_codes is a list
        if isinstance(sic_codes, str):
            try:
                sic_codes = json.loads(sic_codes)
            except:
                sic_codes = [sic_codes]
        
        if not sic_codes:
            logger.warning(f"⚠️ No SIC codes, skipping")
            return
        
        # Classify the company
        logger.info(f"🔍 Classifying company...")
        source_type = classify_company(sic_codes, company_name)
        
        if not source_type:
            logger.info(f"⏭️ Skipping - no classification match")
            logger.info(f"  SIC codes: {sic_codes}")
            logger.info(f"  Target SICs: {TARGET_SIC_CODES}")
            logger.info(f"  Name keywords: {TARGET_NAME_KEYWORDS}")
            return
        
        logger.info(f"✅ Classified as: {source_type}")
        
        # Insert into database
        company_data = {
            "company_number": company_number,
            "company_name": company_name,
            "incorporation_date": date_of_creation,
            "company_status": company_status,
            "sic_codes": sic_codes,
            "published_at": published_at
        }
        
        logger.info(f"💾 Inserting into database...")
        insert_company(company_data, source_type)
        
        # Update status
        update_worker_status("connected")
        
        # Save checkpoint
        timepoint = event.get("timepoint")
        if timepoint:
            save_checkpoint(timepoint)
            logger.debug(f"💾 Checkpoint saved: {timepoint}")
            
    except Exception as e:
        logger.error(f"❌ ERROR in process_event: {e}", exc_info=True)
        update_worker_status("error", str(e))

# ============================================================================
# MAIN WORKER LOOP
# ============================================================================

def run_worker():
    """Main worker loop - runs indefinitely."""
    logger.info("🚀 Starting Companies House worker...")
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
            
            logger.info(f"🔗 Connecting to Companies House stream (checkpoint: {checkpoint})")
            
            with requests.get(url, headers=headers, params=params, stream=True, timeout=30) as response:
                response.raise_for_status()
                update_worker_status("connected")
                logger.info("✅ Connected to Companies House stream")
                
                event_count = 0
                for line in response.iter_lines():
                    try:
                        event = json.loads(line)
                        event_count += 1
                        
                        if event_count % 100 == 0:
                            logger.info(f"📊 Processed {event_count} events")
                        
                        process_event(event)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error: {e}")
                        logger.debug(f"Raw line: {line[:200]}")
                        continue
                        
        except Exception as e:
            error_msg = f"Stream error: {e}"
            logger.error(f"❌ {error_msg}")
            update_worker_status("error", error_msg)
            logger.info("⏳ Reconnecting in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable not set")
        exit(1)
    
    if not API_KEY:
        logger.error("❌ COMPANIES_HOUSE_STREAMING_API_KEY environment variable not set")
        exit(1)
    
    logger.info("✅ All environment variables present")
    run_worker()
