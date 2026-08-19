"""
Companies House Streaming Worker - Render Deployment
OPTIMIZED VERSION - Better logging, faster processing
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

# Logging - INFO level for production
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
    if not company_name:
        return False
    name_lower = company_name.lower()
    return any(keyword in name_lower for keyword in TARGET_NAME_KEYWORDS)

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """
    Classify company based on SIC codes and name.
    Returns: 'target_sic', 'buzzword', 'restricted_sic', or None
    """
    # Check target SIC codes
    if sic_codes and any(str(sic) in TARGET_SIC_CODES for sic in sic_codes):
        return "target_sic"
    
    # Check buzzwords
    if matches_buzzword(company_name):
        return "buzzword"
    
    # Check restricted SIC codes
    if sic_codes and any(str(sic) in RESTRICTED_SIC_CODES for sic in sic_codes):
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
    """Insert or update company in database with timing info."""
    try:
        start_time = time.time()
        
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
            
        insert_time = time.time() - start_time
        logger.info(f"✅ INSERTED {company_data['company_number']} as {source_type} (DB: {insert_time:.2f}s)")
        
    except Exception as e:
        logger.error(f"❌ Error inserting company: {e}")

# ============================================================================
# EVENT PROCESSING - OPTIMIZED
# ============================================================================

def process_event(event: dict):
    """Process a single streaming event with timing."""
    try:
        start_time = time.time()
        
        # Check if it's a company-profile event
        if event.get("resource_kind") != "company-profile":
            return
        
        # Extract data from nested structure
        data = event.get("data", {})
        if not data:
            return
        
        # Extract company number
        company_number = data.get("company_number")
        if not company_number:
            return
        
        # Extract fields from data
        company_name = data.get("company_name", "")
        date_of_creation = data.get("date_of_creation")
        sic_codes = data.get("sic_codes", [])
        company_status = data.get("company_status", "active")
        
        # Extract event metadata
        event_info = event.get("event", {})
        published_at = event_info.get("published_at")
        timepoint = event_info.get("timepoint")
        
        # Only process companies incorporated today
        today = date.today().isoformat()
        
        if not date_of_creation:
            return
        
        if date_of_creation != today:
            return
        
        # Ensure sic_codes is a list
        if isinstance(sic_codes, str):
            try:
                sic_codes = json.loads(sic_codes)
            except:
                sic_codes = [sic_codes]
        
        if not sic_codes:
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
            "company_status": company_status,
            "sic_codes": sic_codes,
            "published_at": published_at
        }
        
        insert_company(company_data, source_type)
        
        # Calculate total processing time
        total_time = time.time() - start_time
        logger.info(f"⏱️ Total processing time: {total_time:.3f}s")
        
        # Update status
        update_worker_status("connected")
        
        # Save checkpoint
        if timepoint:
            save_checkpoint(timepoint)
            
    except Exception as e:
        logger.error(f"❌ Error processing event: {e}")
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
                companies_found = 0
                
                for line in response.iter_lines():
                    try:
                        event = json.loads(line)
                        event_count += 1
                        
                        # Log progress every 100 events
                        if event_count % 100 == 0:
                            logger.info(f"📊 Processed {event_count} events, found {companies_found} companies")
                        
                        # Check if this event resulted in a company being inserted
                        if process_event_with_count(event):
                            companies_found += 1
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error: {e}")
                        continue
                        
        except Exception as e:
            error_msg = f"Stream error: {e}"
            logger.error(f"❌ {error_msg}")
            update_worker_status("error", error_msg)
            logger.info("⏳ Reconnecting in 10 seconds...")
            time.sleep(10)

def process_event_with_count(event: dict) -> bool:
    """Process event and return True if company was inserted."""
    try:
        # Check if it's a company-profile event
        if event.get("resource_kind") != "company-profile":
            return False
        
        # Extract data from nested structure
        data = event.get("data", {})
        if not data:
            return False
        
        # Extract company number
        company_number = data.get("company_number")
        if not company_number:
            return False
        
        # Extract fields from data
        company_name = data.get("company_name", "")
        date_of_creation = data.get("date_of_creation")
        sic_codes = data.get("sic_codes", [])
        company_status = data.get("company_status", "active")
        
        # Extract event metadata
        event_info = event.get("event", {})
        published_at = event_info.get("published_at")
        timepoint = event_info.get("timepoint")
        
        # Only process companies incorporated today
        today = date.today().isoformat()
        
        if not date_of_creation:
            return False
        
        if date_of_creation != today:
            return False
        
        # Ensure sic_codes is a list
        if isinstance(sic_codes, str):
            try:
                sic_codes = json.loads(sic_codes)
            except:
                sic_codes = [sic_codes]
        
        if not sic_codes:
            return False
        
        # Classify the company
        source_type = classify_company(sic_codes, company_name)
        if not source_type:
            return False
        
        # Log timing info
        if published_at:
            try:
                published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                delay = datetime.now(published.tzinfo) - published
                logger.info(f"⏱️ Delay from CH publication: {delay.total_seconds():.1f}s")
            except:
                pass
        
        # Insert into database
        company_data = {
            "company_number": company_number,
            "company_name": company_name,
            "incorporation_date": date_of_creation,
            "company_status": company_status,
            "sic_codes": sic_codes,
            "published_at": published_at
        }
        
        insert_company(company_data, source_type)
        
        # Update status
        update_worker_status("connected")
        
        # Save checkpoint
        if timepoint:
            save_checkpoint(timepoint)
        
        return True
            
    except Exception as e:
        logger.error(f"❌ Error processing event: {e}")
        update_worker_status("error", str(e))
        return False

if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable not set")
        exit(1)
    
    if not API_KEY:
        logger.error("❌ COMPANIES_HOUSE_STREAMING_API_KEY environment variable not set")
        exit(1)
    
    logger.info("✅ All environment variables present")
    run_worker()
