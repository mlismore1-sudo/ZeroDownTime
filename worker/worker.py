"""
Companies House Real-Time Worker - FIXED DATE
Always uses current date (not hardcoded)
"""

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Set, Optional
import logging
import httpx
import psycopg
from psycopg import sql
import backoff

# Configuration
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "30"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
PORT = int(os.getenv("PORT", 8000))

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Target SIC Codes (all as strings for consistent comparison)
TARGET_SIC_CODES = {
    "62011", "62012", "62020", "62030", "62090",
    "63110", "63120", "63910", "63990",
    "64999", "66190", "66220", "66300",
    "70229", "72110", "72190", "72200",
    "73110", "73120", "73200",
    "74100", "74200", "74300", "74900",
    "82990", "85590", "86900", "87900",
    "90030", "91010", "91020", "93290"
}

# Buzzword pattern - " AI" with space to avoid false positives
BUZZWORD_PATTERNS = [" AI"]

# Global state
db_conn = None
last_event_id = None
shutdown_flag = False
connected_clients = set()

def get_db_connection():
    """Get or create database connection."""
    global db_conn
    if db_conn is None:
        db_conn = psycopg.connect(DATABASE_URL)
    return db_conn

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """
    Classify company based on SIC codes and name.
    Returns: 'target_sic', 'buzzword', or None
    """
    # Check target SIC codes FIRST
    if sic_codes:
        for sic in sic_codes:
            if str(sic) in TARGET_SIC_CODES:
                return "target_sic"
    
    # Check buzzwords SECOND - " AI" with space
    if matches_buzzword(company_name):
        return "buzzword"
    
    # No match - don't insert
    return None

def matches_buzzword(company_name: str) -> bool:
    """Check if company name contains buzzword patterns."""
    if not company_name:
        return False
    
    # Check for " AI" (space before AI)
    for pattern in BUZZWORD_PATTERNS:
        if pattern in company_name:
            return True
    
    return False

@backoff.on_exception(
    backoff.constant,
    Exception,
    max_tries=MAX_RETRIES,
    interval=RETRY_DELAY,
    logger=logger
)
def fetch_events(sse_client, last_id=None):
    """Fetch events from SSE stream with retry logic."""
    headers = {
        "Authorization": f"Basic {API_KEY}",
        "Accept": "application/json"
    }
    
    if last_id:
        headers["Last-Event-ID"] = last_id
    
    with httpx.stream("GET", SSE_URL, headers=headers, timeout=30.0) as response:
        response.raise_for_status()
        
        for line in response.iter_lines():
            if shutdown_flag:
                return []
            
            if not line:
                continue
            
            if line.startswith("event:"):
                continue
            
            if line.startswith("id:"):
                last_event_id = line[3:].strip()
                continue
            
            if line.startswith("data:"):
                try:
                    data = json.loads(line[5:])
                    yield data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    continue

def process_event(event_data: dict) -> Optional[dict]:
    """
    Process a single event and return company data if it matches.
    Only returns companies with target SIC codes or buzzword " AI".
    """
    try:
        # Extract company data
        company_data = event_data.get('data', {})
        if not company_data or 'company_number' not in company_data:
            return None
        
        company_number = company_data['company_number']
        company_name = company_data.get('company_name', '')
        incorporation_date = company_data.get('incorporation_date')
        sic_codes = company_data.get('sic_codes', [])
        
        # Skip if no incorporation date
        if not incorporation_date:
            logger.info(f"Skipping {company_number} - no incorporation date")
            return None
        
        # Get today's date dynamically (UTC)
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Check if incorporated today
        if incorporation_date != today:
            logger.info(f"Skipping {company_number} - not incorporated today ({incorporation_date})")
            return None
        
        # Classify the company
        source_type = classify_company(sic_codes, company_name)
        
        if not source_type:
            logger.info(f"Skipping {company_number} - no match (SIC: {sic_codes}, Name: {company_name})")
            return None
        
        logger.info(f"✓ Match found: {company_number} - {source_type}")
        
        # Return company data with current timestamp
        return {
            "company_number": company_number,
            "company_name": company_name,
            "incorporation_date": today,  # Use today's date
            "sic_codes": sic_codes,
            "source_type": source_type,
            "published_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return None

def insert_company(company_data: dict, source_type: str):
    """Insert company into database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO screened_companies 
                (company_number, company_name, incorporation_date, sic_codes, source_type, published_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_number) DO NOTHING
            """, (
                company_data['company_number'],
                company_data['company_name'],
                company_data['incorporation_date'],
                company_data['sic_codes'],
                source_type,
                company_data['published_at']
            ))
            conn.commit()
        logger.info(f"Inserted {company_data['company_number']} - {source_type}")
    except Exception as e:
        logger.error(f"Error inserting company: {e}")
        conn.rollback()

async def broadcast_company(company_data: dict):
    """Broadcast new company to all connected WebSocket clients."""
    if not connected_clients:
        return
    
    message = {
        "type": "company",
        "company": company_data
    }
    
    disconnected = set()
    for client in connected_clients:
        try:
            await client.send_json(message)
        except Exception as e:
            logger.error(f"Error sending to client: {e}")
            disconnected.add(client)
    
    for client in disconnected:
        connected_clients.remove(client)
    
    logger.info(f"Broadcasted {company_data['company_number']} to {len(connected_clients)} clients")

def process_batch(batch: list):
    """Process a batch of events."""
    logger.info(f"Processing batch of {len(batch)} events")
    
    for event_data in batch:
        if shutdown_flag:
            break
        
        try:
            # Process the event
            company_data = process_event(event_data)
            
            if company_data:
                # Insert into database
                insert_company(company_data, company_data['source_type'])
                
                # Broadcast to dashboard
                asyncio.run(broadcast_company(company_data))
                
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            continue

def save_checkpoint():
    """Save last event ID to file."""
    try:
        with open('/tmp/last_event_id.txt', 'w') as f:
            f.write(last_event_id or '')
        logger.info(f"Saved checkpoint: {last_event_id}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

def load_checkpoint():
    """Load last event ID from file."""
    try:
        with open('/tmp/last_event_id.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_flag
    logger.info("Shutdown signal received")
    shutdown_flag = True

def main():
    """Main worker loop."""
    global last_event_id
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Starting Companies House Worker")
    logger.info(f"Target SIC codes: {len(TARGET_SIC_CODES)}")
    logger.info(f"Buzzword patterns: {BUZZWORD_PATTERNS}")
    
    # Load checkpoint
    last_event_id = load_checkpoint()
    if last_event_id:
        logger.info(f"Resuming from event ID: {last_event_id}")
    
    # Initialize SSE client
    with httpx.Client() as sse_client:
        batch = []
        
        while not shutdown_flag:
            try:
                # Fetch events from SSE stream
                for event_data in fetch_events(sse_client, last_event_id):
                    if shutdown_flag:
                        break
                    
                    batch.append(event_data)
                    
                    # Process batch when it reaches the size limit
                    if len(batch) >= BATCH_SIZE:
                        process_batch(batch)
                        batch = []
                
                # Save checkpoint periodically
                if last_event_id:
                    save_checkpoint()
                    
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                if not shutdown_flag:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    asyncio.sleep(RETRY_DELAY)
    
    # Process remaining events
    if batch:
        logger.info(f"Processing remaining {len(batch)} events")
        process_batch(batch)
    
    logger.info("Worker shutdown complete")

if __name__ == "__main__":
    main()
