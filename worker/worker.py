"""
Companies House Real-Time Worker - OPTIMIZED
<10 second delay from event to dashboard
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
from concurrent.futures import ThreadPoolExecutor

# Configuration
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "30"))
PORT = int(os.getenv("PORT", 8000))

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Target SIC Codes
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

# Global state
db_conn = None
last_event_id = None
shutdown_flag = False
executor = ThreadPoolExecutor(max_workers=5)  # Async DB inserts

def get_db_connection():
    """Get or create database connection."""
    global db_conn
    if db_conn is None:
        db_conn = psycopg.connect(DATABASE_URL)
    return db_conn

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """Classify company based on SIC codes only (optimized)."""
    # Check target SIC codes
    if sic_codes:
        for sic in sic_codes:
            if str(sic) in TARGET_SIC_CODES:
                return "target_sic"
    
    return None

@backoff.on_exception(
    backoff.constant,
    Exception,
    max_tries=MAX_RETRIES,
    interval=RETRY_DELAY,
    logger=logger
)
def fetch_events(sse_client, last_id=None):
    """Fetch events from SSE stream."""
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
    """Process a single event and return company data if it matches."""
    try:
        company_data = event_data.get('data', {})
        if not company_data or 'company_number' not in company_data:
            return None
        
        company_number = company_data['company_number']
        company_name = company_data.get('company_name', '')
        incorporation_date = company_data.get('incorporation_date')
        sic_codes = company_data.get('sic_codes', [])
        
        if not incorporation_date:
            return None
        
        # Get today's date (UTC)
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Check if incorporated today
        if incorporation_date != today:
            return None
        
        # Classify the company
        source_type = classify_company(sic_codes, company_name)
        
        if not source_type:
            return None
        
        logger.info(f"✓ Match: {company_number} - {source_type}")
        
        return {
            "company_number": company_number,
            "company_name": company_name,
            "incorporation_date": today,
            "sic_codes": sic_codes,
            "source_type": source_type,
            "published_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return None

def insert_company_sync(company_data: dict, source_type: str):
    """Insert company into database (sync, for thread pool)."""
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
        logger.info(f"✓ Inserted {company_data['company_number']}")
    except Exception as e:
        logger.error(f"Error inserting: {e}")
        conn.rollback()

async def insert_company_async(company_data: dict, source_type: str):
    """Insert company into database asynchronously."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, insert_company_sync, company_data, source_type)

async def broadcast_company(company_data: dict, clients: Set):
    """Broadcast new company to all connected WebSocket clients."""
    if not clients:
        return
    
    message = {
        "type": "company",
        "company": company_data
    }
    
    disconnected = set()
    for client in clients:
        try:
            await client.send_json(message)
        except Exception as e:
            logger.error(f"Error sending to client: {e}")
            disconnected.add(client)
    
    for client in disconnected:
        clients.remove(client)

def save_checkpoint():
    """Save last event ID to file."""
    try:
        with open('/tmp/last_event_id.txt', 'w') as f:
            f.write(last_event_id or '')
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

async def process_event_async(event_data: dict, clients: Set):
    """Process event asynchronously with non-blocking DB insert."""
    try:
        # Process the event (fast, synchronous)
        company_data = process_event(event_data)
        
        if company_data:
            # Insert to database asynchronously (non-blocking)
            asyncio.create_task(insert_company_async(company_data, company_data['source_type']))
            
            # Broadcast immediately (don't wait for DB)
            await broadcast_company(company_data, clients)
            
    except Exception as e:
        logger.error(f"Error processing event: {e}")

def main():
    """Main worker loop - OPTIMIZED."""
    global last_event_id
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("=" * 60)
    logger.info("Starting Companies House Worker - OPTIMIZED")
    logger.info("Target: <10 second delay")
    logger.info("=" * 60)
    logger.info(f"Target SIC codes: {len(TARGET_SIC_CODES)}")
    logger.info(f"SSE URL: {SSE_URL}")
    
    last_event_id = load_checkpoint()
    if last_event_id:
        logger.info(f"Resuming from event ID: {last_event_id}")
    
    # Create event loop for async operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Shared clients set (will be populated by dashboard)
    clients = set()
    
    with httpx.Client() as sse_client:
        event_count = 0
        
        while not shutdown_flag:
            try:
                for event_data in fetch_events(sse_client, last_event_id):
                    if shutdown_flag:
                        break
                    
                    event_count += 1
                    
                    # Process event immediately (no batching)
                    loop.run_until_complete(process_event_async(event_data, clients))
                
                # Save checkpoint
                if last_event_id:
                    save_checkpoint()
                    
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                if not shutdown_flag:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
    
    # Cleanup
    loop.close()
    executor.shutdown(wait=True)
    
    logger.info("Worker shutdown complete")

if __name__ == "__main__":
    import time
    main()
