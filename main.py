"""
Companies House Real-Time Monitor - Production Version
Real-time SSE streaming with proper async handling
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import json
import asyncio
import os
import httpx
from datetime import datetime, timezone
from typing import Set, Optional
import logging
import base64

# Configuration
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")
API_KEY = os.getenv("API_KEY")
DATABASE_FILE = os.getenv("DATABASE_FILE", "./companies.db")
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

# FastAPI app
app = FastAPI(title="Companies House Monitor")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket clients
connected_clients: Set[WebSocket] = set()

# Global state
shutdown_flag = False
last_event_id = None
db_lock = asyncio.Lock()

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def init_db():
    """Initialize SQLite database."""
    conn = sqlite3.connect(DATABASE_FILE)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS screened_companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_number TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                incorporation_date TEXT NOT NULL,
                sic_codes TEXT,
                source_type TEXT NOT NULL,
                published_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON screened_companies(incorporation_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_published ON screened_companies(published_at DESC)")
    conn.close()
    logger.info(f"✓ Database initialized: {DATABASE_FILE}")

def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def insert_company_sync(company_data: dict, source_type: str):
    """Insert company into database (sync)."""
    try:
        conn = get_db_connection()
        with conn:
            conn.execute("""
                INSERT OR IGNORE INTO screened_companies 
                (company_number, company_name, incorporation_date, sic_codes, source_type, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                company_data['company_number'],
                company_data['company_name'],
                company_data['incorporation_date'],
                json.dumps(company_data['sic_codes']),
                source_type,
                company_data['published_at']
            ))
        conn.close()
        logger.info(f"✓ Inserted {company_data['company_number']}")
    except Exception as e:
        logger.error(f"Error inserting: {e}")

async def insert_company(company_data: dict, source_type: str):
    """Insert company into database (async wrapper)."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, insert_company_sync, company_data, source_type)

async def get_companies(limit: int = 100):
    """Get today's companies from database."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    conn = get_db_connection()
    cursor = conn.execute("""
        SELECT company_number, company_name, incorporation_date, 
               sic_codes, source_type, published_at
        FROM screened_companies
        WHERE incorporation_date = ?
        ORDER BY published_at DESC
        LIMIT ?
    """, (today, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

async def get_metrics():
    """Get metrics from database."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    conn = get_db_connection()
    
    cursor = conn.execute("""
        SELECT COUNT(*) FROM screened_companies 
        WHERE incorporation_date = ? AND source_type IN ('target_sic', 'buzzword')
    """, (today,))
    target_count = cursor.fetchone()[0]
    
    cursor = conn.execute("""
        SELECT COUNT(*) FROM screened_companies 
        WHERE incorporation_date = ? AND source_type = 'restricted_sic'
    """, (today,))
    restricted_count = cursor.fetchone()[0]
    
    cursor = conn.execute("""
        SELECT COUNT(*) FROM screened_companies 
        WHERE incorporation_date = ?
    """, (today,))
    total_count = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "target_count": target_count,
        "restricted_count": restricted_count,
        "total_count": total_count
    }

# ============================================================================
# SSE STREAM PROCESSING
# ============================================================================

def classify_company(sic_codes: list, company_name: str) -> Optional[str]:
    """Classify company based on SIC codes."""
    if sic_codes:
        for sic in sic_codes:
            if str(sic) in TARGET_SIC_CODES:
                return "target_sic"
    return None

def process_event(event_data: dict) -> Optional[dict]:
    """Process event and return company data if it matches."""
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
        
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        if incorporation_date != today:
            return None
        
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

async def broadcast_to_clients(company_data: dict):
    """Broadcast company to all connected WebSocket clients."""
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

async def process_and_broadcast(company_data: dict):
    """Process company and broadcast to clients."""
    # Insert to database
    await insert_company(company_data, company_data['source_type'])
    
    # Broadcast to clients
    await broadcast_to_clients(company_data)

async def sse_event_generator(client: httpx.AsyncClient, headers: dict):
    """Generate SSE events from the stream."""
    async with client.stream("GET", SSE_URL, headers=headers, timeout=60.0) as response:
        response.raise_for_status()
        logger.info("✓ Connected to SSE stream")
        
        async for line in response.aiter_lines():
            if shutdown_flag:
                break
            
            if not line:
                continue
            
            if line.startswith("id:"):
                global last_event_id
                last_event_id = line[3:].strip()
                continue
            
            if line.startswith("data:"):
                try:
                    event_data = json.loads(line[5:])
                    yield event_data
                except json.JSONDecodeError as e:
                    logger.error(f"JSON error: {e}")
                    continue

async def fetch_and_process_events():
    """Fetch and process SSE events - runs as async task."""
    global shutdown_flag
    
    logger.info("=" * 60)
    logger.info("Starting SSE stream processor...")
    logger.info(f"API Key configured: {bool(API_KEY)}")
    logger.info(f"API Key length: {len(API_KEY) if API_KEY else 0}")
    logger.info(f"SSE URL: {SSE_URL}")
    logger.info("=" * 60)
    
    if not API_KEY:
        logger.error("❌ API_KEY not set!")
        return
    
    # Create Basic Auth header
    auth_string = f"{API_KEY}:"
    auth_bytes = base64.b64encode(auth_string.encode()).decode('utf-8')
    auth_header = f"Basic {auth_bytes}"
    
    retry_count = 0
    max_retries = 100
    
    while not shutdown_flag:
        try:
            headers = {
                "Accept": "application/json",
                "Authorization": auth_header,
                "User-Agent": "Companies-House-Monitor/1.0"
            }
            
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            
            logger.info(f"Connecting to {SSE_URL}...")
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                async for event_data in sse_event_generator(client, headers):
                    if shutdown_flag:
                        break
                    
                    company_data = process_event(event_data)
                    
                    if company_data:
                        await process_and_broadcast(company_data)
            
            # If we get here, connection was lost
            logger.warning("SSE connection lost, reconnecting...")
            retry_count += 1
            
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP Error: {e.response.status_code}")
            logger.error(f"Response: {e.response.text[:200] if hasattr(e.response, 'text') else 'N/A'}")
            retry_count += 1
            
            if e.response.status_code in [400, 401, 403]:
                logger.error("❌ Auth/Request error - stopping retries")
                return
        
        except Exception as e:
            logger.error(f"❌ SSE Error: {e}")
            logger.error(f"Type: {type(e).__name__}")
            retry_count += 1
        
        if retry_count >= max_retries:
            logger.error(f"❌ Max retries ({max_retries}) reached")
            return
        
        if not shutdown_flag and retry_count > 0:
            logger.info(f"Retrying in 30s... ({retry_count}/{max_retries})")
            await asyncio.sleep(30)
    
    logger.info("SSE processor shutdown complete")

# ============================================================================
# HTML DASHBOARD
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve the dashboard."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Companies House Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; color: #1a1a1a; }
        .container { max-width: 1600px; margin: 0 auto; padding: 0 20px; }
        header { background: white; border-bottom: 1px solid #e1e4e8; padding: 16px 0; position: sticky; top: 0; z-index: 100; }
        .header-content { display: flex; justify-content: space-between; align-items: center; }
        .logo { font-size: 18px; font-weight: 600; display: flex; align-items: center; gap: 10px; }
        .logo-icon { width: 24px; height: 24px; background: #0969da; border-radius: 6px; color: white; display: flex; align-items: center; justify-content: center; font-size: 14px; }
        .status { display: flex; align-items: center; gap: 8px; font-size: 13px; color: #1a7f37; font-weight: 500; }
        .status-dot { width: 8px; height: 8px; background: #1a7f37; border-radius: 50%; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .metrics-bar { background: white; border-bottom: 1px solid #e1e4e8; padding: 12px 0; }
        .metrics-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 24px; }
        .metric-label { font-size: 12px; color: #656d76; text-transform: uppercase; font-weight: 500; }
        .metric-value { font-size: 24px; font-weight: 600; font-family: monospace; }
        .metric-value.target { color: #1a7f37; }
        .metric-value.total { color: #0969da; }
        .content { background: white; margin: 20px 0; border: 1px solid #e1e4e8; border-radius: 6px; }
        .content-header { padding: 16px 20px; border-bottom: 1px solid #e1e4e8; background: #f6f8fa; }
        .content-title { font-size: 14px; font-weight: 600; }
        .table-container { max-height: 700px; overflow-y: auto; }
        .table { width: 100%; border-collapse: collapse; font-size: 13px; }
        .thead { background: #f6f8fa; border-bottom: 1px solid #e1e4e8; }
        .th { padding: 12px 20px; text-align: left; font-weight: 600; color: #656d76; text-transform: uppercase; font-size: 11px; }
        .tr { border-bottom: 1px solid #e1e4e8; }
        .tr:hover { background: #f6f8fa; }
        .tr.new { background: #dafbe1; animation: highlight 3s ease; }
        @keyframes highlight { 0% { background: #dafbe1; } 100% { background: transparent; } }
        .td { padding: 12px 20px; }
        .company-number { font-family: monospace; font-size: 12px; color: #656d76; }
        .company-name { font-weight: 500; }
        .sic-code { display: inline-block; background: #0969da; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-family: monospace; margin-right: 4px; }
        .badge { display: inline-block; padding: 2px 10px; border-radius: 10px; font-size: 11px; font-weight: 500; text-transform: uppercase; }
        .badge.target_sic { background: #dafbe1; color: #1a7f37; }
        .time-ago { font-family: monospace; font-size: 12px; color: #656d76; }
        .links a { color: #0969da; text-decoration: none; font-size: 12px; margin-right: 12px; }
    </style>
</head>
<body>
    <header><div class="container"><div class="header-content"><div class="logo"><div class="logo-icon">CH</div>Companies House Monitor</div><div class="status"><div class="status-dot"></div><span id="status-text">Connecting...</span></div></div></div></header>
    <div class="metrics-bar"><div class="container"><div class="metrics-grid"><div class="metric"><div class="metric-label">Target SIC</div><div class="metric-value target" id="metric-target">0</div></div><div class="metric"><div class="metric-label">Total Today</div><div class="metric-value total" id="metric-total">0</div></div><div class="metric"><div class="metric-label">Companies</div><div class="metric-value" id="companies-count">0</div></div><div class="metric"><div class="metric-label">Status</div><div class="metric-value" id="metric-status">Live</div></div></div></div></div>
    <div class="container"><div class="content"><div class="content-header"><div class="content-title">Today's Incorporations</div><div id="companies-count-header">0 companies</div></div><div class="table-container"><table class="table"><thead class="thead"><tr><th class="th">Company Number</th><th class="th">Company Name</th><th class="th">SIC Codes</th><th class="th">Type</th><th class="th">Time</th><th class="th">Links</th></tr></thead><tbody id="companies-table"></tbody></table></div></div></div>
    <script>
        let companies = []; let ws;
        function updateStatus(connected) { const t = document.getElementById('status-text'); const d = document.querySelector('.status-dot'); if (connected) { t.textContent = 'Live'; t.style.color = '#1a7f37'; d.style.background = '#1a7f37'; } else { t.textContent = 'Disconnected'; t.style.color = '#cf222e'; d.style.background = '#cf222e'; } }
        function connect() { const p = window.location.protocol === 'https:' ? 'wss:' : 'ws:'; ws = new WebSocket(`${p}//${window.location.host}/ws`); ws.onopen = () => { updateStatus(true); fetchInitialData(); }; ws.onclose = () => { updateStatus(false); setTimeout(connect, 3000); }; ws.onmessage = (e) => { const d = JSON.parse(e.data); if (d.type === 'company') addCompany(d.company); else if (d.type === 'metrics') updateMetrics(d.metrics); }; }
        async function fetchInitialData() { const m = await fetch('/api/metrics'); updateMetrics(await m.json()); const c = await fetch('/api/companies?limit=100'); companies = await c.json(); renderCompanies(); }
        function addCompany(c) { if (companies.some(x => x.company_number === c.company_number)) return; companies.unshift(c); if (companies.length > 100) companies = companies.slice(0, 100); renderCompanies(); fetch('/api/metrics').then(r => r.json()).then(updateMetrics); }
        function updateMetrics(m) { document.getElementById('metric-target').textContent = m.target_count || 0; document.getElementById('metric-total').textContent = m.total_count || 0; document.getElementById('companies-count').textContent = m.total_count || 0; document.getElementById('companies-count-header').textContent = `${m.total_count} companies`; }
        function renderCompanies() { const t = document.getElementById('companies-table'); if (companies.length === 0) { t.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:60px;color:#656d76;">No companies yet today</td></tr>'; return; } document.getElementById('companies-count-header').textContent = `${companies.length} companies`; t.innerHTML = companies.map((c, i) => `<tr class="tr ${i < 5 ? 'new' : ''}"><td class="td"><div class="company-number">${c.company_number}</div></td><td class="td"><div class="company-name">${c.company_name}</div></td><td class="td">${c.sic_codes ? JSON.parse(c.sic_codes).slice(0,3).map(s => `<span class="sic-code">${s}</span>`).join('') : ''}</td><td class="td"><span class="badge ${c.source_type}">${c.source_type}</span></td><td class="td"><div class="time-ago">${formatAge(c.published_at)}</div></td><td class="td"><a href="https://find-and-update.company-information.service.gov.uk/company/${c.company_number}" target="_blank">CH</a><a href="https://www.google.com/search?q=${encodeURIComponent(c.company_name)}" target="_blank">Google</a></td></tr>`).join(''); }
        function formatAge(p) { try { const d = Math.floor((new Date() - new Date(p)) / 1000); if (d < 60) return `${d}s`; else if (d < 3600) return `${Math.floor(d / 60)}m`; else return `${Math.floor(d / 3600)}h`; } catch { return 'N/A'; } }
        setInterval(() => { if (ws && ws.readyState === WebSocket.OPEN) fetch('/api/metrics').then(r => r.json()).then(updateMetrics); }, 5000);
        connect();
    </script>
</body>
</html>
    """

# ============================================================================
# WEBSOCKET & API ENDPOINTS
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    logger.info(f"✓ Client connected. Total: {len(connected_clients)}")
    
    try:
        metrics = await get_metrics()
        await websocket.send_json({"type": "metrics", "metrics": metrics})
        
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    break
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        logger.info(f"✗ Client disconnected. Total: {len(connected_clients)}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if websocket in connected_clients:
            connected_clients.remove(websocket)

@app.get("/api/metrics")
async def api_metrics():
    return await get_metrics()

@app.get("/api/companies")
async def api_companies(limit: int = 100):
    return await get_companies(limit)

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("Companies House Monitor - Starting")
    logger.info("=" * 60)
    init_db()
    
    # Start SSE processor as background task
    logger.info("Starting SSE stream processor...")
    asyncio.create_task(fetch_and_process_events())
    logger.info("✓ SSE processor started")

@app.on_event("shutdown")
async def shutdown_event():
    global shutdown_flag
    logger.info("Shutting down...")
    shutdown_flag = True

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
