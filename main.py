"""
Companies House Real-Time Monitor - Render Version
Single app with SQLite, no external database needed
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import json
import asyncio
import os
import httpx
import signal
from datetime import datetime, timezone
from typing import Set, Optional
import logging
import threading

# Configuration
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")
API_KEY = os.getenv("API_KEY")
DATABASE_FILE = os.getenv("DATABASE_FILE", "/data/companies.db")
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
db_lock = threading.Lock()

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def init_db():
    """Initialize SQLite database."""
    os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True)
    
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

def insert_company(company_data: dict, source_type: str):
    """Insert company into database."""
    with db_lock:
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

def get_companies(limit: int = 100):
    """Get today's companies from database."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    with db_lock:
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

def get_metrics():
    """Get metrics from database."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    with db_lock:
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

async def broadcast_company(company_data: dict):
    """Broadcast to all WebSocket clients."""
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

def fetch_and_process_events():
    """Fetch events from SSE stream (runs in background thread)."""
    global last_event_id, shutdown_flag
    
    logger.info("Starting SSE stream processor...")
    
    headers = {
        "Authorization": f"Basic {API_KEY}",
        "Accept": "application/json"
    }
    
    retry_count = 0
    max_retries = 10
    retry_delay = 30
    
    while not shutdown_flag:
        try:
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            
            with httpx.stream("GET", SSE_URL, headers=headers, timeout=30.0) as response:
                response.raise_for_status()
                logger.info("✓ Connected to SSE stream")
                retry_count = 0
                
                for line in response.iter_lines():
                    if shutdown_flag:
                        break
                    
                    if not line:
                        continue
                    
                    if line.startswith("id:"):
                        last_event_id = line[3:].strip()
                        continue
                    
                    if line.startswith("data:"):
                        try:
                            event_data = json.loads(line[5:])
                            company_data = process_event(event_data)
                            
                            if company_data:
                                # Insert to database
                                insert_company(company_data, company_data['source_type'])
                                
                                # Broadcast to dashboard
                                asyncio.run(broadcast_company(company_data))
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON error: {e}")
                            continue
            
        except Exception as e:
            logger.error(f"SSE stream error: {e}")
            retry_count += 1
            
            if retry_count >= max_retries:
                logger.error(f"Max retries ({max_retries}) reached")
            
            if not shutdown_flag:
                logger.info(f"Retrying in {retry_delay}s... (attempt {retry_count}/{max_retries})")
                import time
                time.sleep(retry_delay)

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
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #1a1a1a;
        }
        .container { max-width: 1600px; margin: 0 auto; padding: 0 20px; }
        header {
            background: white;
            border-bottom: 1px solid #e1e4e8;
            padding: 16px 0;
            position: sticky;
            top: 0;
            z-index: 100;
        }
        .header-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .logo {
            font-size: 18px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .logo-icon {
            width: 24px;
            height: 24px;
            background: #0969da;
            border-radius: 6px;
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 14px;
        }
        .status {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
            color: #1a7f37;
            font-weight: 500;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            background: #1a7f37;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .metrics-bar {
            background: white;
            border-bottom: 1px solid #e1e4e8;
            padding: 12px 0;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 24px;
        }
        .metric-label {
            font-size: 12px;
            color: #656d76;
            text-transform: uppercase;
            font-weight: 500;
        }
        .metric-value {
            font-size: 24px;
            font-weight: 600;
            font-family: monospace;
        }
        .metric-value.target { color: #1a7f37; }
        .metric-value.total { color: #0969da; }
        .content {
            background: white;
            margin: 20px 0;
            border: 1px solid #e1e4e8;
            border-radius: 6px;
        }
        .content-header {
            padding: 16px 20px;
            border-bottom: 1px solid #e1e4e8;
            background: #f6f8fa;
        }
        .content-title {
            font-size: 14px;
            font-weight: 600;
        }
        .table-container {
            max-height: 700px;
            overflow-y: auto;
        }
        .table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        .thead {
            background: #f6f8fa;
            border-bottom: 1px solid #e1e4e8;
        }
        .th {
            padding: 12px 20px;
            text-align: left;
            font-weight: 600;
            color: #656d76;
            text-transform: uppercase;
            font-size: 11px;
        }
        .tr {
            border-bottom: 1px solid #e1e4e8;
        }
        .tr:hover {
            background: #f6f8fa;
        }
        .tr.new {
            background: #dafbe1;
            animation: highlight 3s ease;
        }
        @keyframes highlight {
            0% { background: #dafbe1; }
            100% { background: transparent; }
        }
        .td {
            padding: 12px 20px;
        }
        .company-number {
            font-family: monospace;
            font-size: 12px;
            color: #656d76;
        }
        .company-name {
            font-weight: 500;
        }
        .sic-code {
            display: inline-block;
            background: #0969da;
            color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 11px;
            font-family: monospace;
            margin-right: 4px;
        }
        .badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: 500;
            text-transform: uppercase;
        }
        .badge.target_sic {
            background: #dafbe1;
            color: #1a7f37;
        }
        .time-ago {
            font-family: monospace;
            font-size: 12px;
            color: #656d76;
        }
        .links a {
            color: #0969da;
            text-decoration: none;
            font-size: 12px;
            margin-right: 12px;
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <div class="header-content">
                <div class="logo">
                    <div class="logo-icon">CH</div>
                    Companies House Monitor
                </div>
                <div class="status">
                    <div class="status-dot"></div>
                    <span id="status-text">Connecting...</span>
                </div>
            </div>
        </div>
    </header>
    <div class="metrics-bar">
        <div class="container">
            <div class="metrics-grid">
                <div class="metric">
                    <div class="metric-label">Target SIC</div>
                    <div class="metric-value target" id="metric-target">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Total Today</div>
                    <div class="metric-value total" id="metric-total">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Companies</div>
                    <div class="metric-value" id="companies-count">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Status</div>
                    <div class="metric-value" id="metric-status">Live</div>
                </div>
            </div>
        </div>
    </div>
    <div class="container">
        <div class="content">
            <div class="content-header">
                <div class="content-title">Today's Incorporations</div>
                <div id="companies-count-header">0 companies</div>
            </div>
            <div class="table-container">
                <table class="table">
                    <thead class="thead">
                        <tr>
                            <th class="th">Company Number</th>
                            <th class="th">Company Name</th>
                            <th class="th">SIC Codes</th>
                            <th class="th">Type</th>
                            <th class="th">Time</th>
                            <th class="th">Links</th>
                        </tr>
                    </thead>
                    <tbody id="companies-table"></tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
        let companies = [];
        let ws;
        
        function updateStatus(connected) {
            const text = document.getElementById('status-text');
            const dot = document.querySelector('.status-dot');
            if (connected) {
                text.textContent = 'Live';
                text.style.color = '#1a7f37';
                dot.style.background = '#1a7f37';
            } else {
                text.textContent = 'Disconnected';
                text.style.color = '#cf222e';
                dot.style.background = '#cf222e';
            }
        }
        
        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                updateStatus(true);
                fetchInitialData();
            };
            
            ws.onclose = () => {
                updateStatus(false);
                setTimeout(connect, 3000);
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.type === 'company') {
                    addCompany(data.company);
                } else if (data.type === 'metrics') {
                    updateMetrics(data.metrics);
                }
            };
        }
        
        async function fetchInitialData() {
            const metricsRes = await fetch('/api/metrics');
            const metrics = await metricsRes.json();
            updateMetrics(metrics);
            
            const companiesRes = await fetch('/api/companies?limit=100');
            const companiesData = await companiesRes.json();
            companies = companiesData;
            renderCompanies();
        }
        
        function addCompany(company) {
            const exists = companies.some(c => c.company_number === company.company_number);
            if (exists) return;
            
            companies.unshift(company);
            if (companies.length > 100) companies = companies.slice(0, 100);
            renderCompanies();
            fetch('/api/metrics').then(r => r.json()).then(updateMetrics);
        }
        
        function updateMetrics(metrics) {
            document.getElementById('metric-target').textContent = metrics.target_count || 0;
            document.getElementById('metric-total').textContent = metrics.total_count || 0;
            document.getElementById('companies-count').textContent = metrics.total_count || 0;
            document.getElementById('companies-count-header').textContent = `${metrics.total_count} companies`;
        }
        
        function renderCompanies() {
            const tbody = document.getElementById('companies-table');
            if (companies.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:60px;color:#656d76;">No companies yet today</td></tr>';
                return;
            }
            
            document.getElementById('companies-count-header').textContent = `${companies.length} companies`;
            
            tbody.innerHTML = companies.map((c, i) => `
                <tr class="tr ${i < 5 ? 'new' : ''}">
                    <td class="td"><div class="company-number">${c.company_number}</div></td>
                    <td class="td"><div class="company-name">${c.company_name}</div></td>
                    <td class="td">${c.sic_codes ? JSON.parse(c.sic_codes).slice(0,3).map(s => `<span class="sic-code">${s}</span>`).join('') : ''}</td>
                    <td class="td"><span class="badge ${c.source_type}">${c.source_type}</span></td>
                    <td class="td"><div class="time-ago">${formatAge(c.published_at)}</div></td>
                    <td class="td">
                        <a href="https://find-and-update.company-information.service.gov.uk/company/${c.company_number}" target="_blank">CH</a>
                        <a href="https://www.google.com/search?q=${encodeURIComponent(c.company_name)}" target="_blank">Google</a>
                    </td>
                </tr>
            `).join('');
        }
        
        function formatAge(publishedAt) {
            try {
                const published = new Date(publishedAt);
                const diff = Math.floor((new Date() - published) / 1000);
                if (diff < 60) return `${diff}s`;
                else if (diff < 3600) return `${Math.floor(diff / 60)}m`;
                else return `${Math.floor(diff / 3600)}h`;
            } catch { return 'N/A'; }
        }
        
        setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                fetch('/api/metrics').then(r => r.json()).then(updateMetrics);
            }
        }, 5000);
        
        connect();
    </script>
</body>
</html>
    """

# ============================================================================
# WEBSOCKET ENDPOINT
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle WebSocket connections."""
    await websocket.accept()
    connected_clients.add(websocket)
    logger.info(f"✓ Client connected. Total: {len(connected_clients)}")
    
    try:
        # Send initial metrics
        metrics = get_metrics()
        await websocket.send_json({
            "type": "metrics",
            "metrics": metrics
        })
        
        # Keep connection alive
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

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/metrics")
async def api_metrics():
    """Get metrics."""
    return get_metrics()

@app.get("/api/companies")
async def api_companies(limit: int = 100):
    """Get today's companies."""
    return get_companies(limit)

# ============================================================================
# STARTUP
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize database and start SSE processor."""
    logger.info("=" * 60)
    logger.info("Companies House Monitor - Starting")
    logger.info("=" * 60)
    
    # Initialize database
    init_db()
    
    # Start SSE processor in background thread
    thread = threading.Thread(target=fetch_and_process_events, daemon=True)
    thread.start()
    logger.info("✓ SSE processor started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global shutdown_flag
    logger.info("Shutting down...")
    shutdown_flag = True

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
