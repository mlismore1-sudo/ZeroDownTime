"""
Companies House Real-Time Dashboard - FIXED DATE
Always shows today's companies (dynamic date)
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg
import json
import asyncio
from datetime import datetime, timezone
import os
from typing import Set
import logging

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8000))

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Store connected WebSocket clients
connected_clients: Set[WebSocket] = set()

# Database connection
db_conn = None

def get_db_connection():
    """Get or create database connection."""
    global db_conn
    if db_conn is None:
        db_conn = psycopg.connect(DATABASE_URL)
    return db_conn

# ============================================================================
# HTML FRONTEND - FIXED DATE
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve the professional dashboard."""
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
            font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f5f7fa;
            color: #1a1a1a;
            line-height: 1.5;
        }
        
        .container { max-width: 1600px; margin: 0 auto; padding: 0 20px; }
        
        /* Header */
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
            color: #0d1117;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .logo-icon {
            width: 24px;
            height: 24px;
            background: #0969da;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 14px;
        }
        
        .status-bar {
            display: flex;
            align-items: center;
            gap: 20px;
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
        
        .timestamp {
            font-size: 13px;
            color: #656d76;
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
        }
        
        /* Metrics */
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
        
        .metric {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        
        .metric-label {
            font-size: 12px;
            color: #656d76;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }
        
        .metric-value {
            font-size: 24px;
            font-weight: 600;
            color: #0d1117;
            font-family: 'SF Mono', Monaco, monospace;
        }
        
        .metric-value.target { color: #1a7f37; }
        .metric-value.restricted { color: #cf222e; }
        .metric-value.total { color: #0969da; }
        
        /* Content */
        .content {
            background: white;
            margin: 20px 0;
            border: 1px solid #e1e4e8;
            border-radius: 6px;
            overflow: hidden;
        }
        
        .content-header {
            padding: 16px 20px;
            border-bottom: 1px solid #e1e4e8;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: #f6f8fa;
        }
        
        .content-title {
            font-size: 14px;
            font-weight: 600;
            color: #0d1117;
        }
        
        .content-count {
            font-size: 13px;
            color: #656d76;
            font-family: 'SF Mono', Monaco, monospace;
        }
        
        /* Table */
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
            letter-spacing: 0.5px;
            font-size: 11px;
        }
        
        .tr {
            border-bottom: 1px solid #e1e4e8;
            transition: background 0.15s;
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
            vertical-align: middle;
        }
        
        .company-number {
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 12px;
            color: #656d76;
        }
        
        .company-name-cell {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .company-name {
            font-weight: 500;
            color: #0d1117;
        }
        
        .copy-btn {
            background: #f6f8fa;
            border: 1px solid #d0d7de;
            border-radius: 4px;
            padding: 2px 6px;
            cursor: pointer;
            font-size: 11px;
            color: #656d76;
            transition: all 0.15s;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        
        .copy-btn:hover {
            background: #eaeef2;
            border-color: #8d96a0;
            color: #0d1117;
        }
        
        .copy-btn.copied {
            background: #1a7f37;
            border-color: #1a7f37;
            color: white;
        }
        
        .sic-code {
            display: inline-block;
            background: #0969da;
            color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 11px;
            font-family: 'SF Mono', Monaco, monospace;
            margin-right: 4px;
        }
        
        .badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .badge.target_sic {
            background: #dafbe1;
            color: #1a7f37;
        }
        
        .badge.buzzword {
            background: #ddf4ff;
            color: #0969da;
        }
        
        .badge.restricted_sic {
            background: #ffebe9;
            color: #cf222e;
        }
        
        .time-ago {
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 12px;
            color: #656d76;
        }
        
        .links {
            display: flex;
            gap: 12px;
        }
        
        .links a {
            color: #0969da;
            text-decoration: none;
            font-size: 12px;
        }
        
        .links a:hover {
            text-decoration: underline;
        }
        
        /* Empty state */
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #656d76;
        }
        
        .empty-state p {
            font-size: 14px;
        }
        
        /* Scrollbar */
        .table-container {
            max-height: 700px;
            overflow-y: auto;
        }
        
        .table-container::-webkit-scrollbar {
            width: 8px;
        }
        
        .table-container::-webkit-scrollbar-track {
            background: #f6f8fa;
        }
        
        .table-container::-webkit-scrollbar-thumb {
            background: #d0d7de;
            border-radius: 4px;
        }
        
        .table-container::-webkit-scrollbar-thumb:hover {
            background: #8d96a0;
        }
        
        /* Toast notification */
        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #0d1117;
            color: white;
            padding: 12px 20px;
            border-radius: 6px;
            font-size: 13px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            opacity: 0;
            transform: translateY(20px);
            transition: all 0.3s;
            z-index: 1000;
        }
        
        .toast.show {
            opacity: 1;
            transform: translateY(0);
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
                <div class="status-bar">
                    <div class="status">
                        <div class="status-dot"></div>
                        <span id="status-text">Connecting...</span>
                    </div>
                    <div class="timestamp" id="timestamp">--:--:--</div>
                </div>
            </div>
        </div>
    </header>
    
    <div class="metrics-bar">
        <div class="container">
            <div class="metrics-grid">
                <div class="metric">
                    <div class="metric-label">Target & Buzzword</div>
                    <div class="metric-value target" id="metric-target">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Restricted SIC</div>
                    <div class="metric-value restricted" id="metric-restricted">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Total Today</div>
                    <div class="metric-value total" id="metric-total">0</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Connected</div>
                    <div class="metric-value" id="metric-clients">1</div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="container">
        <div class="content">
            <div class="content-header">
                <div class="content-title">Today's Incorporations</div>
                <div class="content-count" id="companies-count">0 companies</div>
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
                    <tbody id="companies-table">
                        <tr>
                            <td colspan="6" class="empty-state">
                                <p>Loading companies...</p>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">Copied to clipboard!</div>
    
    <script>
        let ws;
        let companies = [];
        let reconnectAttempts = 0;
        
        function updateStatus(connected) {
            const text = document.getElementById('status-text');
            const dot = document.querySelector('.status-dot');
            const timestamp = document.getElementById('timestamp');
            
            if (connected) {
                text.textContent = 'Live';
                text.style.color = '#1a7f37';
                dot.style.background = '#1a7f37';
                timestamp.textContent = new Date().toLocaleTimeString('en-GB');
            } else {
                text.textContent = 'Disconnected';
                text.style.color = '#cf222e';
                dot.style.background = '#cf222e';
            }
        }
        
        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function() {
                console.log('WebSocket connected');
                updateStatus(true);
                reconnectAttempts = 0;
                fetchInitialData();
            };
            
            ws.onclose = function() {
                console.log('WebSocket disconnected');
                updateStatus(false);
                const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 10000);
                reconnectAttempts++;
                setTimeout(connect, delay);
            };
            
            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
                updateStatus(false);
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                console.log('Received:', data);
                
                if (data.type === 'company') {
                    addCompany(data.company);
                } else if (data.type === 'metrics') {
                    updateMetrics(data.metrics);
                }
            };
        }
        
        async function fetchInitialData() {
            try {
                const metricsRes = await fetch('/api/metrics');
                const metrics = await metricsRes.json();
                updateMetrics(metrics);
                
                const companiesRes = await fetch('/api/companies?limit=100');
                const companiesData = await companiesRes.json();
                
                console.log('Initial companies:', companiesData.length);
                companies = companiesData;
                renderCompanies();
            } catch (error) {
                console.error('Error fetching initial data:', error);
            }
        }
        
        function addCompany(company) {
            // Check if company already exists
            const exists = companies.some(c => c.company_number === company.company_number);
            if (exists) {
                console.log('Company already exists:', company.company_number);
                return;
            }
            
            // Add to array
            companies.push(company);
            if (companies.length > 100) companies = companies.slice(0, 100);
            
            // Sort by published_at (most recent first)
            companies.sort((a, b) => {
                const timeA = new Date(a.published_at || 0);
                const timeB = new Date(b.published_at || 0);
                return timeB - timeA; // Descending (most recent first)
            });
            
            console.log('New company added, total:', companies.length);
            
            // Re-render table
            renderCompanies();
            
            // Update metrics
            fetch('/api/metrics').then(res => res.json()).then(updateMetrics);
        }
        
        function updateMetrics(metrics) {
            document.getElementById('metric-target').textContent = metrics.target_count || 0;
            document.getElementById('metric-restricted').textContent = metrics.restricted_count || 0;
            document.getElementById('metric-total').textContent = metrics.total_count || 0;
            document.getElementById('companies-count').textContent = `${metrics.total_count} companies`;
        }
        
        function renderCompanies() {
            const tbody = document.getElementById('companies-table');
            const count = document.getElementById('companies-count');
            
            if (companies.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-state"><p>No companies yet today</p></td></tr>';
                count.textContent = '0 companies';
                return;
            }
            
            count.textContent = `${companies.length} companies`;
            
            tbody.innerHTML = companies.map((company, index) => `
                <tr class="tr ${index < 5 ? 'new' : ''}">
                    <td class="td">
                        <div class="company-number">${company.company_number}</div>
                    </td>
                    <td class="td">
                        <div class="company-name-cell">
                            <div class="company-name">${company.company_name}</div>
                            <button class="copy-btn" onclick="copyToClipboard('${company.company_name.replace(/'/g, "\\'")}', this)">
                                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                                </svg>
                            </button>
                        </div>
                    </td>
                    <td class="td">
                        ${company.sic_codes ? company.sic_codes.slice(0, 3).map(sic => `<span class="sic-code">${sic}</span>`).join('') : ''}
                    </td>
                    <td class="td">
                        <span class="badge ${company.source_type}">${formatSourceType(company.source_type)}</span>
                    </td>
                    <td class="td">
                        <div class="time-ago">${formatAge(company.published_at)}</div>
                    </td>
                    <td class="td">
                        <div class="links">
                            <a href="https://find-and-update.company-information.service.gov.uk/company/${company.company_number}" target="_blank">CH</a>
                            <a href="https://www.google.com/search?q=${encodeURIComponent(company.company_name)}" target="_blank">Google</a>
                        </div>
                    </td>
                </tr>
            `).join('');
            
            console.log('Table rendered with', companies.length, 'companies');
        }
        
        function copyToClipboard(text, btn) {
            navigator.clipboard.writeText(text).then(() => {
                // Show visual feedback
                btn.classList.add('copied');
                btn.innerHTML = '✓';
                
                // Show toast
                const toast = document.getElementById('toast');
                toast.classList.add('show');
                
                // Reset after 2 seconds
                setTimeout(() => {
                    btn.classList.remove('copied');
                    btn.innerHTML = `
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                        </svg>
                    `;
                    toast.classList.remove('show');
                }, 2000);
            }).catch(err => {
                console.error('Failed to copy:', err);
            });
        }
        
        function formatSourceType(type) {
            const formats = {
                'target_sic': 'Target',
                'buzzword': 'Buzzword',
                'restricted_sic': 'Restricted'
            };
            return formats[type] || type;
        }
        
        function formatAge(publishedAt) {
            try {
                const published = new Date(publishedAt);
                const now = new Date();
                const diff = Math.floor((now - published) / 1000);
                if (diff < 60) return `${diff}s`;
                else if (diff < 3600) return `${Math.floor(diff / 60)}m`;
                else return `${Math.floor(diff / 3600)}h`;
            } catch { return 'N/A'; }
        }
        
        // Auto-refresh metrics every 5 seconds
        setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                fetch('/api/metrics').then(res => res.json()).then(updateMetrics);
                document.getElementById('timestamp').textContent = new Date().toLocaleTimeString('en-GB');
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
    logger.info(f"Client connected. Total clients: {len(connected_clients)}")
    
    try:
        metrics = await get_metrics()
        await websocket.send_json({
            "type": "metrics",
            "metrics": metrics
        })
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    break
                
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        logger.info(f"Client disconnected. Total clients: {len(connected_clients)}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        connected_clients.remove(websocket)

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/metrics")
async def get_metrics():
    """Get current metrics from database - uses dynamic UTC date."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Get today's date dynamically (UTC)
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            cur.execute("""
                SELECT COUNT(*) FROM screened_companies 
                WHERE incorporation_date = %s 
                AND source_type IN ('target_sic', 'buzzword')
            """, (today,))
            target_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM screened_companies 
                WHERE incorporation_date = %s 
                AND source_type = 'restricted_sic'
            """, (today,))
            restricted_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM screened_companies 
                WHERE incorporation_date = %s
            """, (today,))
            total_count = cur.fetchone()[0]
            
            logger.info(f"Metrics for {today}: target={target_count}, restricted={restricted_count}, total={total_count}")
            
            return {
                "target_count": target_count,
                "restricted_count": restricted_count,
                "total_count": total_count
            }
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return {"target_count": 0, "restricted_count": 0, "total_count": 0}

@app.get("/api/companies")
async def get_companies(limit: int = 100):
    """Get today's companies - sorted by published_at DESC (most recent first)."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Get today's date dynamically (UTC)
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            logger.info(f"Fetching companies for {today}")
            
            cur.execute("""
                SELECT company_number, company_name, incorporation_date, 
                       sic_codes, source_type, published_at
                FROM screened_companies
                WHERE incorporation_date = %s
                ORDER BY published_at DESC
                LIMIT %s
            """, (today, limit,))
            
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            
            logger.info(f"Found {len(result)} companies for {today}")
            
            return result
    except Exception as e:
        logger.error(f"Error getting companies: {e}")
        return []

# ============================================================================
# BROADCAST FUNCTION
# ============================================================================

async def broadcast_company(company_data: dict):
    """Broadcast new company to all connected WebSocket clients."""
    if not connected_clients:
        logger.info(f"No clients connected, skipping broadcast")
        return
    
    message = {
        "type": "company",
        "company": company_data
    }
    
    disconnected = set()
    for client in connected_clients:
        try:
            await client.send_json(message)
            logger.info(f"Broadcasted {company_data['company_number']} to client")
        except Exception as e:
            logger.error(f"Error sending to client: {e}")
            disconnected.add(client)
    
    for client in disconnected:
        connected_clients.remove(client)
    
    logger.info(f"Broadcasted company {company_data['company_number']} to {len(connected_clients)} clients")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Companies House Dashboard on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
