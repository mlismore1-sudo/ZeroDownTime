"""
Companies House Real-Time Dashboard - FastAPI WebSocket Server
VERSION 2 - Auto-refresh, show existing companies, better status
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg
import json
import asyncio
from datetime import datetime
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
app = FastAPI(title="Companies House Real-Time Dashboard")

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
# HTML FRONTEND - IMPROVED
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve the real-time dashboard."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🔍 Live Companies House Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        header {
            background: white;
            padding: 20px 30px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        h1 { color: #333; font-size: 28px; margin-bottom: 10px; }
        .status-bar {
            display: flex;
            align-items: center;
            gap: 15px;
            margin-top: 15px;
            padding: 10px 15px;
            background: #f8fafc;
            border-radius: 8px;
        }
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        .status-indicator.connected { background: #10b981; }
        .status-indicator.disconnected { background: #ef4444; animation: none; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .status-text { font-weight: 600; }
        .status-text.connected { color: #10b981; }
        .status-text.disconnected { color: #ef4444; }
        .last-update { color: #666; font-size: 13px; }
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .metric-label { font-size: 14px; color: #666; margin-bottom: 5px; }
        .metric-value { font-size: 32px; font-weight: bold; color: #667eea; }
        .companies-container {
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .companies-header {
            padding: 20px 30px;
            background: #f8fafc;
            border-bottom: 2px solid #e2e8f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .companies-header h2 { color: #333; font-size: 20px; }
        .companies-count { color: #666; font-size: 14px; }
        .companies-list { max-height: 600px; overflow-y: auto; }
        .company-card {
            padding: 15px 30px;
            border-bottom: 1px solid #e2e8f0;
            transition: all 0.3s ease;
            animation: slideIn 0.5s ease;
        }
        .company-card:hover { background: #f8fafc; }
        .company-card.new { background: #ecfdf5; border-left: 4px solid #10b981; }
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        .company-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .company-number {
            font-size: 12px;
            color: #666;
            background: #f1f5f9;
            padding: 3px 8px;
            border-radius: 4px;
        }
        .company-name { font-size: 16px; font-weight: 600; color: #333; }
        .company-meta {
            display: flex;
            gap: 15px;
            font-size: 13px;
            color: #666;
            flex-wrap: wrap;
        }
        .sic-code {
            background: #667eea;
            color: white;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 12px;
        }
        .source-type { font-weight: 600; }
        .source-type.target_sic { color: #10b981; }
        .source-type.buzzword { color: #3b82f6; }
        .source-type.restricted_sic { color: #ef4444; }
        .company-links { margin-top: 10px; }
        .company-links a {
            color: #667eea;
            text-decoration: none;
            font-size: 13px;
            margin-right: 15px;
        }
        .company-links a:hover { text-decoration: underline; }
        .empty-state { text-align: center; padding: 60px 20px; color: #999; }
        .empty-state p { font-size: 16px; }
        .last-updated {
            text-align: right;
            padding: 10px 30px;
            font-size: 12px;
            color: #999;
            background: #f8fafc;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔍 Live Companies House Dashboard</h1>
            <div class="status-bar">
                <div class="status-indicator" id="status-indicator"></div>
                <span class="status-text" id="status-text">Connecting...</span>
                <span class="last-update" id="last-update">Last update: Never</span>
            </div>
        </header>
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Target & Buzzword</div>
                <div class="metric-value" id="metric-target">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Restricted SIC</div>
                <div class="metric-value" id="metric-restricted">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Today</div>
                <div class="metric-value" id="metric-total">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Connected Clients</div>
                <div class="metric-value" id="metric-clients">1</div>
            </div>
        </div>
        <div class="companies-container">
            <div class="companies-header">
                <h2>📊 Today's Companies</h2>
                <span class="companies-count" id="companies-count">0 companies</span>
            </div>
            <div class="companies-list" id="companies-list">
                <div class="empty-state">
                    <p>⏳ Loading companies...</p>
                </div>
            </div>
            <div class="last-updated">Last updated: <span id="last-updated">Never</span></div>
        </div>
    </div>
    <script>
        let ws;
        let companies = [];
        let reconnectAttempts = 0;
        
        function updateStatus(connected) {
            const indicator = document.getElementById('status-indicator');
            const text = document.getElementById('status-text');
            const lastUpdate = document.getElementById('last-update');
            
            if (connected) {
                indicator.className = 'status-indicator connected';
                text.className = 'status-text connected';
                text.textContent = 'Live ●';
                lastUpdate.textContent = 'Last update: ' + new Date().toLocaleTimeString();
            } else {
                indicator.className = 'status-indicator disconnected';
                text.className = 'status-text disconnected';
                text.textContent = 'Disconnected';
            }
        }
        
        function connect() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function() {
                console.log('Connected to WebSocket');
                updateStatus(true);
                reconnectAttempts = 0;
                // Request initial data
                fetchInitialData();
            };
            
            ws.onclose = function() {
                console.log('Disconnected from WebSocket');
                updateStatus(false);
                // Reconnect with exponential backoff
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
                if (data.type === 'company') {
                    addCompany(data.company);
                } else if (data.type === 'metrics') {
                    updateMetrics(data.metrics);
                }
            };
        }
        
        async function fetchInitialData() {
            try {
                // Fetch metrics
                const metricsRes = await fetch('/api/metrics');
                const metrics = await metricsRes.json();
                updateMetrics(metrics);
                
                // Fetch today's companies
                const companiesRes = await fetch('/api/companies?limit=50');
                const companiesData = await companiesRes.json();
                
                companies = companiesData;
                renderCompanies();
            } catch (error) {
                console.error('Error fetching initial data:', error);
            }
        }
        
        function addCompany(company) {
            // Check if company already exists
            const exists = companies.some(c => c.company_number === company.company_number);
            if (exists) return;
            
            companies.unshift(company);
            if (companies.length > 50) companies = companies.slice(0, 50);
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
            const list = document.getElementById('companies-list');
            const count = document.getElementById('companies-count');
            
            if (companies.length === 0) {
                list.innerHTML = '<div class="empty-state"><p>⏳ No companies yet today</p><p style="font-size: 13px; margin-top: 10px;">Companies will appear here as they are incorporated</p></div>';
                count.textContent = '0 companies';
                return;
            }
            
            count.textContent = `${companies.length} companies`;
            
            list.innerHTML = companies.map(company => `
                <div class="company-card new">
                    <div class="company-header">
                        <span class="company-number">${company.company_number}</span>
                        <span class="source-type ${company.source_type}">${formatSourceType(company.source_type)}</span>
                    </div>
                    <div class="company-name">${company.company_name}</div>
                    <div class="company-meta">
                        <span>📅 ${company.incorporation_date}</span>
                        <span>🕐 ${formatAge(company.published_at)}</span>
                        <span>${company.sic_codes ? company.sic_codes.slice(0, 3).map(sic => `<span class="sic-code">${sic}</span>`).join(' ') : ''}</span>
                    </div>
                    <div class="company-links">
                        <a href="https://find-and-update.company-information.service.gov.uk/company/${company.company_number}" target="_blank">🏢 Companies House</a>
                        <a href="https://www.google.com/search?q=${encodeURIComponent(company.company_name.replace(' Limited', '').replace(' Ltd', ''))}" target="_blank">🔍 Google</a>
                    </div>
                </div>
            `).join('');
            
            document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
        }
        
        function formatSourceType(type) {
            const formats = {
                'target_sic': '🎯 Target SIC',
                'buzzword': '✨ Buzzword',
                'restricted_sic': '⚠️ Restricted'
            };
            return formats[type] || type;
        }
        
        function formatAge(publishedAt) {
            try {
                const published = new Date(publishedAt);
                const now = new Date();
                const diff = Math.floor((now - published) / 1000);
                if (diff < 60) return `${diff}s ago`;
                else if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
                else return `${Math.floor(diff / 3600)}h ago`;
            } catch { return 'N/A'; }
        }
        
        // Auto-refresh metrics every 5 seconds
        setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                fetch('/api/metrics').then(res => res.json()).then(updateMetrics);
            }
        }, 5000);
        
        // Connect on page load
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
        # Send initial metrics
        metrics = await get_metrics()
        await websocket.send_json({
            "type": "metrics",
            "metrics": metrics
        })
        
        # Keep connection alive with periodic pings
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
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
    """Get current metrics from database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            today = datetime.now().date().isoformat()
            
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
            
            return {
                "target_count": target_count,
                "restricted_count": restricted_count,
                "total_count": total_count
            }
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return {"target_count": 0, "restricted_count": 0, "total_count": 0}

@app.get("/api/companies")
async def get_companies(limit: int = 50):
    """Get today's companies - ORDER BY most recent first."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT company_number, company_name, incorporation_date, 
                       sic_codes, source_type, published_at
                FROM screened_companies
                WHERE incorporation_date = CURRENT_DATE
                ORDER BY published_at DESC, company_number DESC
                LIMIT %s
            """, (limit,))
            
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error getting companies: {e}")
        return []

# ============================================================================
# BROADCAST FUNCTION
# ============================================================================

async def broadcast_company(company_data: dict):
    """Broadcast new company to all connected clients."""
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
    
    logger.info(f"Broadcasted company {company_data['company_number']} to {len(connected_clients)} clients")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Companies House Dashboard on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
