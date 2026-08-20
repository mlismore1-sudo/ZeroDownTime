# [Same as before, but replace fetch_and_process_events() with this:]

def fetch_and_process_events():
    """Fetch events from Companies House SSE stream."""
    global last_event_id, shutdown_flag
    
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
    max_retries = 10
    retry_delay = 30
    
    while not shutdown_flag:
        try:
            headers = {
                "Accept": "application/json",
                "Authorization": auth_header,
                "User-Agent": "Companies-House-Monitor/1.0",
                "Connection": "keep-alive"
            }
            
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            
            logger.info(f"Connecting to {SSE_URL}...")
            logger.info(f"Headers: Accept={headers['Accept']}, Auth={headers['Authorization'][:20]}...")
            
            # Use httpx with longer timeout for SSE
            with httpx.Client(timeout=60.0) as client:
                logger.info("Making GET request...")
                
                response = client.get(SSE_URL, headers=headers, follow_redirects=True)
                
                logger.info(f"Response status: {response.status_code}")
                logger.info(f"Response headers: {dict(response.headers)}")
                
                if response.status_code == 503:
                    logger.error("❌ 503 Service Unavailable")
                    logger.error("Companies House SSE stream may be down")
                    retry_count += 1
                elif response.status_code == 400:
                    logger.error("❌ 400 Bad Request")
                    logger.error(f"Response: {response.text[:500]}")
                    retry_count = max_retries
                elif response.status_code == 401:
                    logger.error("❌ 401 Unauthorized")
                    logger.error("API key is invalid")
                    retry_count = max_retries
                elif response.status_code == 200:
                    logger.info("✓ Connected successfully!")
                    retry_count = 0
                    
                    # Process SSE stream
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
                                    insert_company(company_data, company_data['source_type'])
                                    asyncio.run(broadcast_company(company_data))
                                    
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON error: {e}")
                                continue
            
            if retry_count >= max_retries:
                logger.error(f"❌ Max retries reached")
                return
            
            if not shutdown_flag and retry_count > 0:
                logger.info(f"Retrying in {retry_delay}s... ({retry_count}/{max_retries})")
                import time
                time.sleep(retry_delay)
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            retry_count += 1
            
            if retry_count >= max_retries:
                logger.error(f"❌ Max retries reached")
                return
            
            if not shutdown_flag:
                logger.info(f"Retrying in {retry_delay}s... ({retry_count}/{max_retries})")
                import time
                time.sleep(retry_delay)
    
    logger.info("SSE processor shutdown complete")
