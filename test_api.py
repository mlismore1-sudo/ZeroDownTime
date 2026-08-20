"""
Test script to debug Companies House API authentication
Run this locally or on Render to test API key
"""

import os
import httpx

# Get API key from environment
API_KEY = os.getenv("API_KEY")
SSE_URL = os.getenv("SSE_URL", "https://stream.companieshouse.gov.uk/")

print("=" * 60)
print("Companies House API Test")
print("=" * 60)
print(f"API Key configured: {bool(API_KEY)}")
print(f"API Key length: {len(API_KEY) if API_KEY else 0}")
print(f"API Key starts with: {API_KEY[:10] if API_KEY else 'N/A'}...")
print(f"SSE URL: {SSE_URL}")
print("=" * 60)

if not API_KEY:
    print("❌ ERROR: API_KEY not set!")
    print("Set it with: export API_KEY=your-key-here")
    exit(1)

# Test 1: Basic auth with API key as username
print("\nTest 1: Basic Auth (API key as username)")
try:
    with httpx.Client(auth=(API_KEY, "")) as client:
        response = client.get(SSE_URL, headers={"Accept": "application/json"}, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("✓ SUCCESS! Authentication works!")
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - API key is invalid")
        elif response.status_code == 400:
            print("❌ 400 Bad Request - Check API key format")
        else:
            print(f"Response: {response.text[:200]}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 2: Try with Basic header manually
print("\nTest 2: Manual Basic Auth Header")
import base64
auth_string = f"{API_KEY}:"
auth_bytes = base64.b64encode(auth_string.encode()).decode()
headers = {
    "Authorization": f"Basic {auth_bytes}",
    "Accept": "application/json"
}
try:
    response = httpx.get(SSE_URL, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print("✓ SUCCESS!")
    else:
        print(f"Response: {response.text[:200]}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 3: Try without auth (should fail)
print("\nTest 3: No Authentication (should fail)")
try:
    response = httpx.get(SSE_URL, headers={"Accept": "application/json"}, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code == 401:
        print("✓ Expected 401 - API requires auth")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 60)
print("Test complete!")
print("=" * 60)
