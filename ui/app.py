"""
Live Companies House Screener - Streamlit UI
Reads from Supabase database (populated by Render worker)
"""

import streamlit as st
import pandas as pd
import psycopg
import json
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_db_connection():
    """Create a persistent database connection."""
    conn = psycopg.connect(st.secrets["DATABASE_URL"])
    return conn

def get_db_cursor():
    """Get a cursor from the cached connection."""
    return get_db_connection().cursor()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def today_in_uk() -> str:
    """Get today's date in UK timezone."""
    return date.today().isoformat()

def format_age(published_at: str) -> str:
    """Format age as mm:ss."""
    try:
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        age = datetime.now(published.tzinfo) - published
        return f"{int(age.total_seconds() // 60)}m {int(age.total_seconds() % 60)}s"
    except:
        return "N/A"

# ============================================================================
# STREAMLIT UI
# ============================================================================

# Page config
st.set_page_config(page_title="Live Companies House Screener", layout="wide")

# Header
st.title("🔍 Live Companies House Screener")
st.caption("Streaming newly incorporated UK companies")

# Metrics row
col1, col2, col3 = st.columns(3)

@st.cache_data(ttl=5)
def get_company_counts():
    """Get counts for each category."""
    with get_db_cursor() as cur:
        today = today_in_uk()
        
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
        
        return target_count, restricted_count, total_count

target_count, restricted_count, total_count = get_company_counts()

with col1:
    st.metric("🎯 Target & Buzzword Today", target_count)
with col2:
    st.metric("⚠️ Restricted SIC Today", restricted_count)
with col3:
    st.metric("📊 Total Today", total_count)

# Worker status (from database)
@st.cache_data(ttl=10)
def get_worker_status():
    """Get worker status from database."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT status, last_event_at, last_error 
            FROM worker_status 
            WHERE id = 1
        """)
        result = cur.fetchone()
        return result

worker_status = get_worker_status()
if worker_status:
    status, last_event, error = worker_status
    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"**Worker Status:** {status}")
    with col2:
        if last_event:
            st.caption(f"**Last Event:** {last_event.strftime('%H:%M:%S')}")

# User selection
st.sidebar.header("👤 Who is working?")
user_name = st.sidebar.radio("Select user", ["Brad", "James"], key="user_selector")

# ============================================================================
# TARGET & BUZZWORD TABLE
# ============================================================================

st.header("🎯 Target & Buzzword Companies")

@st.cache_data(ttl=10)
def get_target_companies():
    """Fetch target and buzzword companies."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT company_number, company_name, sic_codes, published_at,
                   'https://find-and-update.company-information.service.gov.uk/company/' || company_number as company_url
            FROM screened_companies
            WHERE incorporation_date = %s
            AND source_type IN ('target_sic', 'buzzword')
            ORDER BY published_at DESC
        """, (today_in_uk(),))
        
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        return pd.DataFrame(rows, columns=columns)

target_df = get_target_companies()

if not target_df.empty:
    display_df = target_df.copy()
    
    # Format SIC codes
    display_df['sic_codes'] = display_df['sic_codes'].apply(
        lambda x: ', '.join(json.loads(x)[:3]) if isinstance(x, str) else ', '.join(x[:3])
    )
    
    # Format age
    display_df['age'] = display_df['published_at'].apply(format_age)
    
    # Show relevant columns
    display_cols = ['company_name', 'sic_codes', 'age']
    
    st.dataframe(
        display_df[display_cols],
        column_config={
            "company_name": "Company Name",
            "sic_codes": "SIC Codes",
            "age": "Age (mm:ss)"
        },
        hide_index=True,
        use_container_width=True
    )
    
    # CSV export
    csv = target_df.to_csv(index=False)
    st.download_button("📥 Download CSV", csv, "target_buzzword_companies.csv", "text/csv")

else:
    st.info("No target or buzzword companies yet today")

# ============================================================================
# RESTRICTED SIC TABLE
# ============================================================================

st.header("⚠️ Restricted SIC Companies (for external review)")

@st.cache_data(ttl=10)
def get_restricted_companies():
    """Fetch restricted SIC companies."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT company_number, company_name, sic_codes, published_at,
                   'https://find-and-update.company-information.service.gov.uk/company/' || company_number as company_url
            FROM screened_companies
            WHERE incorporation_date = %s
            AND source_type = 'restricted_sic'
            ORDER BY published_at DESC
        """, (today_in_uk(),))
        
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        return pd.DataFrame(rows, columns=columns)

restricted_df = get_restricted_companies()

if not restricted_df.empty:
    display_df = restricted_df.copy()
    
    # Format SIC codes
    display_df['sic_codes'] = display_df['sic_codes'].apply(
        lambda x: ', '.join(json.loads(x)[:3]) if isinstance(x, str) else ', '.join(x[:3])
    )
    
    # Format age
    display_df['age'] = display_df['published_at'].apply(format_age)
    
    display_cols = ['company_name', 'sic_codes', 'age']
    
    st.dataframe(
        display_df[display_cols],
        column_config={
            "company_name": "Company Name",
            "sic_codes": "SIC Codes",
            "age": "Age (mm:ss)"
        },
        hide_index=True,
        use_container_width=True
    )
    
    csv = restricted_df.to_csv(index=False)
    st.download_button("📥 Download CSV", csv, "restricted_sic_companies.csv", "text/csv")

else:
    st.info("No restricted SIC companies yet today")

# ============================================================================
# USER SHORTLIST
# ============================================================================

st.sidebar.header(f"⭐ {user_name}'s Shortlist")

@st.cache_data(ttl=15)
def get_user_shortlist(user: str):
    """Get user's shortlisted companies."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT sc.company_number, sc.company_name, sc.sic_codes, sc.published_at,
                   us.shortlisted_at
            FROM user_shortlists us
            JOIN screened_companies sc ON us.company_number = sc.company_number
            WHERE us.user_name = %s
            ORDER BY us.shortlisted_at DESC
        """, (user,))
        
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        return pd.DataFrame(rows, columns=columns)

shortlist_df = get_user_shortlist(user_name)

if not shortlist_df.empty:
    st.sidebar.dataframe(shortlist_df[["company_name", "company_number"]], hide_index=True)
    
    csv = shortlist_df.to_csv(index=False)
    st.sidebar.download_button("📥 Download Shortlist CSV", csv, f"{user_name}_shortlist.csv", "text/csv")
else:
    st.sidebar.info("No shortlisted companies yet")

# Auto-refresh
import streamlit_autorefresh as st_autorefresh
st_autorefresh.st_autorefresh(interval=15000, key="datarefresh")
