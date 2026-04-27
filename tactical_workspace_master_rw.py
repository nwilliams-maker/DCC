import streamlit as st
import requests
import base64
import math
import pandas as pd
import time
import hashlib
import json
from datetime import datetime, timedelta
from streamlit_folium import st_folium
import folium
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import re

# --- CONFIG & CREDENTIALS ---
# We check the Environment (Railway) FIRST to avoid the Streamlit Secrets crash
ONFLEET_KEY = os.environ.get("ONFLEET_KEY")
GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_KEY")

# If Railway didn't have them, ONLY THEN do we try st.secrets (and we catch the error)
if not ONFLEET_KEY or not GOOGLE_MAPS_KEY:
    try:
        ONFLEET_KEY = ONFLEET_KEY or st.secrets.get("ONFLEET_KEY")
        GOOGLE_MAPS_KEY = GOOGLE_MAPS_KEY or st.secrets.get("GOOGLE_MAPS_KEY")
    except Exception:
        # If we get here, it means no secrets file exists AND Railway variables are missing
        pass

# Final check to keep the app from crashing with a traceback
if not ONFLEET_KEY or not GOOGLE_MAPS_KEY:
    st.error("🔑 **API Keys Missing!**")
    st.info("I couldn't find your keys in Railway's 'Variables' tab. Please double-check that you added ONFLEET_KEY and GOOGLE_MAPS_KEY there.")
    st.stop()

PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL") or "https://nwilliams-maker.github.io/DCC/portal-dcc-rw.html"
# GAS_WEB_APP_URL: deployment URL acts as auth — rotate via redeploy and update the Railway env var.
GAS_WEB_APP_URL = os.environ.get("GAS_WEB_APP_URL") or "https://script.google.com/macros/s/AKfycbyz16LuLJUJfrtUWxhvK8lGJCVSqRcrqPNOwLEICJ47Oa-BrRnBvFSsy4q8XXo-Y2DTAA/exec"
IC_SHEET_URL = os.environ.get("IC_SHEET_URL") or "https://docs.google.com/spreadsheets/d/1y6wX0x93iDc3gdK_nZKLD-2QcGkUHkcM75u90ffRO6k/edit#gid=0"

# --- TUNABLES (hoisted from inline magic numbers) ---
DEFAULT_DUE_DAYS = 14   # default deadline offset from today when dispatcher hasn't picked one
RATE_CRITICAL = 24.00   # $/stop — red status
RATE_WARNING = 21.00    # $/stop — orange status

# Lightweight stderr logger — replaces silent `except: pass` so failures are visible in Railway logs.
def _log_err(context, exc):
    try:
        print(f"[{context}] {type(exc).__name__}: {exc}", flush=True)
    except Exception:
        pass
SAVED_ROUTES_GID = "1477617688"
ACCEPTED_ROUTES_GID = "934075207"
DECLINED_ROUTES_GID = "600909788"
FIELD_NATION_GID = "1396320527"
FINALIZED_ROUTES_GID = "1907347870"
ARCHIVE_GID = "1841508981"  # Read-only side-channel: only used to harvest archived_wo for the WO counter; never used for clustering.

# Routes created before this date are skipped when reading the route database. This
# was added to cut over the pre-launch migration; rows older than the cutoff should
# eventually be moved to the Archive tab so this constant can go away.
MIGRATION_CUTOFF_DATE = "2026-04-20"

# Terraboost Media Brand Palette
TB_PURPLE = "#633094"
TB_GREEN = "#76bc21"
TB_APP_BG = "#f1f5f9"
TB_HOVER_GRAY = "#e2e8f0"

# Status Fills
TB_GREEN_FILL = "#dcfce7" # Ready
TB_BLUE_FILL = "#dbeafe"  # Sent
TB_RED_FILL = "#fee2e2"   # Flagged
TB_YELLOW_FILL = "#FEF9C3"     # Field Nation
TB_STATIC_FILL = "#f1f5f9"
TB_DIGITAL_FILL = "#ccfbf1"
TB_DIGITAL_BORDER = "#99f6e4" # Teal border

# Standardized Dark Text (for readability)
TB_GREEN_TEXT = "#166534"
TB_RED_TEXT = "#991b1b"
TB_STATIC_TEXT = "#475569"
TB_DIGITAL_TEXT = "#0f766e"

POD_CONFIGS = {
    "Blue": {"states": {"AL", "AR", "FL", "IL", "IA", "LA", "MI", "MN", "MS", "MO", "NC", "SC", "WI"}},
    "Green": {"states": {"CO", "DC", "GA", "IN", "KY", "MD", "NJ", "OH", "UT"}},
    "Orange": {"states": {"AK", "AZ", "CA", "HI", "ID", "NV", "OR", "WA"}},
    "Purple": {"states": {"KS", "MT", "NE", "NM", "ND", "OK", "SD", "TN", "TX", "WY"}},
    "Red": {"states": {"CT", "DE", "ME", "MA", "NH", "NY", "PA", "RI", "VT", "VA", "WV"}}
}

STATE_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN",
    "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC"
}

headers = {"Authorization": f"Basic {base64.b64encode(f'{ONFLEET_KEY}:'.encode()).decode()}"}

st.set_page_config(page_title="Terraboost Media: Dispatch Command Center", layout="wide")

# --- PINNED TOP-LEFT LOGO ---
# Function to convert the local image into web-safe code
def get_base64_image(image_path):
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except Exception as e:
        return ""

# Make sure "terraboost_logo.png" perfectly matches your saved file name!
logo_base64 = get_base64_image("terraboost_logo.png")

if logo_base64:
    st.markdown(f"""
        <div style="position: fixed; top: 15px; left: 20px; z-index: 999999;">
            <img src="data:image/png;base64,{logo_base64}" style="width: 140px;"> 
        </div>
    """, unsafe_allow_html=True)
else:
    st.sidebar.error("Logo file not found! Check the file name.")

# --- UI STYLING ---
st.components.v1.html("""
<script>
(function() {
    var SCROLL_KEY = 'tbm_scroll_pos';

    // Save scroll position on every scroll
    window.parent.document.addEventListener('scroll', function() {
        sessionStorage.setItem(SCROLL_KEY, window.parent.scrollY);
    }, { passive: true });

    // Restore scroll position whenever Streamlit rerenders
    var observer = new MutationObserver(function() {
        var saved = sessionStorage.getItem(SCROLL_KEY);
        if (saved && parseInt(saved) > 50) {
            window.parent.scrollTo({ top: parseInt(saved), behavior: 'instant' });
        }
    });
    observer.observe(window.parent.document.body, { childList: true, subtree: false });
})();
</script>
""", height=0)

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
.stApp {{ background-color: {TB_APP_BG} !important; color: #000000 !important; font-family: 'Inter', sans-serif !important; }}
/* Streamlit injects a fixed-position header bar by default with a dark/black background.
   Recolor it to match the page so the logo doesn't sit on a black strip. Header still
   exists (Streamlit needs it for menu/toolbar), it's just visually invisible now. */
header[data-testid="stHeader"] {{ background-color: {TB_APP_BG} !important; }}
header[data-testid="stHeader"]::before {{ background-color: {TB_APP_BG} !important; }}
div[data-testid="stToolbar"] {{ background-color: transparent !important; }}
.main .block-container {{ max-width: 1400px !important; padding-top: 1rem; padding-left: 1.5rem; padding-right: 1.5rem; }}

/* =========================================
   WIDGET & INPUT STANDARDIZATION (Fixes the White Box Glitch)
   ========================================= */
/* Force clean white backgrounds on all inputs */
div[data-baseweb="select"] > div,
div[data-baseweb="input"],
div[data-baseweb="input"] > div {{
    background-color: #ffffff !important;
    border-color: #cbd5e1 !important;
}}

/* Ensure text inside inputs is dark and legible */
input[type="text"], 
input[type="number"], 
div[data-baseweb="select"] div {{
    color: #0f172a !important;
    -webkit-text-fill-color: #0f172a !important;
    font-weight: 600 !important;
}}

/* Number Input — match date input outline style */
div[data-testid="stNumberInputContainer"] {{
    border-radius: 8px !important;
    border: 1px solid #cbd5e1 !important;
    background-color: #ffffff !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
    overflow: hidden !important;
}}

div[data-testid="stNumberInputContainer"]:focus-within {{
    border-color: #633094 !important;
    box-shadow: 0 0 0 2px rgba(99,48,148,0.15) !important;
}}

/* Kills the white box by forcing transparency on the button wrapper */
div[data-testid="stNumberInputContainer"] div[data-baseweb="input"] > div:nth-child(2) {{
    background-color: transparent !important;
}}

/* Style the + / - icons to match the theme */
div[data-testid="stNumberInputContainer"] button, 
div[data-testid="stNumberInputContainer"] svg {{
    color: #64748b !important;
    fill: #64748b !important;
    background-color: transparent !important;
}}

/* Email Content Preview (st.text_area) — visible border around the preview */
div[data-testid="stTextArea"] textarea,
div[data-testid="stTextAreaRootElement"] textarea,
div[data-baseweb="textarea"] {{
    border: 1px solid #cbd5e1 !important;
    border-radius: 8px !important;
    background-color: #ffffff !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}}

div[data-testid="stTextArea"] textarea:focus,
div[data-baseweb="textarea"]:focus-within {{
    border-color: #633094 !important;
    box-shadow: 0 0 0 2px rgba(99,48,148,0.15) !important;
}}

/* GLOBAL TABS CONTAINER - Clean & Floating with Bottom Line */
.stTabs [data-baseweb="tab-list"] {{ 
    justify-content: center; 
    gap: 12px; 
    background: transparent !important; /* Removes the gray box background */
    padding: 15px 15px 20px 15px !important; /* Adds extra padding on the bottom so pills don't touch the line */
    border-bottom: 2px solid #cbd5e1 !important; /* 🌟 THIS IS THE HORIZONTAL LINE 🌟 */
    margin-bottom: 15px !important; /* Pushes the dashboard content down slightly for breathing room */
}}

/* CENTERED PURPLE HEADERS */
h1, h2, h3, h4, h5, h6 {{ 
    font-weight: 800 !important; 
    text-align: center !important; 
    width: 100%;
}}

/* MODERN CONDENSED REFRESH BUTTON - FAR RIGHT */
div.refresh-btn-container {{
    display: flex;
    justify-content: flex-end;
    width: 100%;
}}

div.refresh-btn-container > div > button {{
    height: 32px !important; /* Slightly taller for breathing room */
    padding: 0 16px !important;
    font-size: 13px !important;
    border-radius: 20px !important;
    border: 1.2px solid #633094 !important;
    background-color: transparent !important;
    color: #633094 !important;
    font-weight: 700 !important;
    transition: all 0.2s ease-in-out !important;
    
    /* THE FIX: Forces icon and text onto one line perfectly centered */
    white-space: nowrap !important; 
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
}}

/* Ensures Streamlit's internal text wrapper doesn't force a line break */
div.refresh-btn-container > div > button div,
div.refresh-btn-container > div > button p {{
    white-space: nowrap !important;
    margin: 0 !important;
    padding: 0 !important;
}}
div.refresh-btn-container > div > button:hover {{
    background-color: #633094 !important;
    color: white !important;
    box-shadow: 0 2px 8px rgba(99, 48, 148, 0.3) !important;
}}

/* GLOBAL TABS STYLING */
.stTabs [data-baseweb="tab-list"] {{ justify-content: center; gap: 8px; background: rgba(255,255,255,0.6); padding: 10px; border-radius: 15px; }}

/* PERMANENT POD TAB OUTLINES & DARK TEXT */
.stTabs [data-baseweb="tab"] {{
    border-top: 1px solid #cbd5e1 !important;
    border-left: 1px solid #cbd5e1 !important;
    border-right: 1px solid #cbd5e1 !important;
    margin: 0 4px !important;
    transition: all 0.2s ease !important;
    font-weight: 800 !important;
    border-radius: 10px 10px 0 0 !important;
    padding: 10px 20px !important;
}}

/* GLOBAL TABS CONTAINER - Clean & Floating */
.stTabs [data-baseweb="tab-list"] {{ 
    justify-content: center; 
    gap: 12px; 
    background: transparent !important; /* Removes the gray box background */
    padding: 15px; 
}}

/* KILL THE DEFAULT UNDERLINE (The "Cutoff" source) */
.stTabs [data-baseweb="tab-highlight"] {{
    background-color: transparent !important;
}}

/* PERMANENT FLOATING PILLS - No flat bottoms */
.stTabs [data-baseweb="tab"] {{
    border-radius: 30px !important; /* Full rounded pill */
    margin: 0 5px !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    font-weight: 800 !important;
    padding: 8px 25px !important;
    border: 2px solid transparent !important; /* Invisible border until set below */
}}

/* Global Tab */
.stTabs [data-baseweb="tab"]:nth-of-type(1) {{ border: 2px solid #633094 !important; color: #3b1d58 !important; background: white !important; }}
/* Blue Pod */
.stTabs [data-baseweb="tab"]:nth-of-type(2) {{ border: 2px solid #3b82f6 !important; background-color: #f0f7ff !important; color: #1e3a8a !important; }}
/* Green Pod */
.stTabs [data-baseweb="tab"]:nth-of-type(3) {{ border: 2px solid #22c55e !important; background-color: #f0fdf4 !important; color: #064e3b !important; }}
/* Orange Pod */
.stTabs [data-baseweb="tab"]:nth-of-type(4) {{ border: 2px solid #f97316 !important; background-color: #fffaf5 !important; color: #7c2d12 !important; }}
/* Purple Pod */
.stTabs [data-baseweb="tab"]:nth-of-type(5) {{ border: 2px solid #a855f7 !important; background-color: #faf5ff !important; color: #4c1d95 !important; }}
/* Red Pod */
.stTabs [data-baseweb="tab"]:nth-of-type(6) {{ border: 2px solid #ef4444 !important; background-color: #fef2f2 !important; color: #7f1d1d !important; }}
/* Digital Pool Tab */
.stTabs [data-baseweb="tab"]:nth-of-type(7) {{ border: 2px solid #0f766e !important; color: #0f766e !important; background: #ccfbf1 !important; }}

/* ACTIVE STATE - The "Full Glow" (No flat bottom border) */
.stTabs [aria-selected="true"] {{ 
    background-color: #ffffff !important;
    transform: translateY(-4px) !important; /* Removed the scale(1.05) so it matches cards perfectly */
    box-shadow: 0 10px 20px rgba(99, 48, 148, 0.25) !important; 
}}

/* TAB ACTION BUTTONS (Top Right - Initialize / Sync) */
div.tab-action-btn {{
    display: flex;
    justify-content: flex-end;
    width: 100%;
    margin-top: 0px !important;
}}
div.tab-action-btn > div > button {{
    height: 32px !important;
    padding: 0 24px !important; /* Slightly wider as requested */
    font-size: 13px !important;
    border-radius: 20px !important;
    border: 1.2px solid #633094 !important;
    background-color: transparent !important;
    color: #633094 !important;
    font-weight: 700 !important;
    transition: all 0.2s ease-in-out !important;
    white-space: nowrap !important; 
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
}}
div.tab-action-btn > div > button:hover {{
    background-color: #633094 !important;
    color: white !important;
    box-shadow: 0 2px 8px rgba(99, 48, 148, 0.3) !important;
}}

/* PRIMARY & SECONDARY BUTTONS */
button[kind="primary"] {{
    background-color: #76bc21 !important;
    color: white !important;
    height: 3.5rem !important;
    font-size: 1.2rem !important;
    font-weight: 800 !important;
    border: none !important;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
    transition: all 0.2s ease !important;
}}

button[kind="secondary"] {{
    background-color: #ffffff !important;
    color: {TB_PURPLE} !important;
    border: 2px solid {TB_PURPLE} !important;
    height: 42px !important;
    font-size: 0.9rem !important;
    font-weight: 800 !important;
    border-radius: 8px !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
    transition: all 0.2s ease !important;
}}

/* =========================================
   1. SCISSORS BUTTON (INSIDE EXPANDER)
   ========================================= */
div[data-testid="stExpander"] div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-child(2) button {{
    margin-top: 2px !important;
    transform: scale(1.1) !important;
    transform-origin: center right !important;
    padding: 0 6px !important;
    border: none !important;
    box-shadow: none !important;
    background: transparent !important;
    color: #ef4444 !important;
    font-weight: 900 !important;
    font-size: 26px !important;
    line-height: 1 !important;
}}

/* =========================================
   2. REVOKE / RE-ROUTE BUTTON — small pill
   ========================================= */
/* 📌 Revoke/re-route popover — the ↩️ emoji IS the button.
   Streamlit's actual DOM (verified live, Apr 27 2026):
     stPopover > div(unnamed) > button[stPopoverButton] > div(flex) > [label][caret]
   So we use `button[data-testid="stPopoverButton"]` (descendant, not direct-child) and
   target the caret via [data-testid="stIconMaterial"]. The :has() guard keeps the
   styling scoped to the btn_col next to an expander column — leaves the standalone
   "Confirm Field Nation Revocation" popover untouched. */

/* Strip the popover wrapper + its unnamed inner div of any chrome/spacing. */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) div[data-testid="stPopover"],
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) div[data-testid="stPopover"] > div {{
    padding: 0 !important;
    margin: 0 !important;
    background: transparent !important;
    border: none !important;
    width: auto !important;
    box-shadow: none !important;
}}

/* The button itself: wipe Streamlit defaults, size to the emoji glyph. */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] {{
    all: unset !important;
    cursor: pointer !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    width: auto !important;
    height: auto !important;
    min-width: 0 !important;
    min-height: 0 !important;
    padding: 0 !important;
    margin: 4px 0 0 0 !important;
    font-size: 18px !important;
    line-height: 1 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    border-radius: 0 !important;
    transition: transform 0.15s ease !important;
}}

/* Strip every nested div/span/p inside the button — these carry padding/gap. */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] div,
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] span,
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] p {{
    padding: 0 !important;
    margin: 0 !important;
    gap: 0 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    line-height: 1 !important;
}}

/* Kill the dropdown caret (Streamlit renders it as <span data-testid="stIconMaterial">expand_more</span>). */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] [data-testid="stIconMaterial"],
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] svg {{
    display: none !important;
}}
/* And collapse the empty caret-container div so it doesn't reserve gap space. */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"] > div > div:last-child {{
    display: none !important;
}}

/* Hover: scale only — no pill, no border, no shadow, no purple. */
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"]:hover,
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"]:focus,
div[data-testid="stHorizontalBlock"]:has(> div[data-testid="stColumn"]:nth-child(1) div[data-testid="stExpander"]) > div[data-testid="stColumn"]:nth-child(2) button[data-testid="stPopoverButton"]:active {{
    transform: scale(1.25) !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    color: inherit !important;
    outline: none !important;
}}

/* Main Expander Container */
div[data-testid="stExpander"] {{ 
    border: 1px solid #cbd5e1 !important; 
    border-radius: 10px !important; 
    box-shadow: 0 2px 4px rgba(0,0,0,0.02) !important;
    margin-bottom: 8px !important;
    background-color: #ffffff !important;
    overflow: hidden !important;
}}

/* Header text color */
div[data-testid="stExpander"] details summary p {{ 
    color: #000000 !important; 
    font-weight: 800 !important; 
    font-size: 0.85rem !important;
}}

/* 🚀 FIX: STOP THE DARK HOVER & BLACK CLICK FILL */
div[data-testid="stExpander"] details summary {{
    background-color: #ffffff !important; /* Force base color */
    transition: background-color 0.2s ease !important;
}}

div[data-testid="stExpander"] details summary:hover {{
    background-color: #fcfaff !important; /* Very light purple on hover */
}}

/* This targets the exact moment you click it */
div[data-testid="stExpander"] details summary:active {{
    background-color: #ffffff !important; 
}}

/* This removes the "Black/Gray Box" focus state that stays after clicking */
div[data-testid="stExpander"] details summary:focus, 
div[data-testid="stExpander"] details summary:focus-visible {{
    background-color: #ffffff !important;
    outline: none !important;
    box-shadow: none !important;
}}

/* Ensure the text stays visible during the click */
div[data-testid="stExpander"] details summary:hover p,
div[data-testid="stExpander"] details summary:active p,
div[data-testid="stExpander"] details summary:focus p {{
    color: #633094 !important;
}}

label, div[data-testid="stWidgetLabel"] p {{ color: #000000 !important; font-weight: 600 !important; }}

/* MAP & FOLIUM */
iframe[title="streamlit_folium.st_folium"] {{
    border-radius: 15px !important;
    box-shadow: 0 4px 6px rgba(0,0,0,0.05) !important;
}}
.stFolium {{ background: transparent !important; }}

/* =========================================
   UNIFIED HOVER & CLICK EFFECTS
   ========================================= */

/* 1. BUTTONS: Lift + Purple Glow */
button[kind="primary"]:hover,
button[kind="secondary"]:hover,
div.refresh-btn-container > div > button:hover {{
    transform: translateY(-4px) !important;
    box-shadow: 0 12px 28px rgba(99, 48, 148, 0.35) !important;
    border-color: #633094 !important;
    z-index: 10;
}}

/* 2. CARDS, TABS & EXPANDERS: Lift + Neutral Drop Shadow (No Purple) */
div[data-testid="stExpander"]:hover,
.pod-card-pill:hover,
.dashboard-supercard:hover,
.stTabs [data-baseweb="tab"]:hover {{
    transform: translateY(-4px) !important;
    box-shadow: 0 12px 24px rgba(0, 0, 0, 0.08) !important; 
    z-index: 10;
}}

/* 3. STRICT CLICK ANIMATION (Kills the "Push In" effect) */
/* Forces all elements to just drop back to baseline smoothly when clicked */
button[kind="primary"]:active,
button[kind="secondary"]:active,
div.refresh-btn-container > div > button:active,
div[data-testid="stExpander"] details summary:active,
.pod-card-pill:active,
.dashboard-supercard:active,
.stTabs [data-baseweb="tab"]:active {{
    transform: translateY(0px) scale(1) !important; 
    box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
}}

/* Smooth transitions for everything */
div[data-testid="stExpander"],
div[data-testid="stExpander"] details summary,
.pod-card-pill,
.dashboard-supercard,
button[kind="primary"],
button[kind="secondary"],
div.refresh-btn-container > div > button,
.stTabs [data-baseweb="tab"] {{
    transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1) !important;
}}

/* =========================================
   SUB-TAB PILL STYLING (Column-Targeting Method)
   ========================================= */

/* 🌟 SIZE OVERRIDES for both Dispatch + Awaiting columns.
   Default Streamlit tabs were too padded to fit 4 pills inside a half-page column at
   100% zoom — text overflow forced horizontal scroll arrows on the sides. Tighter
   padding, smaller font, no-wrap, and hidden scroll arrows put them all on one line. */
div[data-testid="stColumn"] div[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    overflow-x: hidden !important;
    flex-wrap: nowrap !important;
    gap: 6px !important;
    padding: 8px !important;
}}
div[data-testid="stColumn"] div[data-testid="stTabs"] [data-baseweb="tab"] {{
    padding: 6px 16px !important;
    min-width: 0 !important;
    flex-shrink: 1 !important;
    flex-basis: auto !important;
}}
div[data-testid="stColumn"] div[data-testid="stTabs"] [data-baseweb="tab"] p {{
    font-size: 13px !important;
    white-space: nowrap !important;
    margin: 0 !important;
}}
/* Hide the < > scroll buttons that show up when content overflows */
div[data-testid="stColumn"] div[data-testid="stTabs"] button[role="button"][aria-label*="scroll"],
div[data-testid="stColumn"] div[data-testid="stTabs"] [data-baseweb="tab-border"] + div > button {{
    display: none !important;
}}

/* --- LEFT COLUMN: Dispatch Tabs --- */
/* 1. Ready (Green) */
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(1) {{
    background-color: #dcfce7 !important;
    border: 2.5px solid #166534 !important;
}}
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(1) p {{
    color: #166534 !important; 
}}

/* 2. Flagged (Red) */
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(2) {{
    background-color: #fee2e2 !important;
    border: 2.5px solid #991b1b !important;
}}
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(2) p {{
    color: #991b1b !important; 
}}

/* 3. Field Nation (Light Yellow BG / Dark Yellow Text) */
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(3) {{
    background-color: #fef9c3 !important;
    border: 2.5px solid #854d0e !important;
    border-radius: 30px !important;
    margin: 0 5px !important;
}}
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(3) p {{
    color: #854d0e !important;
    font-weight: 800 !important;
}}

/* 4. Digital (Teal - Left Column) */
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(4) {{
    background-color: #ccfbf1 !important;
    border: 2.5px solid #0f766e !important;
    border-radius: 30px !important;
    margin: 0 5px !important;
}}
div[data-testid="stColumn"]:nth-child(1) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(4) p {{
    color: #0f766e !important;
    font-weight: 800 !important;
}}

/* --- RIGHT COLUMN: Awaiting Tabs --- */
/* Force the gap, center the pills, and stop stretching */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab-list"] {{
    gap: 12px !important;
    justify-content: center !important; 
}}

/* 🌟 RESTORE PILL SIZE: Inherit global sizing but prevent stretching */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"] {{
    flex-grow: 0 !important; /* Kills the stretching bloat */
}}

div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"] p {{
    white-space: nowrap !important;
    font-weight: 800 !important; /* Matches left column boldness */
}}

/* 1. Sent (Purple/Blue) */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(1) {{
    background-color: #f3e8ff !important;
    border: 2.5px solid #633094 !important;
    border-radius: 30px !important;
}}
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(1) p {{
    color: #633094 !important; 
}}

/* 2. Accepted (Green) */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(2) {{
    background-color: #dcfce7 !important;
    border: 2.5px solid #166534 !important;
    border-radius: 30px !important;
}}
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(2) p {{
    color: #166534 !important; 
}}

/* 3. Declined (Red) */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(3) {{
    background-color: #fee2e2 !important;
    border: 2.5px solid #991b1b !important;
    border-radius: 30px !important;
}}
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(3) p {{
    color: #991b1b !important; 
}}

/* 4. Finalized (Orange) */
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(4) {{
    background-color: #fffaf5 !important;
    border: 2.5px solid #f97316 !important;
    border-radius: 30px !important;
}}
div[data-testid="stColumn"]:nth-child(2) div[data-testid="stTabs"] [data-baseweb="tab"]:nth-of-type(4) p {{
    color: #7c2d12 !important; 
}}

/* ALIGN COLUMNS AT THE TOP (Fixes the giant gap on the left) */
div[data-testid="stHorizontalBlock"] {{ align-items: flex-start !important; }}

/* TIGHTEN GAPS BETWEEN CARDS */
div[data-testid="stVerticalBlock"] {{ gap: 1rem !important; }}

/* Stop remover multiselect — compact, same size as expansion rows */
div[data-testid="stMultiSelect"] {{
    font-size: 11px !important;
}}
div[data-testid="stMultiSelect"] [data-baseweb="select"] > div {{
    min-height: 32px !important;
    font-size: 11px !important;
    padding: 2px 6px !important;
}}
div[data-testid="stMultiSelect"] [data-baseweb="tag"] {{
    font-size: 10px !important;
    height: 20px !important;
    padding: 0 6px !important;
}}



/* Collapse gap between consecutive stop row columns inside expanders */
div[data-testid="stExpander"] div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] + div[data-testid="stHorizontalBlock"] {{
    margin-top: -14px !important;
}}

div[data-testid="stExpander"] {{ margin-top: 0px !important; margin-bottom: 2px !important; }}

/* Compact button inside revoke/re-route popover. The popover panel is portaled to a
   body-level [data-baseweb="popover"] layer, so we target that. Keeps the green button
   short instead of towering over the two-line prompt. */
[data-baseweb="popover"] button[kind="primary"] {{
    height: 34px !important;
    min-height: 34px !important;
    padding: 4px 12px !important;
    font-size: 13px !important;
    line-height: 1 !important;
    border-radius: 8px !important;
}}

/* MINI REVOKE BUTTON (Single Line, Right Aligned) */
div.mini-btn button {{
    height: 30px !important;
    min-height: 30px !important;
    padding: 0 8px !important;
    font-size: 11px !important;
    white-space: nowrap !important; /* CRITICAL: Stops "Revoke" from dropping to a second line */
    float: right !important;
    margin-top: 4px !important;
    border-radius: 4px !important;
}}


@media (max-width: 768px) {{
    div[data-testid="stHorizontalBlock"] {{ flex-direction: column !important; }}
    div[data-testid="stColumn"] {{ width: 100% !important; min-width: 100% !important; flex: 1 1 100% !important; }}
    .main .block-container {{ padding-left: 0.5rem !important; padding-right: 0.5rem !important; padding-top: 0.5rem !important; }}
    .stTabs [data-baseweb="tab"] {{ padding: 6px 10px !important; font-size: 11px !important; border-radius: 20px !important; }}
    iframe[title="streamlit_folium.st_folium"] {{ height: 250px !important; }}
    .dashboard-supercard {{ height: auto !important; margin-bottom: 8px !important; }}
    div[data-testid="stExpander"] details summary p {{ font-size: 0.75rem !important; }}
    h1 {{ font-size: 1.3rem !important; }}
    h2 {{ font-size: 1.1rem !important; }}
}}
</style>
""", unsafe_allow_html=True)

# --- 1. BACKGROUND THREAD WORKER ---
def background_sheet_move(cluster_hash, payload_json, task_ids=None, action_label="Revoked", ic_name=""):
    """Silent worker to update Google Sheets AND scrub Onfleet — never blocks the UI.

    Apr 27 2026 — also stamps action_label + ic_name into the GAS archiveRoute payload
    so the Ready-card history banner can recover Revoked/Re-Routed events after a
    session reset (was previously session-only via st.session_state[\'_actions_*\'])."""
    try:
        requests.post(GAS_WEB_APP_URL, json={
            "action": "archiveRoute",
            "cluster_hash": cluster_hash,
            "taskIds": ",".join(task_ids) if task_ids else "",  # Fallback for hash mismatch
            "payload": payload_json if payload_json else {},
            "action_label": action_label,
            "ic_name": ic_name,
        }, timeout=15)
    except Exception as e:
        _log_err("background_sheet_move/archive", e)

    # Onfleet scrub: actually UNASSIGN the worker now (was a no-op GET previously).
    # Sets worker=null and clears WO/PAY metadata so the task returns to the team pool.
    if task_ids:
        try:
            auth = {"Authorization": f"Basic {base64.b64encode(f'{ONFLEET_KEY}:'.encode()).decode()}"}
            scrub_payload = json.dumps({"worker": None, "metadata": []})
            # Apr 27 2026 — bookend the scrub loop with start/end log lines so we can
            # tell if the loop is running at all (silent no-PUT vs. PUTs returning 200).
            _scrub_total = len(task_ids)
            _scrub_ok = 0
            _log_err("background_sheet_move/scrub-start", f"scrubbing {_scrub_total} tasks for cluster {cluster_hash}")
            for tid in task_ids:
                try:
                    _r = requests.put(
                        f"https://onfleet.com/api/v2/tasks/{tid}",
                        headers={**auth, "Content-Type": "application/json"},
                        data=scrub_payload,
                        timeout=10,
                    )
                    if _r.status_code == 200:
                        _scrub_ok += 1
                    else:
                        _body = ""
                        try: _body = _r.text[:300]
                        except Exception: _body = ""
                        _log_err(f"background_sheet_move/scrub task={tid}", f"HTTP {_r.status_code}: {_body}")
                except Exception as e:
                    _log_err(f"background_sheet_move/scrub task={tid}", e)
            _log_err("background_sheet_move/scrub-end", f"scrubbed {_scrub_ok}/{_scrub_total} successfully for cluster {cluster_hash}")
        except Exception as e:
            _log_err("background_sheet_move/scrub-outer", e)
        
# --- 2. INSTANT REVOKE LOGIC ---
def background_fn_revoke(cluster_hash):
    """Silently removes a route from the Field Nation tab in Google Sheets."""
    try:
        requests.post(GAS_WEB_APP_URL, json={
            "action": "removeFieldNation",
            "cluster_hash": cluster_hash
        }, timeout=15)
    except Exception as e:
        _log_err("background_fn_revoke", e)

def _onfleet_get_state(tid, auth_header):
    """GET an Onfleet task and return (tid, is_completed). Defaults to NOT completed
    on any error so we err on the side of unassigning (safer for the dispatcher)."""
    try:
        r = requests.get(f"https://onfleet.com/api/v2/tasks/{tid}", headers=auth_header, timeout=4)
        if r.status_code == 200:
            t = r.json()
            # Onfleet task state: 0=Unassigned, 1=Assigned, 2=Active, 3=Completed
            is_done = (t.get('state') == 3) or bool((t.get('completionDetails') or {}).get('success'))
            return (tid, is_done)
    except Exception as e:
        _log_err(f"_onfleet_get_state task={tid}", e)
    return (tid, False)


def move_to_dispatch(cluster_hash, ic_name, pod_name, action_label="Revoked", check_onfleet=True, cluster_data=None, check_completed=False):
    """Moves route to Dispatch column instantly. Sheet update + Onfleet scrub run in background.

    Apr 27 2026 — completion-aware unassign (gated by check_completed=True):
      Sent/Declined tasks are still in Onfleet state=0 (unassigned), so the old behavior
      is correct for them — pass check_completed=False (default) and every task_id is
      unassigned, with a simple toast.
      Accepted/Finalized routes have tasks actively assigned to a contractor that may
      be partially complete. Pass check_completed=True from those buttons; we then GET
      each task in parallel and skip any with state=3 / completionDetails.success.
      Completed work stays attributed to the contractor; only outstanding tasks are
      returned to the pool. The toast reports the outstanding count + city/state."""

    # 1. Parse all task IDs from cluster_data (str CSV or list of dicts).
    all_task_ids = []
    if check_onfleet and cluster_data:
        try:
            raw = cluster_data.get('taskIds', '') or cluster_data.get('data', [])
            if isinstance(raw, str):
                all_task_ids = [t.strip() for t in raw.split(',') if t.strip()]
            elif isinstance(raw, list):
                all_task_ids = [str(t['id']).strip() for t in raw if t.get('id')]
        except Exception as e:
            _log_err("move_to_dispatch/task_ids-parse", e)
            all_task_ids = []

    # 2. Pick which task_ids actually get unassigned.
    #    - check_completed=False → unassign all (Sent/Declined behavior, unchanged).
    #    - check_completed=True  → parallel GET, skip any task already completed.
    outstanding_ids = list(all_task_ids)
    completed_count = 0
    if check_completed and all_task_ids:
        try:
            auth = {"Authorization": f"Basic {base64.b64encode(f'{ONFLEET_KEY}:'.encode()).decode()}"}
            with ThreadPoolExecutor(max_workers=min(10, len(all_task_ids))) as ex:
                results = list(ex.map(lambda tid: _onfleet_get_state(tid, auth), all_task_ids))
            outstanding_ids = []
            for tid, is_done in results:
                if is_done:
                    completed_count += 1
                else:
                    outstanding_ids.append(tid)
        except Exception as e:
            _log_err("move_to_dispatch/state-check", e)
            outstanding_ids = list(all_task_ids)  # fallback: unassign everything

    # 3. Onfleet unassign — SYNCHRONOUS parallel PUTs on the main thread. The daemon-
    # thread version was failing silently (Streamlit thread context issues, no Railway
    # log output), so the worker kept seeing routes after revoke. This runs the PUTs
    # inline; toast at the bottom of this function reflects the real result.
    if outstanding_ids:
        try:
            _put_auth = {"Authorization": f"Basic {base64.b64encode(f'{ONFLEET_KEY}:'.encode()).decode()}", "Content-Type": "application/json"}
            _put_payload = json.dumps({"worker": None, "metadata": []})
            def _do_put(tid):
                try:
                    r = requests.put(f"https://onfleet.com/api/v2/tasks/{tid}", headers=_put_auth, data=_put_payload, timeout=8)
                    if r.status_code != 200:
                        _log_err(f"move_to_dispatch/unassign task={tid}", f"HTTP {r.status_code}: {r.text[:200]}")
                    return (tid, r.status_code)
                except Exception as e:
                    _log_err(f"move_to_dispatch/unassign task={tid}", e)
                    return (tid, -1)
            with ThreadPoolExecutor(max_workers=min(10, len(outstanding_ids))) as _ex:
                _put_results = list(_ex.map(_do_put, outstanding_ids))
        except Exception as e:
            _log_err("move_to_dispatch/unassign-outer", e)

    # 4. Fire-and-forget the SHEET archival to GAS in a background thread (no Onfleet
    # work — pass task_ids=None so background_sheet_move skips its own scrub block).
    threading.Thread(
        target=background_sheet_move,
        args=(cluster_hash, cluster_data, None, action_label, ic_name),
        daemon=True
    ).start()

    # 5. 🛡️ Set reverted flag so UI ignores stale Sheet record immediately
    st.session_state[f"reverted_{cluster_hash}"] = True

    # 6. 🧠 INSTANT RESET: Clear all state for this route
    st.session_state.pop(f"route_state_{cluster_hash}", None)
    st.session_state.pop(f"sent_ts_{cluster_hash}", None)
    st.session_state.pop(f"contractor_{cluster_hash}", None)
    st.session_state.pop(f"sync_{cluster_hash}", None)
    st.session_state.pop(f"scrub_timer_{cluster_hash}", None)

    # 📜 Record this action so the dispatch card can show "Revoked / Re-Routed"
    # context the next time this same route surfaces in the Ready pool.
    try:
        from datetime import datetime as _dt
        _now_str = _dt.now().strftime('%m/%d %I:%M %p')
        _key = f'_actions_{cluster_hash}'
        st.session_state.setdefault(_key, []).append({
            'action': action_label,
            'ic': ic_name,
            'time': _now_str,
            'ts': _dt.now(),
        })
    except Exception as _e:
        _log_err('move_to_dispatch/action-record', _e)

    # 6. Toast — completion-aware variant only when check_completed=True.
    if check_completed:
        _city = (cluster_data.get('city') if cluster_data else '') or ''
        _state = (cluster_data.get('state') if cluster_data else '') or ''
        _header = (f"{_city} {_state}").strip() or "Dispatch"
        n_out = len(outstanding_ids)
        if n_out > 0:
            _plural = 's' if n_out != 1 else ''
            _kept = f" ({completed_count} completed kept)" if completed_count else ""
            st.toast(f"✅ {n_out} Task{_plural} Unassigned from \"{ic_name}\": {_header}{_kept}")
        elif completed_count > 0:
            st.toast(f"✅ Route removed from \"{ic_name}\" — all {completed_count} tasks already completed.")
        else:
            st.toast(f"✅ {action_label}! Route moved back to Dispatch.")
    else:
        st.toast(f"✅ {action_label}! Route moved back to Dispatch.")
    # No st.rerun() — callback handles the rerender

@st.fragment(run_every=15)
def auto_sync_checker(pod_name):
    """Polls every 15s. Refreshes the sheet cache and triggers a rerun whenever
    any sheet content changed — not just Accepted/Declined status flips. Previously
    new routes appearing in Saved_Routes (or comp/due updates) wouldn't reflect
    until a user interaction (zooming the map, clicking somewhere, etc.) forced
    a rerender."""
    pod_clusters = st.session_state.get(f"clusters_{pod_name}", [])
    if not pod_clusters:
        return

    pod_tid_set = set()
    for c in pod_clusters:
        for t in c.get('data', []):
            pod_tid_set.add(str(t['id']).strip())

    if not pod_tid_set:
        return

    try:
        # Force-refresh the cached sheet pull. The cached function has a 15s TTL but
        # only re-runs when something actively calls it — and nothing else does between
        # user interactions. By clearing here we guarantee the next call is fresh.
        fetch_sent_records_from_sheet.clear()
        fresh_sent_db, _, _archived_wos, _history_db = fetch_sent_records_from_sheet()
        st.session_state['_history_db'] = _history_db

        # Build a fingerprint of the fields the cards actually display, restricted to
        # tasks in THIS pod. If the fingerprint differs from last poll, rerun the page.
        # This catches new routes, comp updates, due-date changes, status flips — any
        # sheet edit that would change what the dispatcher sees.
        def _fp(db):
            parts = []
            for tid in sorted(pod_tid_set):
                rec = db.get(tid)
                if not rec:
                    continue
                # 📌 Removed `time` from fingerprint: it ticks on every sheet write
                # (even no-op edits), causing spurious reruns. Status/WO/comp/due are the
                # only fields the cards display, so changes outside those don't matter.
                parts.append(f"{tid}|{rec.get('status','')}|{rec.get('wo','')}|{rec.get('comp','')}|{rec.get('due','')}")
            return hashlib.md5("\n".join(parts).encode()).hexdigest()

        new_fp = _fp(fresh_sent_db)
        last_fp_key = f"_auto_sync_fp_{pod_name}"
        last_fp = st.session_state.get(last_fp_key)

        # First poll: just record the baseline, don't rerun (avoids a rerun storm on init).
        if last_fp is None:
            st.session_state[last_fp_key] = new_fp
            st.session_state.sent_db = fresh_sent_db
            st.session_state['archived_wos'] = _archived_wos
            return

        if new_fp != last_fp:
            st.session_state[last_fp_key] = new_fp
            st.session_state.sent_db = fresh_sent_db
            st.session_state['archived_wos'] = _archived_wos
            # Clear reverted flags for any cluster whose tasks just got new sheet data,
            # so the traffic cop in run_pod_tab picks up the fresh status.
            for _c in pod_clusters:
                _c_tids = [str(t['id']).strip() for t in _c.get('data', [])]
                if any(tid in fresh_sent_db for tid in _c_tids):
                    _c_hash = hashlib.md5("".join(sorted(_c_tids)).encode()).hexdigest()
                    if st.session_state.get(f"reverted_{_c_hash}", False):
                        # Only clear if the sheet now reflects a non-revoked state
                        for _tid in _c_tids:
                            if _tid in fresh_sent_db:
                                st.session_state[f"reverted_{_c_hash}"] = False
                                break
            st.rerun(scope="app")

    except Exception as e:
        _log_err("auto_sync_checker", e)

@st.fragment
def render_finalization_checklist(cluster_hash, pod_name, prefix="chk"):
    """Isolates checkbox reruns so the whole page doesn't reload, making checks instant."""
    st.markdown("<p style='font-size: 13px; font-weight: 600;'>Finalization Checklist:</p>", unsafe_allow_html=True)
    cc1, cc2, cc3 = st.columns(3)
    chk1 = cc1.checkbox("Optimized Route in OnFleet.", key=f"{prefix}1_{cluster_hash}_{pod_name}")
    chk2 = cc2.checkbox("Dispatched in Route Planning.", key=f"{prefix}2_{cluster_hash}_{pod_name}")
    chk3 = cc3.checkbox("Packing list created.", key=f"{prefix}3_{cluster_hash}_{pod_name}")
    
    if chk1 and chk2 and chk3:
        if st.button("🏁 Finalize Route", key=f"finbtn_{prefix}_{cluster_hash}_{pod_name}", type="primary", use_container_width=True):
            # 1. 🚀 SYNCHRONOUS SHEET UPDATE
            with st.spinner("Archiving to Google Sheets..."):
                try:
                    res = requests.post(GAS_WEB_APP_URL, json={"action": "finalizeRoute", "cluster_hash": cluster_hash}, timeout=15)
                    res_data = res.json() # 🌟 Parse the response!
                    
                    if not res_data.get("success"):
                        st.error(f"Google Sheets Error: {res_data.get('error')}")
                        st.stop() # 🚨 HALT EXECUTION! Do not hide the card if the database failed.
                except Exception as e:
                    st.error(f"Failed to connect to Google Sheets: {e}")
                    st.stop() # 🚨 HALT EXECUTION!
            
            # 2. 🧠 INSTANT UI OVERRIDE (Only runs if Google Sheets confirmed the move!)
            st.session_state[f"route_state_{cluster_hash}"] = "finalized"
            st.session_state[f"reverted_{cluster_hash}"] = True 
            
            st.toast("🏁 Route Finalized! Moving to Finalized tab...")
            st.rerun(scope="app")
        

    
def instant_revoke_handler(cluster_hash, ic_name, payload_json, pod_name):
    # We now enable Onfleet scrubbing (State 0 check) immediately
    move_to_dispatch(cluster_hash, ic_name, pod_name, action_label="Revoked", check_onfleet=True, cluster_data=payload_json)

def revoke_field_nation(cluster_hash, pod_name):
    """Removes route from Field Nation sheet tab AND resets UI state.

    Fires two background calls intentionally:
      1. removeFieldNation — explicit FN tab cleanup (handler in GAS)
      2. archiveRoute (via move_to_dispatch) — moves the row to Archive
    The archiveRoute call would already remove the FN row, but the explicit
    removeFieldNation makes intent legible if you ever wire up FN-only flows
    that shouldn't archive.
    """
    import threading
    threading.Thread(target=background_fn_revoke, args=(cluster_hash,), daemon=True).start()
    move_to_dispatch(cluster_hash, "Field Nation", pod_name, action_label="Field Nation Revoked", check_onfleet=True)

# --- FIELD NATION MASS UPLOAD GENERATOR ---

from fn_utils import FN_STATE_MANAGER, generate_fn_upload, save_fn_to_sheet

# --- UTILITIES ---
def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def normalize_state(st_str):
    if not st_str: return "UNKNOWN"
    clean = str(st_str).strip().upper()
    return STATE_MAP.get(clean, clean)

@st.cache_data(ttl=15, show_spinner=False)
def fetch_sent_records_from_sheet():
    """
    Returns: (sent_dict, ghost_routes, archived_wos)

    archived_wos was previously written to st.session_state from inside this cached
    function. That meant on cache hits the function body was skipped and archived_wos
    never refreshed even though the cached return looked fresh. Now returned in the
    tuple so callers must explicitly assign it after each call.
    """
    try:
        # Use safe extraction to prevent KeyError/AttributeError
        if not isinstance(IC_SHEET_URL, str):
            raise ValueError("IC_SHEET_URL must be a string. Check for trailing commas!")
            
        base_url = f"{IC_SHEET_URL.split('/edit')[0]}/export?format=csv&gid="
        
        sheets_to_fetch = [
            (SAVED_ROUTES_GID, "sent"),
            (FIELD_NATION_GID, "field_nation"),
            (DECLINED_ROUTES_GID, "declined"),
            (ACCEPTED_ROUTES_GID, "accepted"),
            (FINALIZED_ROUTES_GID, "finalized"),
        ]

        
        sent_dict = {}
        # 🌟 THE FIX: Add Global_Digital to the dictionary!
        ghost_routes = {"Blue": [], "Green": [], "Orange": [], "Purple": [], "Red": [], "Global_Digital": [], "UNKNOWN": []}
        # 📜 HISTORY DB: per-task event list, in chronological order across every
        # sheet tab a task ever appeared in. Used by render_dispatch to show "declined by X",
        # "removed from Field Nation on Y", etc. on Ready cards so dispatchers see the
        # route's prior journey instead of a blank slate.
        history_db = {}  # tid -> list of {status, name, time, wo, raw_ts}
        
        for gid, status_label in sheets_to_fetch:
            try:
                # Ensure gid is cast to string just in case it's an integer
                df = pd.read_csv(base_url + str(gid))
                df.columns = [str(c).strip().lower() for c in df.columns]
                
                if 'json payload' in df.columns:
                    for _, row in df.iterrows():
                        try:
                            p = json.loads(row['json payload'])
                            tids = str(p.get('taskIds', '')).replace('|', ',').split(',')
                            c_name = str(row.get('contractor', 'Unknown Contractor'))
                            
                            raw_ts = row.get('date created', '')
                            ts_display = ""
                            dt_obj = None  # 📌 reset per row so the history append never
                                            # picks up a stale timestamp from a previous iteration.
                            
                            # Filter out routes older than the migration cutoff (see top-of-file constant).
                            if pd.notna(raw_ts) and str(raw_ts).strip():
                                try:
                                    dt_obj = pd.to_datetime(raw_ts)
                                    if dt_obj < pd.to_datetime(MIGRATION_CUTOFF_DATE):
                                        continue 
                                    ts_display = dt_obj.strftime('%m/%d %I:%M %p')
                                except:
                                    ts_display = str(raw_ts)
                            else:
                                continue # Skip empty date rows just to be safe
                            
                            # 1. Live Task Matching
                            for tid in tids:
                                tid = tid.strip()
                                if tid:
                                    # 1. Enforce Contractor name for FN routes
                                    display_name = "Field Nation" if status_label == "field_nation" else c_name
                                    sent_dict[tid] = {
                                        "name": display_name, 
                                        "status": status_label,
                                        "time": ts_display,
                                        "wo": p.get('wo', display_name),
                                        "comp": p.get('comp', 0),     
                                        "due": p.get('due', 'N/A')    
                                    }
                                    # 📜 Record this row as a history event for the task.
                                    # `dt_obj` (computed above) is the raw pandas Timestamp we can sort on later.
                                    history_db.setdefault(tid, []).append({
                                        "status": status_label,
                                        "name": display_name,
                                        "time": ts_display,
                                        "wo": p.get('wo', display_name),
                                        "raw_ts": dt_obj,
                                    })
                            
                            # 🌟 THE FIX: Omni-Ghost Engine - Capture Sent routes too!
                            if status_label in ['accepted', 'finalized', 'sent']:
                                locs_str = str(p.get('locs', ''))
                                state_guess, city_guess = "UNKNOWN", "Unknown"
                                stops_list = [s.strip() for s in locs_str.split('|') if s.strip()]
                                
                                # 🌟 THE FIX: Prioritize direct payload extraction, fallback to string splitting
                                state_guess = str(p.get('state', 'UNKNOWN'))
                                city_guess = str(p.get('city', 'Unknown'))
                                
                                if state_guess == "UNKNOWN" or city_guess == "Unknown":
                                    if len(stops_list) > 1:
                                        addr_parts = stops_list[1].split(',')
                                        if len(addr_parts) >= 2:
                                            state_raw = addr_parts[-1].strip().upper()
                                            state_guess = state_raw.split(' ')[0] 
                                            city_guess = addr_parts[-2].strip()
                                    elif len(stops_list) == 1:
                                        addr_parts = stops_list[0].split(',')
                                        if len(addr_parts) >= 2:
                                            state_raw = addr_parts[-1].strip().upper()
                                            state_guess = state_raw.split(' ')[0] 
                                            city_guess = addr_parts[-2].strip()
                                
                                # 🌟 THE FIX: ALWAYS define norm_state outside the if/else block!
                                norm_state = STATE_MAP.get(state_guess, state_guess)
                                
                                is_digital_ghost = False
                                if tids and tids[0].strip() in sent_dict:
                                    is_digital_ghost = sent_dict[tids[0].strip()].get('is_digital', False)
                                    
                                if not is_digital_ghost:
                                    job_only = str(p.get('jobOnly', ''))
                                    is_digital_ghost = any(trigger in job_only.lower() for trigger in ['🔌', '🔧', '⚙️', '📵', 'service', 'offline', 'ins/rem'])
                                
                                pod_name = "UNKNOWN"
                                if is_digital_ghost:
                                    pod_name = "Global_Digital"
                                else:
                                    for p_name, p_config in POD_CONFIGS.items():
                                        if norm_state in p_config['states']:
                                            pod_name = p_name
                                            break
                                
                                if pod_name != "UNKNOWN":
                                    ghost_hash = p.get("cluster_hash")
                                    if not ghost_hash:
                                        clean_tids = [str(t).strip() for t in tids if str(t).strip()]
                                        ghost_hash = hashlib.md5("".join(sorted(clean_tids)).encode()).hexdigest()
                                    # Parse rich stop data if available
                                    try:
                                        stop_data = json.loads(p.get("stopData", "[]"))
                                    except:
                                        stop_data = []

                                    ghost_routes[pod_name].append({
                                        "contractor_name": c_name,
                                        "route_ts": ts_display,
                                        "city": city_guess,
                                        "state": norm_state,
                                        "stops": p.get('lCnt', 0),
                                        "tasks": p.get('tCnt', len(tids)),
                                        "pay": p.get('comp', 0),
                                        "wo": p.get('wo', c_name),
                                        "due": p.get('due', 'N/A'),
                                        "status": status_label,
                                        "hash": ghost_hash,
                                        "locs": p.get('locs', ''),
                                        "stop_data": stop_data
                                    })
                        except Exception: continue
            except Exception: continue
            
        # 🌟 ARCHIVE SIDE-CHANNEL: harvest WOs of archived routes so the WO counter
        # can move past suffixes that were "freed" when a route was archived. We also
        # harvest revoke/re-route HISTORY events so the Ready-card history banner survives
        # session resets — was previously session-only via _actions_*. Tasks/status are
        # still NEVER read into clustering — only into history_db.
        _archived_wos = set()
        try:
            _archive_df = pd.read_csv(base_url + str(ARCHIVE_GID))
            _archive_df.columns = [str(c).strip().lower() for c in _archive_df.columns]
            if 'json payload' in _archive_df.columns:
                for _, _arow in _archive_df.iterrows():
                    try:
                        _ap = json.loads(_arow['json payload'])
                        _orig = str(_ap.get('archived_wo', '') or '').strip()
                        if _orig and _orig.lower() != 'archived':
                            _archived_wos.add(_orig)
                        # History harvest: Revoked / Re-Routed / Ghost Archived events.
                        # Timestamp handling: GAS writes archive_ts via new Date().toISOString()
                        # which ends in "Z" (tz-aware UTC). pandas comparisons against the rest
                        # of history_db (tz-naive sheet "Date Created") would fail with
                        # "Cannot compare tz-naive and tz-aware timestamps" — so we explicitly
                        # parse with utc=True then tz_localize(None) to strip the tz.
                        _act = str(_ap.get('archive_action', '') or '').strip()
                        if _act in ('Revoked', 'Re-Routed', 'Ghost Archived'):
                            _ic = str(_ap.get('archive_ic', '') or _ap.get('icn', '') or '')
                            _ats = _ap.get('archive_ts', '')
                            _dt = pd.Timestamp.min
                            try:
                                if _ats:
                                    _parsed = pd.to_datetime(_ats, errors='coerce', utc=True)
                                    if pd.notna(_parsed):
                                        try:
                                            _dt = _parsed.tz_localize(None)
                                        except (TypeError, AttributeError):
                                            _dt = _parsed
                            except Exception:
                                _dt = pd.Timestamp.min
                            _ts_display = ''
                            try:
                                if pd.notna(_dt) and _dt is not pd.Timestamp.min:
                                    _ts_display = _dt.strftime('%m/%d %I:%M %p')
                            except Exception:
                                _ts_display = ''
                            _hist_status = 'revoked' if _act == 'Revoked' else ('re-routed' if _act == 'Re-Routed' else 'ghost-archived')
                            _tids_str = str(_ap.get('taskIds', ''))
                            for _tid in _tids_str.replace('|', ',').split(','):
                                _tid = _tid.strip()
                                if not _tid:
                                    continue
                                history_db.setdefault(_tid, []).append({
                                    'status': _hist_status,
                                    'name': _ic,
                                    'time': _ts_display,
                                    'wo': _orig,
                                    'raw_ts': _dt,
                                })
                    except Exception:
                        pass
        except Exception as _ae:
            _log_err("fetch_sent_records_from_sheet/archive_harvest", _ae)

        # Sort each task's events chronologically (oldest first). Defensive key: strip
        # any accidental tz from raw_ts so a single tz-aware value can\'t poison the
        # sort and bubble up to the outer "Failed to fetch portal records" catch.
        def _sort_key(e):
            ts = e.get('raw_ts') or pd.Timestamp.min
            try:
                if pd.notna(ts) and getattr(ts, 'tzinfo', None) is not None:
                    return ts.tz_localize(None)
            except Exception:
                pass
            return ts if pd.notna(ts) else pd.Timestamp.min
        for _tid, _evts in history_db.items():
            try:
                _evts.sort(key=_sort_key)
            except Exception as _se:
                _log_err(f"history_db sort tid={_tid}", _se)
        return sent_dict, ghost_routes, _archived_wos, history_db
    except Exception as e:
        st.error(f"Failed to fetch portal records: {e}")
        return {}, {}, set(), {}

# Manual session-state cache (replaces @st.cache_data) so we can cache ONLY successes.
# Previously @st.cache_data(ttl=3600) was caching the (0, 0, "0h 0m", []) failure tuple
# for an hour after any transient API hiccup — Drive Time / Round Trip would lock to
# "—" until the TTL expired. The Bundle Routes preview made this very visible because
# every preview produces a fresh cache key (different waypoint set) → fresh API call →
# any one bad call locks that bundle preview into dashes.
def get_gmaps(home, waypoints):
    _wp_tuple = tuple(waypoints) if waypoints else ()
    _cache_key = (home, _wp_tuple)
    _cache = st.session_state.setdefault('_gmaps_cache', {})
    _now = time.time()
    _entry = _cache.get(_cache_key)
    if _entry is not None and (_now - _entry[1] < 3600):
        return _entry[0]

    # Encode each waypoint so addresses containing '&', '=', '|' or other reserved
    # characters don't corrupt the query string. Google's directions API accepts
    # 'optimize:true|<wp1>|<wp2>...' as the value of the waypoints param — we URL-encode
    # each piece individually then join with literal '|'.
    from urllib.parse import quote
    enc_wp = "optimize:true|" + "|".join(quote(w, safe='') for w in waypoints)
    enc_home = quote(home, safe='')
    url = (
        "https://maps.googleapis.com/maps/api/directions/json"
        f"?origin={enc_home}&destination={enc_home}"
        f"&waypoints={enc_wp}"
        f"&departure_time=now&key={GOOGLE_MAPS_KEY}"
    )
    try:
        res = requests.get(url, timeout=15).json()
        if res.get('status') == 'OK':
            mi = sum(l['distance']['value'] for l in res['routes'][0]['legs']) * 0.000621371
            drive_hrs = sum(l['duration']['value'] for l in res['routes'][0]['legs']) / 3600
            service_hrs = len(waypoints) * (10/60)
            total_hrs = drive_hrs + service_hrs
            waypoint_order = res['routes'][0].get('waypoint_order', list(range(len(waypoints))))
            _result = (round(mi, 1), total_hrs, f"{int(total_hrs)}h {int((total_hrs * 60) % 60)}m", waypoint_order)
            _cache[_cache_key] = (_result, _now)  # only cache successes
            return _result
        else:
            _log_err("get_gmaps", f"API status: {res.get('status')} / msg: {res.get('error_message','')[:200]}")
    except Exception as e:
        _log_err("get_gmaps", e)
    return 0, 0, "0h 0m", []

@st.cache_data(ttl=120, show_spinner=False)
def fetch_worker_task_counts():
    """Fetch current assigned task count per worker from Onfleet, keyed by phone. Cached 2 min."""
    try:
        res = requests.get("https://onfleet.com/api/v2/workers?analytics=true", headers=headers, timeout=10)
        if res.status_code != 200:
            _log_err("fetch_worker_task_counts", f"HTTP {res.status_code}")
            return {}
        workers = res.json()
        counts = {}
        for w in workers:
            phone = re.sub(r'\D', '', str(w.get('phone', '')))[-10:]
            task_count = len(w.get('tasks', []))
            if phone:
                counts[phone] = task_count
        return counts
    except Exception as e:
        _log_err("fetch_worker_task_counts", e)
        return {}

@st.cache_data(ttl=600)
def load_ic_database(sheet_url):
    try:
        export_url = f"{sheet_url.split('/edit')[0]}/export?format=csv&gid=0"
        return pd.read_csv(export_url)
    except Exception as e:
        _log_err("load_ic_database", e)
        return None

def process_digital_pool(master_bar=None):
    prog_bar = master_bar if master_bar else st.progress(0)
    prog_bar.progress(0.1, text="📥 Fetching National Tasks from Onfleet...")
    # Tick digital overlay timer
    _ov = st.session_state.get('_loading_overlay')
    _st = st.session_state.get('_loading_start')
    if _ov and _st:
        import time as _t2
        _el = int(_t2.time() - _st); _m = _el // 60; _s = _el % 60
        _ov.markdown(f"""<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
.dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:36px 32px;text-align:center;margin:20px 0;}}
.dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;border-top:4px solid #0f766e;border-radius:50%;animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
.dcc-pill{{display:inline-block;font-size:13px;font-weight:700;color:#0f766e;background:#ccfbf1;border-radius:20px;padding:4px 14px;margin-top:12px;}}</style>
<div class='dcc-card'><div class='dcc-spin'></div>
<p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing Digital Pool</p>
<div class='dcc-pill'>⏱ {_m}:{_s:02d}</div></div>""", unsafe_allow_html=True)
    
    # 1. Fetch Onfleet (ONCE)
    APPROVED_TEAMS = ["a - escalation", "b - boosted campaigns", "b - local campaigns", "c - priority nationals", "cvs kiosk removal", "cvs kiosk removals", "d - digital routes", "n - national campaigns"]
    teams_res = requests.get("https://onfleet.com/api/v2/teams", headers=headers, timeout=15).json()
    target_team_ids = [t['id'] for t in teams_res if any(appr in str(t.get('name', '')).lower() for appr in APPROVED_TEAMS)]
    esc_team_ids = [t['id'] for t in teams_res if 'escalation' in str(t.get('name', '')).lower()]

    all_tasks_raw = []
    time_window = int(time.time()*1000) - (45*24*3600*1000)
    url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}"

    _MAX_PAGES = 200  # was 50; same loop-guard pattern as process_pod
    _page = 0
    _seen_last_ids = set()
    while url and _page < _MAX_PAGES:
        _page += 1
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 429:
            time.sleep(2); continue
        if response.status_code != 200: break
        res_json = response.json()
        all_tasks_raw.extend(res_json.get('tasks', []))
        _next_id = res_json.get('lastId')
        if _next_id and _next_id in _seen_last_ids:
            _log_err("process_digital_pool", f"lastId loop detected at {_next_id} (page {_page})")
            break
        if _next_id:
            _seen_last_ids.add(_next_id)
        url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}&lastId={_next_id}" if _next_id else None
        # Tick timer on every page fetch (was previously nested under the cap-hit branch
        # below, so it never updated during normal pagination — overlay sat frozen).
        _ov2 = st.session_state.get('_loading_overlay')
        _st2 = st.session_state.get('_loading_start')
        if _ov2 and _st2:
            import time as _t3
            _el2 = int(_t3.time() - _st2); _m2 = _el2 // 60; _s2 = _el2 % 60
            _pct = min(0.1 + 0.3 * (len(all_tasks_raw) / max(500, len(all_tasks_raw))), 0.39)
            prog_bar.progress(_pct, text=f"📡 Fetching tasks... {len(all_tasks_raw)} found")
            _ov2.markdown(f"""<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
.dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:36px 32px;text-align:center;margin:20px 0;}}
.dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;border-top:4px solid #0f766e;border-radius:50%;animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
.dcc-pill{{display:inline-block;font-size:13px;font-weight:700;color:#0f766e;background:#ccfbf1;border-radius:20px;padding:4px 14px;margin-top:12px;}}</style>
<div class='dcc-card'><div class='dcc-spin'></div>
<p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing Digital Pool</p>
<p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>Fetching tasks... {len(all_tasks_raw)} found</p>
<div class='dcc-pill'>⏱ {_m2}:{_s2:02d}</div></div>""", unsafe_allow_html=True)
    if _page >= _MAX_PAGES:
        _log_err("process_digital_pool", f"hit pagination cap ({_MAX_PAGES} pages)")
        st.warning(f"⚠️ Hit pagination cap of {_MAX_PAGES} pages while fetching Onfleet tasks. Some tasks may be missing.")
        
    prog_bar.progress(0.4, text="🔍 Isolating Digital Service Calls...")
    # Tick digital overlay timer
    _ov = st.session_state.get('_loading_overlay')
    _st = st.session_state.get('_loading_start')
    if _ov and _st:
        import time as _t2
        _el = int(_t2.time() - _st); _m = _el // 60; _s = _el % 60
        _ov.markdown(f"""<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
.dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:36px 32px;text-align:center;margin:20px 0;}}
.dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;border-top:4px solid #0f766e;border-radius:50%;animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
.dcc-pill{{display:inline-block;font-size:13px;font-weight:700;color:#0f766e;background:#ccfbf1;border-radius:20px;padding:4px 14px;margin-top:12px;}}</style>
<div class='dcc-card'><div class='dcc-spin'></div>
<p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing Digital Pool</p>
<div class='dcc-pill'>⏱ {_m}:{_s:02d}</div></div>""", unsafe_allow_html=True)
    
    # 🌟 STRICT DIGITAL FILTER
    # --- 🌟 STRICT DIGITAL FILTER ---
    DIGITAL_WHITELIST = ["service", "ins/rem", "offline"]
    fresh_sent_db, _, _archived_wos, _history_db = fetch_sent_records_from_sheet()
    st.session_state['_history_db'] = _history_db
    st.session_state.sent_db = fresh_sent_db
    st.session_state['archived_wos'] = _archived_wos

    pool = []
    unique_tasks_dict = {t['id']: t for t in all_tasks_raw}
    
    for t in unique_tasks_dict.values():
        # 🚫 DRIVER-HOME PSEUDO-TASK GUARD: Onfleet auto-generates
        # "Start at driver address" / "End at driver's address" tasks for native
        # Route Plans, bound to a contractor's home address. Real kiosk tasks
        # always carry a `state` custom field; the pseudo-tasks don't. Require it
        # so the pseudo-tasks never land in the dispatchable pool.
        _has_state_cf = any(
            (str(_f.get('name', '')).strip().lower() == 'state'
             or str(_f.get('key', '')).strip().lower() == 'state')
            and str(_f.get('value', '')).strip()
            for _f in (t.get('customFields') or [])
        )
        if not _has_state_cf:
            continue

        container = t.get('container', {})
        c_type = str(container.get('type', '')).upper()
        # 🛡️ DOUBLE-ROUTING GUARD: skip tasks already assigned to a worker.
        if c_type == 'WORKER' or t.get('worker'):
            continue
        if c_type == 'TEAM' and container.get('team') not in target_team_ids: continue

        addr = t.get('destination', {}).get('address', {})
        stt = normalize_state(addr.get('state', ''))
        is_esc = (c_type == 'TEAM' and container.get('team') in esc_team_ids)
        
        # --- 🔍 STRICT CLASSIFICATION ENGINE (v4 - Final) ---
        native_details = str(t.get('taskDetails', '')).strip()
        custom_fields = t.get('customFields') or []
        
        # 1. EXTRACT OFFICIAL CUSTOM FIELDS
        custom_task_type = ""
        custom_boosted = ""
        venue_name = ""
        venue_id = ""
        client_company = ""
        campaign_name = ""
        location_in_venue = ""
        
        # Default UI display to native details unless a custom field overwrites it
        tt_val = native_details 
        
        for f in custom_fields:
            f_name = str(f.get('name', '')).strip().lower()
            f_key = str(f.get('key', '')).strip().lower()
            f_val = str(f.get('value', '')).strip()
            f_val_lower = f_val.lower()
            
            # Capture Official 'Task Type' Custom Field
            if f_name in ['task type', 'tasktype'] or f_key in ['tasktype', 'task_type']:
                custom_task_type = f_val_lower
                tt_val = f_val # 🌟 UI Display is now officially the Custom Field
                
            # Capture 'Boosted Standard' Custom Field
            if f_name in ['boosted standard', 'boostedstandard'] or f_key in ['boostedstandard', 'boosted_standard']:
                custom_boosted = f_val_lower
                
            # Capture Escalation
            if 'escalation' in f_name or 'escalation' in f_key:
                if f_val_lower in ['1', '1.0', 'true', 'yes'] or 'escalation' in f_val_lower:
                    is_esc = True

            # 🌟 Capture Field Nation metadata fields
            if f_name in ['venuename', 'venue name'] or f_key in ['venuename', 'venue_name']:
                venue_name = f_val
            if f_name in ['venueid', 'venue id'] or f_key in ['venueid', 'venue_id']:
                venue_id = f_val
            if f_name in ['clientcompany', 'client company'] or f_key in ['clientcompany', 'client_company']:
                client_company = f_val
            if f_name in ['locationinvenue', 'location in venue'] or f_key in ['locationinvenue', 'location_in_venue']:
                location_in_venue = f_val
            if f_name in ['campaignname', 'campaign name'] or f_key in ['campaignname', 'campaign_name']:
                campaign_name = f_val  # 🌟 Captured separately so Client Company can't overwrite it

        # 🌟 Campaign Name always wins over Client Company for FN Customer Name
        client_company = campaign_name or client_company

        # 2. CHECK REGULAR (STATIC) EXEMPTIONS FIRST
        # Expanded to include "escalation" to prevent crossing over
        search_string = f"{native_details} {custom_task_type}".lower()
        REGULAR_EXEMPTIONS = ["photo", "magnet", "continuity", "new ad", "pull down", "kiosk install", "kiosk removal", "escalation"]
        is_exempt = any(ex in search_string for ex in REGULAR_EXEMPTIONS)
        
        # 3. STRICT DIGITAL CHECK
        is_digital_task = False

        if not is_exempt:
            # Rule A: Task Type contains service, ins/rem, or offline
            if any(trigger in custom_task_type for trigger in ["service", "ins/rem", "offline"]):
                is_digital_task = True
            # 🌟 Rule B: Boosted Standard contains the word 'digital' (Matches 'Premium_Digital')
            elif "digital" in custom_boosted:
                is_digital_task = True
        
        # 🌟 SPEED FIX: Skip routing math entirely if it's not strictly digital
        if not is_digital_task: 
            continue
            
        # --- 4. ASSIGN STATUS & POOL ---
        t_status = fresh_sent_db.get(t['id'], {}).get('status', 'ready').lower() if t['id'] in fresh_sent_db else 'ready'
        t_wo = fresh_sent_db.get(t['id'], {}).get('wo', 'none') if t['id'] in fresh_sent_db else 'none'
        
        pool.append({
            "id": t['id'], "city": addr.get('city', 'Unknown'), "state": stt,
            "full": f"{addr.get('number','')} {addr.get('street','')}, {addr.get('city','')}, {stt}",
            "zip": addr.get('postalCode', ''),
            "lat": t['destination']['location'][1], "lon": t['destination']['location'][0],
            "escalated": is_esc, "task_type": tt_val, "is_digital": True, "db_status": t_status, "wo": t_wo,
            "boosted_standard": custom_boosted,
            "venue_name": venue_name,
            "venue_id": venue_id,
            "client_company": client_company,
            "location_in_venue": location_in_venue,
        })

    prog_bar.progress(0.6, text=f"🗺️ Routing {len(pool)} Digital Tasks...")
    # Tick digital overlay timer
    _ov = st.session_state.get('_loading_overlay')
    _st = st.session_state.get('_loading_start')
    if _ov and _st:
        import time as _t2
        _el = int(_t2.time() - _st); _m = _el // 60; _s = _el % 60
        _ov.markdown(f"""<style>@keyframes spin{{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
.dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:36px 32px;text-align:center;margin:20px 0;}}
.dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;border-top:4px solid #0f766e;border-radius:50%;animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
.dcc-pill{{display:inline-block;font-size:13px;font-weight:700;color:#0f766e;background:#ccfbf1;border-radius:20px;padding:4px 14px;margin-top:12px;}}</style>
<div class='dcc-card'><div class='dcc-spin'></div>
<p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing Digital Pool</p>
<div class='dcc-pill'>⏱ {_m}:{_s:02d}</div></div>""", unsafe_allow_html=True)
    
    # 3. Route ONLY the Digital Tasks
    ic_df = st.session_state.get('ic_df', pd.DataFrame())
    lat_col = next((col for col in ic_df.columns if 'lat' in str(col).lower()), 'lat')
    lng_col = next((col for col in ic_df.columns if 'lng' in str(col).lower()), 'lng')
    v_ics_base = ic_df[~ic_df.astype(str).apply(lambda x: x.str.contains('Field Agent', case=False, na=False).any(), axis=1)].dropna(subset=[lat_col, lng_col]).copy() if (lat_col in ic_df.columns and lng_col in ic_df.columns) else pd.DataFrame()

    clusters = []
    route_radius = 25 # Strict 25-mile radius for digital
    
    while pool:
        anc = pool.pop(0)
        candidates = []
        rem = []
        
        for t in pool:
            if anc['db_status'] in ['sent', 'accepted', 'field_nation']:
                if t['db_status'] == anc['db_status'] and t['wo'] == anc['wo']: candidates.append((0, t))
                else: rem.append(t)
            elif anc['db_status'] in ['ready', 'declined']:
                if t['db_status'] in ['ready', 'declined']:
                    d = haversine(anc['lat'], anc['lon'], t['lat'], t['lon'])
                    if d <= route_radius: candidates.append((d, t))
                    else: rem.append(t)
                else: rem.append(t)
        
        candidates.sort(key=lambda x: x[0])
        
        group = [anc]
        unique_stops = {anc['full']}
        spillover = []
        for _, t in candidates:
            if len(unique_stops) < 20 or t['full'] in unique_stops:
                group.append(t); unique_stops.add(t['full'])
            else: spillover.append(t)
        rem.extend(spillover)
        
        has_ic = False
        ic_dist = 0
        if not v_ics_base.empty:
            dists = [haversine(anc['lat'], anc['lon'], lat, lng) for lat, lng in zip(v_ics_base[lat_col], v_ics_base[lng_col])]
            valid_ics = v_ics_base.copy()
            valid_ics['d'] = dists
            valid_ics = valid_ics[valid_ics['d'] <= 100]
            if not valid_ics.empty:
                best_ic = valid_ics.sort_values('d').iloc[0]
                has_ic = True
                ic_dist = best_ic['d']

        status = "Ready" if anc['db_status'] not in ['sent', 'accepted', 'finalized'] else anc['db_status'].capitalize()

        # 🌟 DIGITAL FLAGGING: No IC, IC >40mi, or rate >$50/stop → Flagged
        if status == "Ready":
            if not has_ic or ic_dist > 40:
                status = "Flagged"
            else:
                ic_loc_d = f"{anc['lat']},{anc['lon']}"
                _, d_hrs, _, _ = get_gmaps(ic_loc_d, tuple(list(unique_stops)[:25]))
                d_pay = round(d_hrs * 25.0, 2)
                d_rate = round(d_pay / len(unique_stops), 2) if unique_stops else 0
                if d_rate > 50.0:
                    status = "Flagged"

        # Any-match boosted-tier (see process_pod for rationale).
        _d_boosted_vals = [str(x.get('boosted_standard', '')).lower() for x in group if x.get('boosted_standard')]
        if any('local plus' in v for v in _d_boosted_vals):
            _d_boosted_tag = 'local plus'
        elif any('boosted' in v for v in _d_boosted_vals):
            _d_boosted_tag = 'boosted'
        else:
            _d_boosted_tag = ''
        clusters.append({
            "data": group, "center": [anc['lat'], anc['lon']], "stops": len(unique_stops), 
            "city": anc['city'], "state": anc['state'], "status": status, "has_ic": has_ic,
            "esc_count": sum(1 for x in group if x.get('escalated')),
            "is_digital": True,
            "boosted_tag": _d_boosted_tag,
            "inst_count": sum(1 for x in group if "install" in str(x.get('task_type', '')).lower()),
            "remov_count": sum(1 for x in group if "remove" in str(x.get('task_type', '')).lower()),
            "wo": anc['wo']
        })
        pool = rem

    # Save to dedicated Global Digital State
    st.session_state['global_digital_clusters'] = clusters
    # Re-apply Global_Digital bundles after the rebuild.
    _replay_bundles("Global_Digital")
    prog_bar.empty()

# --- CORE LOGIC ---
def process_pod(pod_name, master_bar=None, pod_idx=0, total_pods=1):
    config = POD_CONFIGS[pod_name]
    
    # Logic to handle if we are doing a single pod or a global pull
    pod_weight = 1.0 / total_pods
    start_pct = pod_idx * pod_weight
    
    # Use the master bar if provided, otherwise create a local one
    prog_bar = master_bar if master_bar else st.progress(0)
    
    def update_prog(rel_val, msg):
        global_val = min(start_pct + (rel_val * pod_weight), 0.99)
        prog_bar.progress(global_val, text=f"[{pod_name}] {msg}")
        # 🌟 Tick the loading overlay timer if it exists
        _ov = st.session_state.get('_loading_overlay')
        _st = st.session_state.get('_loading_start')
        _pn = st.session_state.get('_loading_pod')
        if _ov and _st and _pn:
            import time as _t
            elapsed = int(_t.time() - _st)
            m = elapsed // 60; s = elapsed % 60
            _ov.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #633094;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#633094;background:#f3e8ff;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing {_pn} Pod</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>{msg}</p>
                    <div class='dcc-pill'>⏱ {m}:{s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

    try:
        update_prog(0.0, "📥 Extracting tasks...")
        APPROVED_TEAMS = [
            "a - escalation", "b - boosted campaigns", "b - local campaigns", 
            "c - priority nationals", "cvs kiosk removal", "digital routes", "n - national campaigns"
        ]

        teams_res = requests.get("https://onfleet.com/api/v2/teams", headers=headers, timeout=15).json()
        target_team_ids = [t['id'] for t in teams_res if any(appr in str(t.get('name', '')).lower() for appr in APPROVED_TEAMS)]
        esc_team_ids = [t['id'] for t in teams_res if 'escalation' in str(t.get('name', '')).lower()]
        cvs_remov_team_ids = [t['id'] for t in teams_res if 'cvs kiosk remov' in str(t.get('name', '')).lower()]

        all_tasks_raw = []
        time_window = int(time.time()*1000) - (45*24*3600*1000)
        url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}"

        # Cap pagination so a misbehaving lastId can't infinite-loop. 200 pages × 64
        # tasks/page = 12,800 tasks of headroom — well above any real pod workload.
        # The loop also breaks early if lastId stops advancing (true infinite-loop guard).
        _MAX_PAGES = 200
        _page = 0
        _seen_last_ids = set()
        while url and _page < _MAX_PAGES:
            _page += 1
            response = requests.get(url, headers=headers, timeout=15)

            # Handle Rate Limiting (Error 429) dynamically
            if response.status_code == 429:
                st.toast("⚠️ Onfleet Throttling... waiting 2 seconds.")
                time.sleep(2)
                continue

            if response.status_code != 200:
                st.error(f"Onfleet API Error: {response.json()}")
                break

            res_json = response.json()
            tasks_page = res_json.get('tasks', [])
            all_tasks_raw.extend(tasks_page)

            _next_id = res_json.get('lastId')
            if _next_id and _next_id in _seen_last_ids:
                # lastId not advancing — Onfleet returned a duplicate cursor. Bail out
                # so we don't waste pages refetching the same data.
                _log_err("process_pod", f"lastId loop detected at {_next_id} (page {_page})")
                break
            if _next_id:
                _seen_last_ids.add(_next_id)
            url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}&lastId={_next_id}" if _next_id else None
            update_prog(min(len(all_tasks_raw)/500 * 0.4, 0.4), "📡 Fetching tasks...")
        if _page >= _MAX_PAGES:
            _log_err("process_pod", f"hit pagination cap ({_MAX_PAGES} pages)")
            st.warning(f"⚠️ Hit pagination cap of {_MAX_PAGES} pages while fetching Onfleet tasks for {pod_name}. Some tasks may be missing.")

        unique_tasks_dict = {t['id']: t for t in all_tasks_raw}
        all_tasks = list(unique_tasks_dict.values())

        # PERFORMANCE FIX: Fetch Google Sheets data once before the loop
        fresh_sent_db, _, _archived_wos, _history_db = fetch_sent_records_from_sheet()
        st.session_state['_history_db'] = _history_db
        st.session_state.sent_db = fresh_sent_db
        st.session_state['archived_wos'] = _archived_wos

        pool = []
        _skipped_assigned = 0
        # 📊 ATTRITION COUNTERS: track tasks dropped at each filter gate
        _skipped_no_state_cf = 0
        _skipped_wrong_team = 0
        _skipped_out_of_pod_states = 0
        for t in all_tasks:
            # 🚫 DRIVER-HOME PSEUDO-TASK GUARD: Onfleet auto-generates
            # "Start at driver address" / "End at driver's address" tasks for native
            # Route Plans, bound to a contractor's home address. Real kiosk tasks
            # always carry a `state` custom field; the pseudo-tasks don't. Require it
            # so the pseudo-tasks never land in the dispatchable pool.
            _has_state_cf = any(
                (str(_f.get('name', '')).strip().lower() == 'state'
                 or str(_f.get('key', '')).strip().lower() == 'state')
                and str(_f.get('value', '')).strip()
                for _f in (t.get('customFields') or [])
            )
            if not _has_state_cf:
                _skipped_no_state_cf += 1
                continue

            container = t.get('container', {})
            c_type = str(container.get('type', '')).upper()

            # 🛡️ DOUBLE-ROUTING GUARD: Onfleet's `state=0` URL filter sometimes leaks
            # already-assigned tasks through (container=WORKER or worker field set on task).
            # Skip them explicitly so the supercard count + cluster pool only reflects
            # tasks actually available to dispatch. Without this, dispatchers can see
            # phantom availability and accidentally re-dispatch a task to a second IC.
            if c_type == 'WORKER' or t.get('worker'):
                _skipped_assigned += 1
                continue

            if c_type == 'TEAM' and container.get('team') not in target_team_ids: 
                _skipped_wrong_team += 1
                continue

            addr = t.get('destination', {}).get('address', {})
            stt = normalize_state(addr.get('state', ''))
            is_esc = (c_type == 'TEAM' and container.get('team') in esc_team_ids)
            
            # --- 🔍 STRICT CLASSIFICATION ENGINE (v5) ---
            native_details = str(t.get('taskDetails', '')).strip()
            custom_fields = t.get('customFields') or []
            
            # 1. EXTRACT OFFICIAL CUSTOM FIELDS
            custom_task_type = ""
            custom_boosted = ""
            tt_val = native_details # Fallback UI display
            venue_name = ""
            venue_id = ""
            client_company = ""
            campaign_name = ""
            location_in_venue = ""
            
            for f in custom_fields:
                f_name = str(f.get('name', '')).strip().lower()
                f_key = str(f.get('key', '')).strip().lower()
                f_val = str(f.get('value', '')).strip()
                f_val_lower = f_val.lower()
                
                # Capture Official 'Task Type'
                if f_name in ['task type', 'tasktype'] or f_key in ['tasktype', 'task_type']:
                    custom_task_type = f_val_lower
                    tt_val = f_val # Set the UI badge text
                    
                # Capture Official 'Boosted Standard'
                if f_name in ['boosted standard', 'boostedstandard'] or f_key in ['boostedstandard', 'boosted_standard']:
                    custom_boosted = f_val_lower
                    
                # Capture Escalation (Adds the ⭐)
                if 'escalation' in f_name or 'escalation' in f_key:
                    if f_val_lower in ['1', '1.0', 'true', 'yes'] or 'escalation' in f_val_lower:
                        is_esc = True

                # 🌟 Capture Field Nation metadata fields
                if f_name in ['venuename', 'venue name'] or f_key in ['venuename', 'venue_name']:
                    venue_name = f_val
                if f_name in ['venueid', 'venue id'] or f_key in ['venueid', 'venue_id']:
                    venue_id = f_val
                if f_name in ['clientcompany', 'client company'] or f_key in ['clientcompany', 'client_company']:
                    client_company = f_val
                if f_name in ['locationinvenue', 'location in venue'] or f_key in ['locationinvenue', 'location_in_venue']:
                    location_in_venue = f_val
                if f_name in ['campaignname', 'campaign name'] or f_key in ['campaignname', 'campaign_name']:
                    campaign_name = f_val  # 🌟 Captured separately so Client Company can't overwrite it

            # 🌟 Campaign Name always wins over Client Company for FN Customer Name
            client_company = campaign_name or client_company
            # 2. CHECK REGULAR (STATIC) EXEMPTIONS FIRST
            # Combines native and custom type to ensure "Magnet" or "Photo" are never missed
            search_string = f"{native_details} {custom_task_type}".lower()
            REGULAR_EXEMPTIONS = ["photo", "magnet", "continuity", "new ad", "pull down", "kiosk", "escalation"]
            is_exempt = any(ex in search_string for ex in REGULAR_EXEMPTIONS)
            
            # 3. APPLY DIGITAL RULES
            # Locked strictly to the triggers you defined
            DIGITAL_WHITELIST = ["service", "ins/rem", "offline"]
            is_digital_task = False

            if not is_exempt:
                # Rule A: Official Task Type matches whitelist
                if any(trigger in custom_task_type for trigger in DIGITAL_WHITELIST):
                    is_digital_task = True
                # 🌟 Rule B: Boosted Standard contains the word 'digital' (matches 'Premium_Digital')
                elif "digital" in custom_boosted:
                    is_digital_task = True

            # --- 3. ASSIGN STATUS & POOL ---
            t_status = 'ready'
            t_wo = 'none'
            if t['id'] in fresh_sent_db:
                t_status = fresh_sent_db[t['id']].get('status', 'ready').lower()
                t_wo = fresh_sent_db[t['id']].get('wo', 'none')
            
            if stt not in config['states']:
                _skipped_out_of_pod_states += 1
                continue
            if stt in config['states']:
                _remov_keywords = ["kiosk removal", "remove kiosk"]
                _is_cvs_team = (c_type == 'TEAM' and container.get('team') in cvs_remov_team_ids)
                _is_removal = _is_cvs_team and any(kw in f"{native_details} {custom_task_type}".lower() for kw in _remov_keywords)
                pool.append({
                    "id": t['id'], 
                    "city": addr.get('city', 'Unknown'), 
                    "state": stt,
                    "full": f"{addr.get('number','')} {addr.get('street','')}, {addr.get('city','')}, {stt}",
                    "zip": addr.get('postalCode', ''),
                    "lat": t['destination']['location'][1], 
                    "lon": t['destination']['location'][0],
                    "escalated": is_esc, 
                    "task_type": tt_val,
                    "is_digital": is_digital_task,
                    "is_removal": _is_removal,
                    "boosted_standard": custom_boosted,
                    "db_status": t_status, 
                    "wo": t_wo,
                    "venue_name": venue_name,
                    "venue_id": venue_id,
                    "client_company": client_company,
                    "location_in_venue": location_in_venue,
                })
                
        clusters = []
        total_pool = len(pool)
        ic_df = st.session_state.get('ic_df', pd.DataFrame())
        
        # 🌟 CRITICAL FIX: Safe extraction using standardized headers
        lat_col = next((col for col in ic_df.columns if 'lat' in str(col).lower()), 'lat')
        lng_col = next((col for col in ic_df.columns if 'lng' in str(col).lower()), 'lng')
        
        if lat_col in ic_df.columns and lng_col in ic_df.columns:
            v_ics_base = ic_df[~ic_df.astype(str).apply(lambda x: x.str.contains('Field Agent', case=False, na=False).any(), axis=1)].dropna(subset=[lat_col, lng_col]).copy()
        else:
            v_ics_base = pd.DataFrame()

        while pool:
            # Routing progress calculation
            rel_prog = 0.4 + (0.6 * (1 - (len(pool) / total_pool if total_pool > 0 else 1)))
            update_prog(rel_prog, f"🗺️ Routing {len(pool)} remaining tasks...")
            
            anc = pool.pop(0)
            
            # --- NEW: Strict Digital Separation & Dynamic Radius ---
            anc_tt = str(anc.get('task_type', '')).lower()
            anc_is_digital = anc.get('is_digital', False)
            anc_is_removal = anc.get('is_removal', False)
            anc_status = anc.get('db_status', 'ready')
            anc_wo = anc.get('wo', 'none')
            
            # Set radius strictly based on type
            route_radius = 25 if anc_is_digital else 35
            
            candidates = []; rem = []
            for t in pool:
                t_tt = str(t.get('task_type', '')).lower()
                t_is_digital = t.get('is_digital', False)
                t_is_removal = t.get('is_removal', False)
                t_status = t.get('db_status', 'ready')
                t_wo = t.get('wo', 'none')
                
                # Rule 1: Digital, Removal, and Standard never mix
                if anc_is_digital == t_is_digital and anc_is_removal == t_is_removal:
                    
                    # Rule 2: Sent and Accepted are FROZEN
                    # 🌟 FIX 1: Add 'field_nation' so these routes stay grouped together!
                    if anc_status in ['sent', 'accepted', 'field_nation']:
                        # Bypasses distance! ONLY groups if the Work Order matches perfectly.
                        if t_status == anc_status and t_wo == anc_wo:
                            candidates.append((0, t)) 
                        else:
                            rem.append(t)
                            
                    # Rule 3: Ready and Declined are LIQUID (They can mix!)
                    elif anc_status in ['ready', 'declined']:
                        if t_status in ['ready', 'declined']:
                            d = haversine(anc['lat'], anc['lon'], t['lat'], t['lon'])
                            if d <= route_radius: 
                                candidates.append((d, t))
                            else: 
                                rem.append(t)
                        else:
                            rem.append(t)
                else:
                    rem.append(t)
            
            candidates.sort(key=lambda x: x[0])
            
            # --- STOP LIMIT: 10 for CVS Removal, 20 for all others ---
            stop_limit = 10 if anc_is_removal else 20
            group = [anc]
            unique_stops = {anc['full']}
            spillover = []
            
            for _, t in candidates:
                if len(unique_stops) < stop_limit or t['full'] in unique_stops:
                    group.append(t)
                    unique_stops.add(t['full'])
                else:
                    spillover.append(t)
            
            # 🌟 BRIDGE: Put spillover back and fix the 'd' column error
            rem.extend(spillover)
            
            # --- 📡 1. IC SEARCH & DISTANCE CHECK (OPTIMIZED) ---
            has_ic = False
            ic_dist = 0
            closest_ic_loc = f"{anc['lat']},{anc['lon']}" 
            
            if not v_ics_base.empty:
                # 🚀 OPTIMIZATION: Use list comprehension instead of pandas .apply(). It is ~100x faster.
                dists = [
                    haversine(anc['lat'], anc['lon'], lat, lng) 
                    for lat, lng in zip(v_ics_base[lat_col], v_ics_base[lng_col])
                ]
                
                valid_ics = v_ics_base.copy()
                valid_ics['d'] = dists
                valid_ics = valid_ics[valid_ics['d'] <= 100]
                
                if not valid_ics.empty:
                    best_ic = valid_ics.sort_values('d').iloc[0]
                    has_ic = True
                    ic_dist = best_ic['d']
                    closest_ic_loc = best_ic.get('location', closest_ic_loc)

            def check_viability(grp):
                seen = set(); u_locs = []
                for x in grp:
                    if x['full'] not in seen: seen.add(x['full']); u_locs.append(x['full'])
                if not u_locs: return 0, 0
                
                # 🚀 OPTIMIZATION: Reverted back to real Google Maps!
                # Wrapping u_locs[:25] in a tuple() makes Streamlit's cache process it instantly.
                _, hrs, _, _ = get_gmaps(closest_ic_loc, tuple(u_locs[:25]))
                pay = round(hrs * 25.0, 2) # 🌟 STRICTLY HOURLY ($25/hr)
                return round(pay / len(u_locs), 2), len(u_locs)
            
            gate_avg, _ = check_viability(group)
            
            # --- 🚦 2. UPDATED FLAGGING LOGIC ---
            if anc_status in ['sent', 'accepted', 'finalized']:
                status = anc_status.capitalize()
            else:
                status = "Ready" # Default status
                
                # Flag Criteria A: High Rate (> $23/stop)
                if gate_avg > 23.00:
                    if len(group) > 1:
                        removed = group.pop()
                        new_avg, _ = check_viability(group)
                        if new_avg <= 23.00:
                            rem.append(removed)
                        else:
                            group.append(removed)
                            status = "Flagged"
                    else:
                        status = "Flagged"
                
                # Flag Criteria B: Long Distance (> 60 miles) or No Contractor
                if not has_ic or ic_dist > 60:
                    status = "Flagged"

            # --- 📊 3. COUNTERS & SAVE TO SESSION ---
            g_data = group

            # 🌟 CLEANUP: No need to loop again; the anchor already knows!
            route_is_digital = anc_is_digital
            
            # Route is tagged boosted/local-plus if ANY task in the cluster has that tier.
            # The header pill ensures the dispatcher sees boosted routes immediately,
            # even when the boosted task is a single one inside a mostly-pulldown cluster.
            # Drill-down to the specific stop + campaign happens via make_venue_details,
            # which counts boosted tasks per location and per campaign row.
            _boosted_vals = [str(x.get('boosted_standard', '')).lower() for x in g_data if x.get('boosted_standard')]
            if any('local plus' in v for v in _boosted_vals):
                _boosted_tag = 'local plus'
            elif any('boosted' in v for v in _boosted_vals):
                _boosted_tag = 'boosted'
            else:
                _boosted_tag = ''

            clusters.append({
                "data": g_data, 
                "center": [anc['lat'], anc['lon']], 
                "stops": len(set(x['full'] for x in g_data)), 
                "city": anc['city'], "state": anc['state'],
                "status": status,
                "has_ic": has_ic,
                "esc_count": sum(1 for x in g_data if x.get('escalated')),
                "is_digital": route_is_digital,
                "is_removal": anc_is_removal,
                "boosted_tag": _boosted_tag,
                "inst_count": sum(1 for x in g_data if "install" in str(x.get('task_type', '')).lower()),
                "remov_count": sum(1 for x in g_data if str(x.get('task_type', '')).lower() in ["kiosk removal", "remove kiosk"]),
                "wo": anc_wo
            })
            pool = rem

        st.session_state[f"clusters_{pod_name}"] = clusters
        # Re-apply any bundles the dispatcher previously confirmed for this pod, so a
        # full re-init via Initialize Data doesn\'t silently undo them.
        _replay_bundles(pod_name)
        if _skipped_assigned > 0:
            _log_err(f"process_pod/{pod_name}", f"skipped {_skipped_assigned} already-assigned tasks (state=0 leak)")

        # 📊 Save attrition funnel to session state for the supercard expander.
        st.session_state[f'_attrition_{pod_name}'] = {
            'raw_fetched': len(all_tasks_raw),
            'after_dedup': len(all_tasks),
            'skipped_no_state_cf': _skipped_no_state_cf,
            'skipped_assigned_worker': _skipped_assigned,
            'skipped_wrong_team': _skipped_wrong_team,
            'skipped_out_of_pod_states': _skipped_out_of_pod_states,
            'final_pool': len(pool),
        }
        if not master_bar: 
            prog_bar.empty()

        st.session_state['_worker_counts'] = fetch_worker_task_counts()

    except Exception as e:
        st.error(f"Error initializing {pod_name}: {str(e)}")
# 🌟 NEW HELPER: Standardized Digital Badges
def get_digi_badges(cluster_data):
    icons = set()
    for t in cluster_data:
        if t.get('is_digital'):
            tt = str(t.get('task_type', '')).lower()
            if 'offline' in tt: icons.add('📵')
            elif 'ins/re' in tt: icons.add('🔧') # 🌟 Standard Wrench
            else: icons.add('⚙️')
    return "".join(sorted(list(icons)))


# 🔗 Bundled tag for route-card expander headers. Dim non-bold gray, far-right of the
# label, only shown when the cluster has absorbed other routes via the Bundle Routes
# preview-and-confirm flow. cluster.bundle_count is set by _replay_bundles() below.
def _bundle_pill(cluster):
    _n = cluster.get('bundle_count', 0) if isinstance(cluster, dict) else 0
    return "  ·  :gray[🔗 Bundled]" if _n and _n > 0 else ""


# 🔗 BUNDLE PERSISTENCE — survives process_pod / smart_sync_pod / process_digital_pool
# rebuilds. Each entry is a SET of Onfleet task IDs that the dispatcher decided should
# end up in the same cluster. After any cluster rebuild we call _replay_bundles() and
# it re-merges the relevant clusters. Keyed by pod_name (or "Global_Digital").
def _bundle_clusters_store(pod_name):
    """Return (read_key, write_key) for the cluster store this pod uses. Global_Digital
    has its own bucket; everything else uses clusters_{pod_name}."""
    if pod_name == "Global_Digital":
        return "global_digital_clusters"
    return f"clusters_{pod_name}"

def _commit_bundle(pod_name, target_task_ids, source_task_ids):
    """Record that target_task_ids and source_task_ids should be in the same cluster.
    Idempotent — overlapping entries get merged into one set so a route bundled three
    times shows up as a single entry, not three."""
    combined = set(target_task_ids) | set(source_task_ids)
    bm = st.session_state.setdefault('_bundle_map', {})
    pm = bm.setdefault(pod_name, [])
    keep = []
    for entry in pm:
        if entry & combined:
            combined |= entry
        else:
            keep.append(entry)
    keep.append(combined)
    bm[pod_name] = keep

def _replay_bundles(pod_name):
    """Re-merge any clusters that the dispatcher previously bundled. Safe to call after
    every cluster rebuild — if the bundle has already been applied (single cluster
    contains all the listed task IDs) this is a no-op for that entry."""
    bm = st.session_state.get('_bundle_map', {})
    entries = bm.get(pod_name, []) or []
    if not entries:
        return
    _key = _bundle_clusters_store(pod_name)
    cls = st.session_state.get(_key, [])
    if not cls:
        return
    _changed = False
    for entry in entries:
        # Find every cluster index whose data overlaps with this bundle\'s task-ID set.
        members = []
        for ci, c in enumerate(cls):
            tids = {str(t['id']).strip() for t in c.get('data', [])}
            if tids & entry:
                members.append(ci)
        if len(members) < 2:
            continue  # already merged, or members no longer present
        target_idx = members[0]
        source_indices = members[1:]
        target = cls[target_idx]
        existing_ids = {str(t['id']).strip() for t in target.get('data', [])}
        for si in source_indices:
            for t in cls[si].get('data', []):
                tid = str(t['id']).strip()
                if tid not in existing_ids:
                    target['data'].append(t)
                    existing_ids.add(tid)
        target['stops'] = len(set(x['full'] for x in target['data']))
        target['inst_count'] = sum(1 for x in target['data'] if 'install' in str(x.get('task_type','')).lower())
        target['remov_count'] = sum(1 for x in target['data'] if str(x.get('task_type','')).lower() in ('kiosk removal','remove kiosk'))
        target['esc_count'] = sum(1 for x in target['data'] if x.get('escalated'))
        target['bundle_count'] = target.get('bundle_count', 0) + len(source_indices)
        # Drop merged sources (highest indices first to preserve earlier ones).
        for si in sorted(source_indices, reverse=True):
            cls.pop(si)
        _changed = True
    if _changed:
        st.session_state[_key] = cls


# 🌟 Reusable pill helper for route card titles (Sent/Accepted/Declined/Finalized expanders
# and the Dispatch column big card). Returns a string like " | 🛠️ 5 Kiosk" for static
# routes or " | 🔧 3 Ins/2 Rem" for digital ones — empty string when no relevant tasks.
# Color is enforced via the surrounding markdown's CSS where needed.
def get_task_pill(cluster_data):
    if not cluster_data:
        return ""
    is_digi = any(t.get('is_digital') for t in cluster_data)
    if is_digi:
        ins_n = sum(1 for t in cluster_data if t.get('is_digital') and 'install' in str(t.get('task_type','')).lower() and 'ins/re' not in str(t.get('task_type','')).lower())
        rem_n = sum(1 for t in cluster_data if t.get('is_digital') and ('remov' in str(t.get('task_type','')).lower() or 'remove' in str(t.get('task_type','')).lower()))
        insrem_n = sum(1 for t in cluster_data if t.get('is_digital') and 'ins/re' in str(t.get('task_type','')).lower())
        parts = []
        if ins_n > 0: parts.append(f"{ins_n} Ins")
        if rem_n > 0: parts.append(f"{rem_n} Rem")
        if insrem_n > 0: parts.append(f"{insrem_n} Ins/Rem")
        return f" | 🔧 {' / '.join(parts)}" if parts else ""
    # Static kiosk path: count install tasks
    k_n = sum(1 for t in cluster_data if 'install' in str(t.get('task_type','')).lower())
    return f" | 🛠️ {k_n} Kiosk" if k_n > 0 else ""


# 🌟 NEW HELPER: Groups clusters by State, then sorts them by geographical proximity
def group_and_sort_by_proximity(bucket):
    if not bucket: return []
    grouped = {}
    for c in bucket:
        stt = c.get('state', 'UNKNOWN')
        if stt not in grouped: grouped[stt] = []
        grouped[stt].append(c)
    
    final_list = []
    for stt in sorted(grouped.keys()):
        state_cls = grouped[stt]
        if not state_cls: continue
        
        # Start with the first cluster and chain the nearest neighbors
        sorted_st_cls = [state_cls.pop(0)]
        while state_cls:
            last_center = sorted_st_cls[-1]['center']
            # Find the closest remaining cluster in this state
            closest_idx, min_d = 0, float('inf')
            for idx, x in enumerate(state_cls):
                d = haversine(last_center[0], last_center[1], x['center'][0], x['center'][1])
                if d < min_d:
                    min_d, closest_idx = d, idx
            sorted_st_cls.append(state_cls.pop(closest_idx))
        
        final_list.extend(sorted_st_cls)
    return final_list
# 🌟 NEW HELPER: Groups Awaiting routes by Date Sent, unifying Live and Ghost routes
def unify_and_sort_by_date(live_routes, ghost_routes, live_hashes):
    unified = []
    
    # 1. Process Live Routes
    for c in live_routes:
        c_copy = c.copy()
        c_copy['is_ghost'] = False
        ts = c_copy.get('route_ts', '')
        c_copy['sort_date'] = str(ts).split(' ')[0] if ts else 'Unknown Date'
        unified.append(c_copy)
        
    # 2. Process Ghost Routes (Skipping active duplicates)
    for g in ghost_routes:
        if g.get('hash') in live_hashes:
            continue
        g_copy = g.copy()
        g_copy['is_ghost'] = True
        ts = g_copy.get('route_ts', '')
        g_copy['sort_date'] = str(ts).split(' ')[0] if ts else 'Unknown Date'
        unified.append(g_copy)
        
    # 3. Sort descending (Newest dates at the very top)
    unified.sort(key=lambda x: x['sort_date'], reverse=True)
    return unified

# --- DISPATCH RENDERING ---
@st.fragment
def render_dispatch(i, cluster, pod_name, is_sent=False, is_declined=False):
    # Capture current state identifiers (cluster_hash is computed from the ORIGINAL data
    # and stays constant through preview — keeps session-state continuity intact).
    task_ids = [str(t['id']).strip() for t in cluster['data']]
    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()

    # 🔗 BUNDLE PREVIEW — multiselect at the bottom of this fragment can flag nearby
    # clusters for preview. While in preview, this render shows the route card AS IF
    # those clusters were merged. Deselecting reverts the card. The actual merge into
    # session_state only happens when the dispatcher clicks "Confirm Bundle".
    _bundle_select_key = f"bundle_select_{pod_name}_{cluster_hash}"
    # Deferred-clear consumer: Streamlit forbids writing to a widget\'s session-state
    # key AFTER the widget is instantiated, so the Confirm/Clear button handlers below
    # set this intent flag and rerun. Here, BEFORE the multiselect renders, we honor
    # the flag by popping the multiselect\'s saved state (which IS allowed pre-render).
    _bundle_clear_intent_key = f"_bundle_clear_intent_{pod_name}_{cluster_hash}"
    if st.session_state.pop(_bundle_clear_intent_key, False):
        st.session_state.pop(_bundle_select_key, None)
    _preview_hashes = st.session_state.get(_bundle_select_key) or []
    _in_preview = False
    _preview_added_count = 0  # tasks added by preview (for display in Confirm button)
    if _preview_hashes:
        _pv_pool = st.session_state.get(f"clusters_{pod_name}", [])
        _pv_data = list(cluster['data'])
        _pv_seen = {str(_t['id']).strip() for _t in _pv_data}
        _matched_any = False
        for _ph in _preview_hashes:
            for _other in _pv_pool:
                _o_tids_pv = sorted([str(_t['id']).strip() for _t in _other.get('data', [])])
                _o_h_pv = hashlib.md5("".join(_o_tids_pv).encode()).hexdigest()
                if _o_h_pv == _ph:
                    _matched_any = True
                    for _t in _other.get('data', []):
                        _tid = str(_t['id']).strip()
                        if _tid not in _pv_seen:
                            _pv_data.append(_t)
                            _pv_seen.add(_tid)
                            _preview_added_count += 1
                    break
        if _matched_any and _preview_added_count > 0:
            cluster = dict(cluster)
            cluster['data'] = _pv_data
            cluster['stops'] = len(set(_t['full'] for _t in _pv_data))
            cluster['inst_count'] = sum(1 for _t in _pv_data if 'install' in str(_t.get('task_type','')).lower())
            cluster['remov_count'] = sum(1 for _t in _pv_data if str(_t.get('task_type','')).lower() in ('kiosk removal','remove kiosk'))
            cluster['esc_count'] = sum(1 for _t in _pv_data if _t.get('escalated'))
            task_ids = [str(_t['id']).strip() for _t in _pv_data]
            _in_preview = True

    # 📜 Route history banner — documents the 3 events that matter when a route
    # comes back to Ready: Declined (from the contractor portal, sourced from the sheet),
    # Revoked (dispatcher pulled it back), Re-Routed (dispatcher moved it to a new IC).
    # Apr 27 2026 — Revoked/Re-Routed are now ALSO sheet-backed via Archive, so the
    # banner survives session resets. Session-state actions are still merged in for
    # instant feedback before the GAS round-trip lands.
    if not is_sent and not is_declined:
        _events = []
        _seen = set()

        # 1. Declined / Revoked / Re-Routed events from sheet-backed history_db.
        _hist_db = st.session_state.get('_history_db', {})
        _STATUS_TO_KIND = {
            'declined':       'Declined',
            'revoked':        'Revoked',
            'ghost-archived': 'Revoked',  # archived ghost rows are conceptually revokes
            're-routed':      'Re-Routed',
        }
        for _tid in task_ids:
            for _h in _hist_db.get(_tid, []):
                _kind = _STATUS_TO_KIND.get(_h.get('status', ''))
                if not _kind:
                    continue
                _key = (_kind, _h.get('name',''), _h.get('time',''), _h.get('wo',''))
                if _key in _seen:
                    continue
                _seen.add(_key)
                _events.append({
                    'kind': _kind,
                    'ic':   _h.get('name',''),
                    'wo':   _h.get('wo',''),
                    'time': _h.get('time',''),
                    'ts':   _h.get('raw_ts'),
                })

        # 2. Revoked / Re-Routed actions from session state (instant feedback before
        # the GAS archiveRoute round-trip lands in the sheet).
        for _a in st.session_state.get(f'_actions_{cluster_hash}', []):
            _act = _a.get('action','')
            if _act in ('Revoked', 'Re-Routed'):
                _key = (_act, _a.get('ic',''), _a.get('time',''), '')
                if _key in _seen:
                    continue
                _seen.add(_key)
                _events.append({
                    'kind': _act,
                    'ic':   _a.get('ic',''),
                    'wo':   '',
                    'time': _a.get('time',''),
                    'ts':   _a.get('ts'),
                })

        if _events:
            _STYLE = {
                'Declined':  ('❌', '#dc2626', 'Declined by'),
                'Revoked':   ('↩️', '#ca8a04', 'Revoked from'),
                'Re-Routed': ('🔄', '#7e22ce', 'Re-routed from'),
            }
            _events.sort(key=lambda e: (e.get('ts') or pd.Timestamp.min), reverse=True)
            _rows_html = []
            for _e in _events[:6]:  # cap to most-recent 6 to keep banner compact
                _icon, _color, _verb = _STYLE[_e['kind']]
                _line = f"<span style='color:{_color};font-weight:700;'>{_icon} {_verb} {_e['ic']}</span>"
                if _e.get('wo'):
                    _line += f" <span style='color:#94a3b8;'>· {_e['wo']}</span>"
                if _e.get('time'):
                    _line += f" <span style='color:#94a3b8;'>· {_e['time']}</span>"
                _rows_html.append(f"<div style='font-size:11px;padding:2px 0;'>{_line}</div>")
            st.markdown(
                "<div style='background:#fef3c7;border-left:3px solid #f59e0b;border-radius:6px;"
                "padding:8px 12px;margin:8px 0 12px 0;'>"
                "<div style='font-size:9px;font-weight:900;color:#92400e;text-transform:uppercase;"
                "letter-spacing:0.08em;margin-bottom:4px;'>📜 Route History</div>"
                + "".join(_rows_html)
                + "</div>",
                unsafe_allow_html=True,
            )

    sync_key = f"sync_{cluster_hash}"
    real_id = st.session_state.get(sync_key)
    link_id = real_id if real_id else "LINK_PENDING"

    # Scrub now runs silently in background_sheet_move — nothing blocks here

    # --- 1. STATE KEYS & INITIALIZATION (🌟 UNIQUE BY POD) ---
    pay_key = f"pay_val_{pod_name}_{cluster_hash}"
    rate_key = f"rate_val_{pod_name}_{cluster_hash}"
    sel_key = f"sel_{pod_name}_{cluster_hash}"
    last_sel_key = f"last_sel_{pod_name}_{cluster_hash}"

    # --- 2. STOP METRICS & PILLS (build dict — UI rendered after financials) ---
    stop_metrics = {}
    for t in cluster['data']:
        addr = t['full']
        if addr not in stop_metrics:
            stop_metrics[addr] = {
                't_count': 0, 'n_ad': 0, 'c_ad': 0, 'd_ad': 0,
                'inst': 0, 'remov': 0, 'digi_off': 0, 'digi_ins': 0, 'digi_srv': 0,
                'custom': {}, 'esc': False, 'is_new': False, 'venue_name': '',
                'boost_cnt': 0, 'lplus_cnt': 0,
            }
        stop_metrics[addr]['t_count'] += 1
        if t.get('escalated'): stop_metrics[addr]['esc'] = True
        # Per-stop boosted/local-plus task counts so the stop row can show 🔥 N / ⭐ N
        # pills alongside the kiosk and escalation indicators.
        _t_bs = str(t.get('boosted_standard','')).lower()
        if 'local plus' in _t_bs: stop_metrics[addr]['lplus_cnt'] += 1
        elif 'boosted' in _t_bs: stop_metrics[addr]['boost_cnt'] += 1
        if t.get('is_new'): stop_metrics[addr]['is_new'] = True
        if not stop_metrics[addr]['venue_name'] and t.get('venue_name'):
            stop_metrics[addr]['venue_name'] = t.get('venue_name', '')
            
        raw_tt = str(t.get('task_type', '')).strip()
        parts = [p.strip().lower() for p in raw_tt.split(',') if p.strip()]
        if "escalation" in parts:
            if len(parts) > 1: parts.remove("escalation") 
            else: parts = ["new ad"] 
        tt = ", ".join(parts)

        found_category = False
        
        # 🌟 Split Digital Tasks
        if t.get('is_digital'):
            if "offline" in tt: stop_metrics[addr]['digi_off'] += 1
            elif "ins/re" in tt: stop_metrics[addr]['digi_ins'] += 1
            else: stop_metrics[addr]['digi_srv'] += 1
            found_category = True
        else:
            if "install" in tt: 
                stop_metrics[addr]['inst'] += 1
                found_category = True
            if any(trigger in tt for trigger in ["kiosk removal", "remove kiosk"]):
                stop_metrics[addr]['remov'] += 1
                found_category = True
            if any(x in tt for x in ["continuity", "photo retake", "swap"]): 
                stop_metrics[addr]['c_ad'] += 1
                found_category = True
            if any(x in tt for x in ["default", "pull down"]): 
                stop_metrics[addr]['d_ad'] += 1
                found_category = True
        
        if any(x in tt for x in ["new ad", "art change", "top"]) or not tt:
            stop_metrics[addr]['n_ad'] += 1
        elif not found_category:
            # 🌟 THE FIX: Push exactly the remaining task type over
            display_tt = tt.title()
            if display_tt not in stop_metrics[addr]['custom']:
                stop_metrics[addr]['custom'][display_tt] = 0
            stop_metrics[addr]['custom'][display_tt] += 1
            
    # --- 3. CONTRACTOR FILTERING (100 MILES) ---
    ic_df = st.session_state.get('ic_df', pd.DataFrame())
    ic_opts = {} 
    v_ics = pd.DataFrame()
    _worker_counts = st.session_state.get('_worker_counts', {})

    if not ic_df.empty:
        ic_df.columns = [str(c).strip().lower() for c in ic_df.columns]
        lat_col, lng_col = 'lat', 'lng'
        if lat_col in ic_df.columns and lng_col in ic_df.columns:
            v_ics = ic_df[~ic_df.astype(str).apply(lambda x: x.str.contains('Field Agent', case=False, na=False).any(), axis=1)].copy()
            v_ics = v_ics.dropna(subset=[lat_col, lng_col])
            if not v_ics.empty:
                v_ics['d'] = v_ics.apply(lambda x: haversine(cluster['center'][0], cluster['center'][1], x[lat_col], x[lng_col]), axis=1)
                v_ics = v_ics[v_ics['d'] <= 100].sort_values('d')
                for _, r in v_ics.iterrows():
                    cert_val = str(r.get('digital certified', '')).strip().upper()
                    cert_icon = " 🔌" if cert_val in ['YES', 'Y', 'TRUE', '1', '1.0'] else ""
                    ic_name = r.get('name', 'Unknown')
                    _ic_phone = re.sub(r'\D', '', str(r.get('phone', '')))[-10:]
                    _task_cnt = _worker_counts.get(_ic_phone, 0)
                    _cnt_tag = f" 🔵{_task_cnt}" if _task_cnt > 0 else " 🔵0"
                    label = f"{ic_name}{cert_icon}{_cnt_tag} ({round(r['d'], 1)} mi)"
                    ic_opts[label] = r

    # --- DYNAMIC PRICING SYNC ---
    def sync_on_total():
        val = st.session_state.get(pay_key)
        if val is not None:
            st.session_state[rate_key] = round(val / cluster['stops'], 2) if cluster['stops'] > 0 else 0

    def sync_on_rate():
        val = st.session_state.get(rate_key)
        if val is not None:
            st.session_state[pay_key] = round(val * cluster['stops'], 2)

    def update_for_new_contractor():
        selected_label = st.session_state.get(sel_key)
        if selected_label and selected_label != st.session_state.get(last_sel_key):
            ic_new = ic_opts[selected_label]
            _, h, _, _ = get_gmaps(ic_new.get('location', f"{cluster['center'][0]},{cluster['center'][1]}"), tuple(stop_metrics.keys()))
            new_pay = float(round(h * 25.0, 2)) # 🌟 STRICTLY HOURLY
            # 🌟 BUGFIX: Apply same $20/stop floor used in init logic. Without this, when
            # gmaps fails (network error, bad IC location, etc.) the comp/rate fields zero out
            # and the dispatcher loses the displayed data even though the route is real.
            if new_pay == 0:
                new_pay = round(20.0 * cluster.get('stops', 1), 2)
            st.session_state[pay_key] = new_pay
            st.session_state[rate_key] = round(new_pay / cluster['stops'], 2) if cluster['stops'] > 0 else 20.0
            st.session_state[last_sel_key] = selected_label

    # --- 4. INITIAL SETUP (FIXED SAVING LOGIC) ---
    if pay_key not in st.session_state:
        prev_name = cluster.get('contractor_name', 'Unknown')
        default_label = list(ic_opts.keys())[0] if ic_opts else None
        
        # Match previous contractor if possible
        if prev_name != 'Unknown' and ic_opts:
            for label, row in ic_opts.items():
                if row.get('name') == prev_name:
                    default_label = label; break

        # 🌟 THE FIX: Restore saved database pay first, OR calculate via Google Maps
        saved_comp = float(cluster.get('comp', 0))
        
        if saved_comp > 0:
            # Load the exact amount stored in Google Sheets
            initial_pay = saved_comp
            if default_label:
                st.session_state[sel_key] = default_label
                st.session_state[last_sel_key] = default_label
        elif default_label:
            # Calculate from the Contractor's Home
            ic_init = ic_opts[default_label]
            _, h, _, _ = get_gmaps(ic_init.get('location', f"{cluster['center'][0]},{cluster['center'][1]}"), tuple(stop_metrics.keys()))
            initial_pay = float(round(h * 25.0, 2)) # 🌟 STRICTLY HOURLY
            st.session_state[sel_key] = default_label
            st.session_state[last_sel_key] = default_label
        else:
            # 🌟 THE FIX: If no IC is found, calculate the hourly rate from the cluster's center!
            _, h, _, _ = get_gmaps(f"{cluster['center'][0]},{cluster['center'][1]}", tuple(stop_metrics.keys()))
            initial_pay = float(round(h * 25.0, 2)) # 🌟 STRICTLY HOURLY

        # 🌟 Floor: if Maps returned 0 (fail/no IC), seed from $20/stop default
        if initial_pay == 0:
            initial_pay = round(20.0 * cluster.get('stops', 1), 2)
        st.session_state[pay_key] = initial_pay
        st.session_state[rate_key] = round(initial_pay / cluster['stops'], 2) if cluster['stops'] > 0 else 20.0
    
    # --- 4. UI RENDERING & BUTTON LOGIC ---
    route_state = st.session_state.get(f"route_state_{cluster_hash}")
    is_fn = (route_state == "field_nation")

    # Default ic for FN routes — overridden below if not is_fn
    ic = {"name": "Field Nation", "location": f"{cluster['center'][0]},{cluster['center'][1]}", "d": 0}
    mi, hrs, t_str = 0, 0, "N/A"  # defaults for FN routes
    is_unlocked = True

    if not is_fn:
        ic_location_tmp = f"{cluster['center'][0]},{cluster['center'][1]}"

        # ── CONTRACTOR ──────────────────────────────────────────────────
        st.markdown(f"""<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px;">
            <span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Contractor</span>
            <span style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em;">{cluster['stops']} Stops / {len(cluster['data'])} Tasks</span>
        </div>""", unsafe_allow_html=True)

        if ic_opts:
            selected_label = st.selectbox("Contractor", list(ic_opts.keys()), key=sel_key, on_change=update_for_new_contractor, label_visibility="collapsed")
            ic = ic_opts[selected_label]
            ic_location_tmp = ic.get('location', ic_location_tmp)
        else:
            ic = {"name": "Manual/FN", "location": ic_location_tmp, "d": 0}
            st.info("No ICs within 100mi.")

        ic_location = ic_location_tmp
        mi, hrs, t_str, _wp_order = get_gmaps(ic_location, tuple(stop_metrics.keys()))

        curr_rate = st.session_state[rate_key]
        ic_dist = ic.get('d', 0)
        needs_unlock = (curr_rate >= 25.0) or (ic_dist > 60) or (cluster['status'] == 'Flagged')
        is_unlocked = True

        if needs_unlock:
            reasons = []
            if curr_rate >= 25.0: reasons.append(f"High Rate (${curr_rate})")
            if ic['d'] > 60: reasons.append(f"Distance ({round(ic['d'],1)}mi)")
            if cluster['status'] == 'Flagged': reasons.append("Flagged Route")
            st.markdown(f"""<div style="background:#fef2f2; border:1px solid #ef4444; padding:8px 10px; border-radius:8px; margin:6px 0;"><span style="color:#b91c1c; font-weight:800; font-size:11px;">🔒 ACTION REQUIRED:</span> <span style="color:#7f1d1d; font-size:11px;">{" & ".join(reasons)}</span></div>""", unsafe_allow_html=True)
            is_unlocked = st.checkbox("Authorize Premium Rate / Distance", key=f"lock_{pod_name}_{cluster_hash}")

        # ── INPUTS ──────────────────────────────────────────────────────
        st.markdown("<div style='border-top:1px solid #f1f5f9; margin:8px 0 6px 0;'></div>", unsafe_allow_html=True)
        _inp_a, _inp_b, _inp_c = st.columns([1.5, 1.5, 1.5])
        with _inp_a:
            st.number_input("Total Comp ($)", min_value=0.0, step=5.0, format="%.2f", key=pay_key, on_change=sync_on_total, disabled=not is_unlocked)
        with _inp_b:
            st.number_input("Rate/Stop ($)", min_value=0.0, step=1.0, format="%.2f", key=rate_key, on_change=sync_on_rate, disabled=not is_unlocked)
        with _inp_c:
            st.date_input("Deadline", datetime.now().date()+timedelta(DEFAULT_DUE_DAYS), key=f"dd_{pod_name}_{cluster_hash}", disabled=not is_unlocked)

        # ── FINANCIALS CARD ──────────────────────────────────────────────
        final_pay = st.session_state.get(pay_key, 0.0)
        final_rate = st.session_state.get(rate_key, 0.0)

        if final_rate >= RATE_CRITICAL: status_color = "#ef4444"
        elif final_rate >= RATE_WARNING: status_color = "#f97316"
        else: status_color = TB_GREEN

        # 🌟 BUGFIX: when get_gmaps returns 0 / "0h 0m" (network/API failure or empty IC
        # location), don't render literal zeros. Show "—" so dispatchers know the value
        # didn't come from a real measurement, and rely on the floor-seeded comp/rate
        # values for the dispatch decision instead.
        _t_display = t_str if t_str and t_str not in ("0h 0m", "N/A") else "—"
        _mi_display = f"{mi} mi" if mi and mi > 0 else "—"

        st.markdown(f"""
<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:8px;">
    <div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div>
            <div style="font-size:20px; font-weight:900; color:{status_color};">${final_pay:,.2f}</div>
            <div style="font-size:10px; color:#94a3b8; margin-top:1px;">${final_rate}/stop</div>
        </div>
        <div style="text-align:right;">
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Drive Time</div>
            <div style="font-size:20px; font-weight:900; color:#0f172a;">{_t_display}</div>
            <div style="font-size:10px; color:#94a3b8; margin-top:1px;">Round Trip: {_mi_display}</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

        # ── ROUTE STOPS ─────────────────────────────────────────────────────
        hist = st.session_state.get(f"history_{cluster_hash}", [])
        if hist:
            st.markdown(f"<p style='color:#94a3b8; font-size:11px; margin-bottom:2px; font-weight:600;'>↩️ Previously sent to: {', '.join(hist)}</p>", unsafe_allow_html=True)

        # Build expandable stop rows with task pills in summary + campaign in expansion
        _dispatch_rows = []
        for addr, metrics in stop_metrics.items():
            # Icons only for address row summary (kiosk install gets its own green pill below
            # to match the right column's make_venue_details styling — was previously a plain
            # "🛠️ 1" inline icon with no color treatment).
            icon_parts = []
            if metrics['n_ad'] > 0: icon_parts.append("🆕")
            if metrics['c_ad'] > 0: icon_parts.append("🔄")
            if metrics['d_ad'] > 0: icon_parts.append("⚪")
            if metrics['remov'] > 0: icon_parts.append(f"🗑️ {metrics['remov']}")
            if metrics['custom']: icon_parts.append("📋")
            if metrics['digi_off'] > 0: icon_parts.append("📵")
            if metrics['digi_srv'] > 0: icon_parts.append("⚙️")
            pill_str = " ".join(icon_parts)
            # Green Kiosk pill — matches right column's make_venue_details treatment.
            k_tag_html = f" <span style='color:#16a34a;font-weight:800;font-size:10px;'>🛠️ {metrics['inst']} Kiosk</span>" if metrics['inst'] > 0 else ""
            # Digital Ins/Rem pill in matching teal — same visual rhythm as the kiosk pill.
            digi_ins_html = f" <span style='color:#0f766e;font-weight:800;font-size:10px;'>🔧 {metrics['digi_ins']} Ins/Rem</span>" if metrics['digi_ins'] > 0 else ""
            # Boosted / Local Plus pills — count of tasks at THIS stop with that tier.
            boost_html = f" <span style='color:#dc2626;font-weight:800;font-size:10px;'>🔥 {metrics['boost_cnt']}</span>" if metrics.get('boost_cnt', 0) > 0 else ""
            lplus_html = f" <span style='color:#ca8a04;font-weight:800;font-size:10px;'>⭐ {metrics['lplus_cnt']}</span>" if metrics.get('lplus_cnt', 0) > 0 else ""
            # Full icon+name for expansion
            expand_parts = []
            if metrics['n_ad'] > 0: expand_parts.append(f"🆕 {metrics['n_ad']} New Ad")
            if metrics['c_ad'] > 0: expand_parts.append(f"🔄 {metrics['c_ad']} Continuity")
            if metrics['d_ad'] > 0: expand_parts.append(f"⚪ {metrics['d_ad']} Default")
            if metrics['inst'] > 0: expand_parts.append(f"🛠️ {metrics['inst']} Install")
            if metrics['remov'] > 0: expand_parts.append(f"🗑️ {metrics['remov']} Removal")
            for cn, cnt in metrics['custom'].items(): expand_parts.append(f"📋 {cnt} {cn}")
            if metrics['digi_off'] > 0: expand_parts.append(f"📵 {metrics['digi_off']} Offline")
            if metrics['digi_ins'] > 0: expand_parts.append(f"🔧 {metrics['digi_ins']} Ins/Rem")
            if metrics['digi_srv'] > 0: expand_parts.append(f"⚙️ {metrics['digi_srv']} Service")
            expand_str = " | ".join(expand_parts)
            esc_count_stop = sum(1 for t in cluster['data'] if t.get('full') == addr and t.get('escalated'))
            esc_inline = f" <span style='color:#dc2626;font-weight:900;font-size:10px;'>❗ {esc_count_stop}</span>" if esc_count_stop > 0 else ""
            display_addr = f"+ {addr}" if metrics.get('is_new') else addr
            venue_prefix = f"<span style='color:#94a3b8;font-size:11px;font-weight:600;white-space:normal;'>{metrics['venue_name']} — </span>" if metrics.get('venue_name') else ""
            task_pill = f"<span style='color:#633094;background:#f3e8ff;padding:1px 5px;border-radius:8px;font-weight:800;font-size:10px;'>{metrics['t_count']} Tasks</span>"
            pill_html = f"<span style='font-size:11px;color:#94a3b8;'> — {pill_str}</span>" if pill_str else ""
            # Campaign expansion: aggregate by (campaign, task_type) with × N count
            # plus per-group escalation/boost/local-plus counters. Same shape as
            # make_venue_details so Sent/Accepted/Declined/Finalized/FN tabs match
            # what dispatchers see here in Ready/Flagged.
            from collections import defaultdict as _dd
            loc_tasks = [t for t in cluster['data'] if t.get('full') == addr]
            _camp_groups = _dd(lambda: {'count': 0, 'esc': 0, 'boost': 0, 'lplus': 0, 'tt_badge': '', 'cmp': ''})
            for t in loc_tasks:
                cmp = t.get('client_company','')
                if not cmp: continue
                tt = str(t.get('task_type','')).lower()
                if t.get('is_digital'):
                    if 'offline' in tt: tt_badge = "📵 Offline"
                    elif 'ins/re' in tt: tt_badge = "🔧 Ins/Rem"
                    else: tt_badge = "⚙️ Service"
                elif 'install' in tt: tt_badge = "🛠️ Install"
                elif any(x in tt for x in ['kiosk removal','remove kiosk']): tt_badge = "🗑️ Removal"
                elif any(x in tt for x in ['continuity','photo retake','swap']): tt_badge = "🔄 Continuity"
                elif any(x in tt for x in ['default','pull down']): tt_badge = "⚪ Default"
                elif any(x in tt for x in ['new ad','art change','top']) or not tt: tt_badge = "🆕 New Ad"
                else: tt_badge = f"📋 {tt.title()}"
                key = (cmp, tt_badge)
                grp = _camp_groups[key]
                grp['cmp'] = cmp
                grp['tt_badge'] = tt_badge
                grp['count'] += 1
                if t.get('escalated'): grp['esc'] += 1
                bs = str(t.get('boosted_standard','')).lower()
                if 'local plus' in bs: grp['lplus'] += 1
                elif 'boosted' in bs: grp['boost'] += 1
            camp_rows = []
            for (cmp, tt_badge), grp in _camp_groups.items():
                cnt = grp['count']
                count_suffix = f" <span style='color:#94a3b8;font-weight:600;'>× {cnt}</span>" if cnt > 1 else ""
                esc_pill   = f" <span style='color:#dc2626;font-weight:800;'>❗ {grp['esc']}</span>" if grp['esc'] > 0 else ""
                boost_pill = f" <span style='color:#dc2626;font-weight:800;'>🔥 {grp['boost']}</span>" if grp['boost'] > 0 else ""
                lplus_pill = f" <span style='color:#ca8a04;font-weight:800;'>⭐ {grp['lplus']}</span>" if grp['lplus'] > 0 else ""
                row = (
                    f"<div style='font-size:11px;color:#475569;padding:2px 4px;margin-top:3px;'>"
                    f"• <span style='color:#0f172a;font-weight:600;'>{cmp}</span>"
                    f"&nbsp;<span style='font-weight:700;color:#0f172a;'>{tt_badge}</span>"
                    f"{count_suffix}{esc_pill}{boost_pill}{lplus_pill}"
                    f"</div>"
                )
                camp_rows.append(row)
            camp_block = f"<div style='padding:6px 8px;background:#f8fafc;border-radius:6px;margin-top:4px;'>{''.join(camp_rows)}</div>" if camp_rows else ""
            _icon_html = f"<span style='font-size:13px;margin-left:6px;'>{pill_str}</span>" if pill_str else ""
            _dispatch_rows.append(
                f"<details class='fn-loc-row'>"
                f"<summary class='fn-loc-summary'>"
                f"<span class='fn-chevron'>›</span>"
                f"{venue_prefix}<span style='font-weight:700;color:#0f172a;'>{display_addr}</span>{k_tag_html}{digi_ins_html}{boost_html}{lplus_html}{esc_inline} &nbsp;{task_pill}{_icon_html}"
                f"</summary>{camp_block}</details>"
            )

        st.markdown(f"{VENUE_SECTION_CSS}<div style='background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;margin-bottom:8px;'><div style='background:#f8fafc;border-bottom:1px solid #e2e8f0;padding:6px 12px;'><span style='font-size:9px;font-weight:900;color:#94a3b8;text-transform:uppercase;letter-spacing:0.1em;'>Route Stops</span></div><div style='padding:2px 8px 4px 8px;'>{''.join(_dispatch_rows)}</div></div>", unsafe_allow_html=True)

        # 🔗 BUNDLE ROUTES — Apr 27 2026 (preview-driven).
        # Find OTHER unsent clusters in this pod within 50mi and offer them as a
        # multiselect. Selecting a route triggers a preview at the top of this fragment
        # which re-renders the entire card AS IF those routes were merged. The merge is
        # only committed to session_state when the dispatcher clicks "Confirm Bundle".
        # Deselecting any candidate reverts the card to its original state. Generate Link
        # is disabled while in preview so dispatch stays consistent with what was committed.
        _bundle_blocked_states = ('email_sent', 'field_nation', 'finalized')
        _current_route_state = st.session_state.get(f"route_state_{cluster_hash}")
        if (not is_sent and not is_declined
                and _current_route_state not in _bundle_blocked_states):
            BUNDLE_RADIUS_MI = 50
            _pod_clusters_for_bundle = st.session_state.get(f"clusters_{pod_name}", [])
            _nearby_bundles = []
            for _other in _pod_clusters_for_bundle:
                _o_tids = sorted([str(_t['id']).strip() for _t in _other.get('data', [])])
                if not _o_tids:
                    continue
                _o_hash = hashlib.md5("".join(_o_tids).encode()).hexdigest()
                if _o_hash == cluster_hash:
                    continue  # skip self
                # Don\'t suggest a cluster that\'s already been "absorbed" by the preview,
                # since the preview application above re-uses the same source-cluster IDs.
                _o_state = st.session_state.get(f"route_state_{_o_hash}")
                if _o_state in _bundle_blocked_states:
                    continue
                _o_sheet = st.session_state.get('sent_db', {}).get(
                    next((_tid for _tid in _o_tids if _tid in st.session_state.get('sent_db', {})), None)
                )
                if _o_sheet and not st.session_state.get(f"reverted_{_o_hash}", False):
                    _o_status = str(_o_sheet.get('status', '')).lower()
                    if _o_status in ('sent', 'accepted', 'declined', 'finalized', 'field_nation'):
                        continue
                if bool(_other.get('is_digital', False)) != bool(cluster.get('is_digital', False)):
                    continue
                if bool(_other.get('is_removal', False)) != bool(cluster.get('is_removal', False)):
                    continue
                _o_dist = haversine(cluster['center'][0], cluster['center'][1],
                                     _other['center'][0], _other['center'][1])
                if _o_dist > BUNDLE_RADIUS_MI:
                    continue
                _nearby_bundles.append((_o_dist, _o_hash, _other))
            _nearby_bundles.sort(key=lambda x: x[0])

            # If the preview has any selections that no longer appear as candidates
            # (e.g., the source cluster was just sent in another tab), prune them so
            # they don\'t silently linger in the multiselect\'s saved state.
            if _preview_hashes:
                _valid_now = {h for _, h, _ in _nearby_bundles}
                _stale = [h for h in _preview_hashes if h not in _valid_now]
                if _stale:
                    st.session_state[_bundle_select_key] = [h for h in _preview_hashes if h not in _stale]

            if _nearby_bundles or _in_preview:
                _opt_hashes = [h for _, h, _ in _nearby_bundles]
                _opt_label = {}
                for _d, _h, _o in _nearby_bundles:
                    _o_loc = f"{_o.get('city','')}, {_o.get('state','')}"
                    _o_stops = _o.get('stops', 0)
                    _o_tasks = len(_o.get('data', []))
                    _o_inst = _o.get('inst_count', 0)
                    _o_inst_pill = f" · 🛠️ {_o_inst}" if _o_inst > 0 else ""
                    _opt_label[_h] = f"{_o_loc} — {round(_d, 1)}mi · {_o_stops} stops · {_o_tasks} tasks{_o_inst_pill}"

                _banner_color = "#1e40af" if not _in_preview else "#b45309"
                _banner_bg    = "#eff6ff" if not _in_preview else "#fffbeb"
                _banner_border= "#3b82f6" if not _in_preview else "#f59e0b"
                _banner_text  = (f"🔗 Bundle Routes ({len(_nearby_bundles)} nearby) — select to preview"
                                 if not _in_preview else
                                 f"🔍 PREVIEW — {len(_preview_hashes)} route(s) merged · {_preview_added_count} tasks added · click Confirm to commit, deselect to revert")
                st.markdown(
                    f"<div style='background:{_banner_bg};border-left:3px solid {_banner_border};border-radius:6px;"
                    f"padding:8px 12px;margin:8px 0 4px 0;'>"
                    f"<div style='font-size:9px;font-weight:900;color:{_banner_color};text-transform:uppercase;"
                    f"letter-spacing:0.08em;'>{_banner_text}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # Multiselect — its saved value IS the preview state.
                st.multiselect(
                    "Bundle nearby routes",
                    options=_opt_hashes,
                    format_func=lambda h: _opt_label.get(h, h),
                    key=_bundle_select_key,
                    label_visibility="collapsed",
                    placeholder="Select nearby routes to preview a merged version...",
                )

                # Confirm button only shows while preview is active. No "Clear" button —
                # the multiselect chips have built-in × icons that achieve the same thing.
                if _in_preview:
                    if st.button(f"✅ Confirm Bundle ({len(_preview_hashes)} route{'s' if len(_preview_hashes)!=1 else ''}, +{_preview_added_count} tasks)",
                                 key=f"bundle_confirm_{cluster_hash}", use_container_width=True):
                        # Pull target + source task IDs from the live cluster store, commit
                        # the bundle to session_state[\'_bundle_map\'] (so it survives any
                        # future cluster rebuild — process_pod / smart_sync_pod / WS reset
                        # → re-init), then call _replay_bundles which does the actual merge.
                        # All merge logic lives in _replay_bundles — Confirm just records intent.
                        _store_key = _bundle_clusters_store(pod_name)
                        _live_clusters = st.session_state.get(_store_key, [])
                        _target_tids = set()
                        _source_tids = set()
                        _src_count = 0
                        for _cc in _live_clusters:
                            _cc_tids = sorted([str(_t['id']).strip() for _t in _cc.get('data', [])])
                            _cc_hash = hashlib.md5("".join(_cc_tids).encode()).hexdigest()
                            if _cc_hash == cluster_hash:
                                _target_tids = set(_cc_tids)
                            if _cc_hash in _preview_hashes:
                                _source_tids |= set(_cc_tids)
                                _src_count += 1
                        if _target_tids and _source_tids:
                            _commit_bundle(pod_name, _target_tids, _source_tids)
                            _replay_bundles(pod_name)
                            # cluster_hash changes on the next render (cluster.data changed),
                            # so pay_key/rate_key/sel_key/_bundle_select_key all become NEW
                            # keys with no saved state — Streamlit re-initializes the widgets
                            # fresh. No manual pop needed (post-render widget writes would
                            # crash anyway).
                            st.toast(f"🔗 Bundled {_src_count} route(s) — {_preview_added_count} tasks added")
                            st.rerun()

        if not is_sent and not is_declined and len(stop_metrics) > 1:
            _all_addrs = list(stop_metrics.keys())
            _ms_key = f"multi_split_{pod_name}_{cluster_hash}_{i}_{hashlib.md5(str(list(stop_metrics.keys())).encode()).hexdigest()[:4]}"
            _selected = st.multiselect(
                "Remove stops",
                options=_all_addrs,
                format_func=lambda x: f"{stop_metrics[x].get('venue_name','') + ' — ' if stop_metrics[x].get('venue_name') else ''}{x}",
                key=_ms_key,
                label_visibility="collapsed",
                placeholder="Select stops to remove from route..."
            )
            if _selected:
                if st.button(f"✂️ Remove {len(_selected)} Stop{'s' if len(_selected) > 1 else ''}", key=f"btn_{_ms_key}"):
                    for _addr in _selected:
                        tasks_to_move = [t for t in cluster['data'] if t['full'] == _addr]
                        if not tasks_to_move: continue
                        new_fragment = {
                            "data": tasks_to_move, "center": [tasks_to_move[0]['lat'], tasks_to_move[0]['lon']],
                            "stops": 1, "city": tasks_to_move[0]['city'], "state": tasks_to_move[0]['state'],
                            "status": "Ready", "has_ic": cluster.get('has_ic', False),
                            "esc_count": sum(1 for x in tasks_to_move if x.get('escalated')),
                            "is_digital": any(x.get('is_digital') for x in tasks_to_move),
                            "inst_count": sum(1 for x in tasks_to_move if "install" in str(x.get('task_type','')).lower()),
                            "remov_count": sum(1 for x in tasks_to_move if "remove" in str(x.get('task_type','')).lower()),
                            "wo": "none"
                        }
                        cluster['data'] = [t for t in cluster['data'] if t['full'] != _addr]
                        target_pod = pod_name if pod_name != "Global_Digital" else next((p for p, cfg in POD_CONFIGS.items() if new_fragment['state'] in cfg['states']), "UNKNOWN")
                        if target_pod != "UNKNOWN" and f"clusters_{target_pod}" in st.session_state:
                            st.session_state[f"clusters_{target_pod}"].append(new_fragment)
                    cluster['stops'] = len(set(t['full'] for t in cluster['data']))
                    st.session_state.pop(pay_key, None)
                    st.session_state.pop(rate_key, None)
                    st.toast(f"✂️ {len(_selected)} stop(s) broken off into standalone routes!")
                    st.rerun()





        stops_text = ""
        for i, (addr, metrics) in enumerate(list(stop_metrics.items())[:2], start=1):
            esc_star = "" if metrics['esc'] else ""
            stops_text += f"📍 Stop {i}: {esc_star}{addr}\n"
        
        if len(stop_metrics) > 2:
            stops_text += f"   ... and {len(stop_metrics) - 2} more stops.\n"

        loc_pills = {}
        for t in cluster['data']:
            addr = t.get('full', 'Unknown')
            if addr not in loc_pills: loc_pills[addr] = ""
            if t.get('escalated'): pass  # escalation shown in header only
        
            # 🌟 THE FIX: Split Digital Email Output
            if t.get('is_digital'):
                tt_lower = str(t.get('task_type','')).lower()
                if "offline" in tt_lower and "📵" not in loc_pills[addr]: loc_pills[addr] += "🔌"
                elif "ins/re" in tt_lower and "🔧" not in loc_pills[addr]: loc_pills[addr] += "🔧"
                elif ("offline" not in tt_lower and "ins/re" not in tt_lower) and "⚙️" not in loc_pills[addr]: 
                    loc_pills[addr] += "⚙️"
            else:
                if "install" in str(t.get('task_type','')).lower() and "🛠️" not in loc_pills[addr]: loc_pills[addr] += "🛠️"
                if str(t.get('task_type','')).lower() in ["kiosk removal", "remove kiosk"] and "🗑️" not in loc_pills[addr]: 
                    loc_pills[addr] += "🗑️"

        due = st.session_state.get(f"dd_{pod_name}_{cluster_hash}", datetime.now().date()+timedelta(DEFAULT_DUE_DAYS))
        is_already_sent = is_sent or is_declined or st.session_state.get(f"route_state_{cluster_hash}") == "email_sent"
    
        prev_ic_name = cluster.get('contractor_name', 'Unknown')
        ic_name = ic.get('name', 'Unknown Contractor') 
    
        if ic_name == prev_ic_name and cluster.get('wo', 'none') != 'none':
            wo_val = cluster['wo']
        else:
            _base_wo = f"{ic.get('name', 'Unknown')}-{datetime.now().strftime('%m%d%Y')}"
            # Compute next suffix from the max of (active sent WOs ∪ archived WOs) for this IC today.
            # Archived suffixes are "consumed" — we never recycle them, so a route created after an
            # archive will read -2 (or -N+1) instead of clobbering the archived -1.
            _local_sent_db = st.session_state.get('sent_db', {})
            _archived_wos_set = st.session_state.get('archived_wos', set()) or set()
            _candidate_wos = set()
            for _info in _local_sent_db.values():
                _w = str(_info.get('wo', '') or '')
                if _w.startswith(_base_wo):
                    _candidate_wos.add(_w)
            for _w in _archived_wos_set:
                if str(_w).startswith(_base_wo):
                    _candidate_wos.add(str(_w))
            _max_suffix = 0
            for _w in _candidate_wos:
                try:
                    _suffix = int(str(_w).rsplit('-', 1)[-1])
                    if _suffix > _max_suffix:
                        _max_suffix = _suffix
                except Exception:
                    pass
            _wo_num = _max_suffix + 1
            wo_val = f"{_base_wo}-{_wo_num}"
        # 🌟 NEW: Calculate route-level task breakdowns for the email preview
        route_task_counts = {}
        total_installs = 0
    
        for t in cluster['data']:
            raw_tt = str(t.get('task_type', '')).strip()
            clean_tt_lower = raw_tt.lower().replace("escalation", "").replace("  ", " ").strip(" ,-|:")
        
            # Default to New Ad if empty
            if not clean_tt_lower:
                clean_tt_lower = "new ad"
                display_tt = "New Ad"
            else:
                display_tt = clean_tt_lower.title()

            is_digi = t.get('is_digital')
            category = None
        
            # 🚦 Match exactly to the UI buckets
            if is_digi:
                if "offline" in clean_tt_lower: category = "📵 Offline"
                elif "ins/re" in clean_tt_lower: category = "🔧 Ins/Rem"
                else: category = "⚙️ Service"
            else:
                if "install" in clean_tt_lower: 
                    category = "🛠️ Kiosk Install"
                    total_installs += 1
                elif any(x in clean_tt_lower for x in ["kiosk removal", "remove kiosk"]): category = "🗑️ Kiosk Removal"
                elif any(x in clean_tt_lower for x in ["continuity", "photo retake", "swap"]): category = "🔄 Continuity"
                elif any(x in clean_tt_lower for x in ["default", "pull down"]): category = "⚪ Default"
                elif any(x in clean_tt_lower for x in ["new ad", "art change", "top"]): category = "🆕 New Ad"
                else: category = f"📋 {display_tt}" # Pass custom types straight through
            
            if category not in route_task_counts:
                route_task_counts[category] = 0
            route_task_counts[category] += 1

        # Format the breakdown list cleanly for the email
        task_breakdown_str = "\n".join([f"  {cat}: {count}" for cat, count in route_task_counts.items()]) + "\n"
    
        install_warning = f"\n⚠️ NOTE: This route contains Kiosk Installs. Please ensure you have adequate storage and vehicle space.\n" if total_installs > 0 else ""
    
        sig_preview = (
            f"Hello {ic.get('name', 'Contractor')},\n\n"
            f"We have a new route available for you to review.\n\n"
            f" Work Order: {wo_val}\n"
            f"📅 Due Date: {due.strftime('%A, %b %d, %Y')}\n"
            f" Total Stops: {cluster['stops']}\n"
            f" Estimated Compensation: ${final_pay:.2f}\n\n"
            f" Task Breakdown:\n"
            f"{task_breakdown_str}"
            f"{install_warning}\n"
            f"To view the complete route details—including total stops, estimated mileage, and time—please click the secure link below to access your Route Summary.\n\n"
            f"⚠️ ACTION REQUIRED:\n"
            f"You must confirm by selecting 'Accept' or 'Decline' directly through the portal link.\n\n"
            f"Route Summary Link:\n"
            f"{PORTAL_BASE_URL}?route={link_id}&v2=true"
        )
    
        # 🌟 UNIQUE KEY
        last_data_key = f"last_data_{pod_name}_{cluster_hash}"
        version_key = f"tx_ver_{pod_name}_{cluster_hash}"
        current_data_fingerprint = f"{ic.get('name', 'Unknown')}_{final_pay}_{due}_{wo_val}"
    
        if version_key not in st.session_state:
            st.session_state[version_key] = 1

        if st.session_state.get(last_data_key) != current_data_fingerprint:
            st.session_state[version_key] += 1
            st.session_state[last_data_key] = current_data_fingerprint
            st.session_state[f"tx_{pod_name}_{cluster_hash}_{st.session_state[version_key]}"] = sig_preview
    
        active_tx_key = f"tx_{pod_name}_{cluster_hash}_{st.session_state[version_key]}"

        if active_tx_key not in st.session_state:
            st.session_state[active_tx_key] = sig_preview
        elif real_id and "LINK_PENDING" in st.session_state[active_tx_key]:
            st.session_state[active_tx_key] = st.session_state[active_tx_key].replace("LINK_PENDING", real_id)
    
       # 🌟 UNIQUE KEY & PERFECT INDENTATION
        email_body_content = st.text_area("Email Content Preview", value=sig_preview, height=120, key=f"txt_area_{pod_name}_{current_data_fingerprint}_{cluster_hash}", disabled=not is_unlocked)

        # --- HIGH-SPEED DISPATCH BUTTON ---
        btn_label = "✉️ RESEND LINK & OPEN GMAIL" if is_already_sent else "🚀 GENERATE LINK & OPEN GMAIL"
        if is_fn:
            st.caption("📋 Email dispatch disabled — route is assigned to Field Nation.")

        if st.button(btn_label, type="primary", key=f"gbtn_{pod_name}_{cluster_hash}", disabled=not is_unlocked or is_fn or _in_preview, use_container_width=True, help=("Confirm or clear the bundle preview before dispatching." if _in_preview else None)):
            # 🛡️ STEP 1: FAST COLLISION CHECK — only block active sent routes (not revoked/declined)
            local_sent_db = st.session_state.get('sent_db', {})
            _active_statuses = ('sent',)
            collision = next(
                (tid for tid in task_ids
                 if tid in local_sent_db
                 and local_sent_db[tid].get('status', '').lower() in _active_statuses
                 and not st.session_state.get(f"reverted_{cluster_hash}", False)),
                None
            )

            if collision and not is_already_sent:
                st.error(f"🚫 COLLISION: Dispatched by someone else ({local_sent_db[collision]['name']}).")
                st.rerun()
                return

            # 🚀 STEP 2: PROCEED WITH DISPATCH
            _dispatch_result = {}
            with st.spinner("Generating link..."):
                home = ic.get('location', f"{cluster['center'][0]},{cluster['center'][1]}")
                # Build ordered task IDs from Google Maps waypoint order
                _addr_list = list(stop_metrics.keys())
                _ordered_addrs = [_addr_list[i] for i in _wp_order] if _wp_order else _addr_list
                _stop_order_ids = []
                for _oa in _ordered_addrs:
                    for _t in cluster['data']:
                        if _t.get('full') == _oa:
                            _stop_order_ids.append(_t['id'])

                payload = {
                    "cluster_hash": cluster_hash,
                    "icn": ic.get('name', 'Unknown'),
                    "ice": ic.get('email', ''),
                    "wo": wo_val, 
                    "city": cluster.get('city', 'Unknown'),
                    "state": cluster.get('state', 'Unknown'),
                    "due": str(due), "comp": final_pay, "lCnt": cluster['stops'], "mi": mi, "time": t_str,
                    "phone": str(ic.get('phone', '')),
                    "locs": " | ".join([home] + list(stop_metrics.keys()) + [home]),
                    "taskIds": ",".join(task_ids),
                    "tCnt": len(task_ids),
                    "kCnt": cluster.get('inst_count', 0),
                    "rCnt": cluster.get('remov_count', 0),
                    "dCnt": sum(1 for t in cluster['data'] if t.get('is_digital')),
                    "jobOnly": " | ".join([f"{addr} {pills}" for addr, pills in loc_pills.items()]),
                    "stopOrder": ",".join(str(tid) for tid in _stop_order_ids),
                    "stopData": json.dumps([{
                        "addr": addr,
                        "venue": metrics.get("venue_name", ""),
                        "t_count": metrics.get("t_count", 0),
                        "esc": metrics.get("esc", False),
                        "inst": metrics.get("inst", 0),
                        "remov": metrics.get("remov", 0),
                        "n_ad": metrics.get("n_ad", 0),
                        "c_ad": metrics.get("c_ad", 0),
                        "d_ad": metrics.get("d_ad", 0),
                        "campaigns": list({
                            (t.get("client_company",""), t.get("escalated",False), str(t.get("boosted_standard","")).lower()):
                            {"name": t.get("client_company",""), "esc": t.get("escalated",False), "bs": str(t.get("boosted_standard","")).lower()}
                            for t in cluster["data"] if t.get("full") == addr and t.get("client_company")
                        }.values())
                    } for addr, metrics in stop_metrics.items()])
                }
                try:
                    _dispatch_result = requests.post(GAS_WEB_APP_URL, json={"action": "saveRoute", "payload": payload}, timeout=25).json()
                except requests.exceptions.Timeout:
                    _dispatch_result = {"_timeout": True}
                except Exception as e:
                    _dispatch_result = {"_error": str(e)}

            # Spinner now closed — handle result
            if _dispatch_result.get("_timeout"):
                st.warning("⏱️ Google Sheets is taking too long. The route may still have saved — click **Generate Link** again to retry.")
            elif _dispatch_result.get("_error"):
                st.error(f"Connection Error: {_dispatch_result['_error']} — Please try again.")
            elif _dispatch_result.get("success"):
                final_route_id = _dispatch_result.get("routeId")
                st.session_state[sync_key] = final_route_id
                st.session_state[f"sent_ts_{cluster_hash}"] = datetime.now().strftime('%m/%d %I:%M %p')
                st.session_state[f"contractor_{cluster_hash}"] = ic.get('name', 'Unknown')
                st.session_state[f"wo_{cluster_hash}"] = wo_val
                # Stash comp + due locally so the post-dispatch card has correct values
                # even if the sheet readback is briefly stale (was previously rendering
                # $0 / N/A for ~15s while the cached fetch_sent_records_from_sheet expired).
                st.session_state[f"comp_{cluster_hash}"] = final_pay
                st.session_state[f"due_{cluster_hash}"] = str(due)
                st.session_state[f"route_state_{cluster_hash}"] = "email_sent"
                st.session_state[f"reverted_{cluster_hash}"] = False
                # Force the next render to re-pull the sheet so the new row is visible immediately.
                fetch_sent_records_from_sheet.clear()
                final_sig = email_body_content.replace("LINK_PENDING", final_route_id)
                subject_line = requests.utils.quote(f"Route Request | {wo_val}")
                body_content = requests.utils.quote(final_sig)
                gmail_url = f"https://mail.google.com/mail/?view=cm&fs=1&to={ic.get('email', '')}&su={subject_line}&body={body_content}"
                _link_ph = st.empty()
                _link_ph.success("✅ Link Live! Gmail opening...")
                # Desktop: fire popup via height=0 script (not blocked by browser)
                st.components.v1.html(f"<script>if(window.screen.width>768){{window.open('{gmail_url}','_blank');}}</script>", height=0)
                # Show a visible link unconditionally — desktop users hit by popup blockers
                # used to get nothing; mobile users use the mailto. Now both have a fallback.
                _mailto = f"mailto:{ic.get('email','')}?subject={subject_line}&body={body_content}"
                st.markdown(f"""<div style="display:flex;gap:8px;margin:6px 0;">
<a href="{gmail_url}" target="_blank"
style="flex:1;text-align:center;background:#633094;color:white;
padding:12px;border-radius:10px;font-weight:800;font-size:14px;
text-decoration:none;">📧 Open Gmail</a>
<a href="{_mailto}"
style="flex:1;text-align:center;background:#ffffff;color:#633094;border:1px solid #633094;
padding:12px;border-radius:10px;font-weight:800;font-size:14px;
text-decoration:none;">📨 Default Mail</a>
</div>""", unsafe_allow_html=True)
                time.sleep(1)
                _link_ph.empty()
                st.rerun()
    
    # --- 🌐 FIELD NATION PERSISTENCE (CHECKBOX) ---
    
    if route_state != "email_sent":
        # 🌟 UNIQUE KEY
        fn_checked = st.checkbox("🌐 Assign to Field Nation", value=is_fn, key=f"fn_check_{pod_name}_{cluster_hash}")
        
        if fn_checked and not is_fn:
            # 🌟 INSTANT UI UPDATE — Sheet write fires in background
            home = ic.get('location', f"{cluster['center'][0]},{cluster['center'][1]}")
            _fn_due = st.session_state.get(f"dd_{pod_name}_{cluster_hash}", datetime.now().date()+timedelta(DEFAULT_DUE_DAYS))
            fn_payload = {
                "cluster_hash": cluster_hash,
                "icn": "Field Nation",
                "city": cluster.get('city', 'Unknown'),
                "state": cluster.get('state', 'Unknown'),
                "taskIds": ",".join(task_ids),
                "wo": f"FN-{datetime.now().strftime('%m%d%Y')}",
                "due": str(_fn_due),
                "lCnt": cluster['stops'],
                "tCnt": len(task_ids),
                "kCnt": cluster.get('inst_count', 0),
                "locs": " | ".join([home] + list(stop_metrics.keys()) + [home])
            }

            save_fn_to_sheet(GAS_WEB_APP_URL, fn_payload, session_state=st.session_state)
            st.session_state[f"route_state_{cluster_hash}"] = "field_nation"
            st.session_state[f"reverted_{cluster_hash}"] = True  # 🌟 Block stale sheet match until background write completes
            st.toast("✅ Saved to Field Nation Tab")
            st.rerun()
        
        elif not fn_checked and is_fn:
            # 🌟 ADDED Safety check for Field Nation revocation
            with st.popover("🚨 Confirm Field Nation Revocation", use_container_width=True):
                st.error("Remove this route from Field Nation tracking?")
                # 🌟 THE FIX: Upgraded to a callback so it doesn't freeze the screen!
                st.button("🚨 Yes, Revoke FN", key=f"fn_rev_confirm_{pod_name}_{cluster_hash}", type="primary", use_container_width=True, on_click=revoke_field_nation, kwargs={"cluster_hash": cluster_hash, "pod_name": pod_name})
            st.stop()

    BG_COLOR = "#FEF9C3"
    TEXT_COLOR = "#854D0E"
    BORDER_COLOR = "#FACC15"

    if route_state == "field_nation":
        st.info("💡 Route is currently tracked in the Field Nation tab.")

        # 🌟 FIELD NATION BUTTONS
        _due = st.session_state.get(f"dd_{pod_name}_{cluster_hash}", datetime.now().date() + timedelta(DEFAULT_DUE_DAYS))
        _pay = st.session_state.get(pay_key, 0.0)
        fn_buf, _ = generate_fn_upload(stop_metrics, cluster, _due, _pay, cluster_hash)

        dl_col, link_col = st.columns(2)
        with dl_col:
            if fn_buf:
                st.download_button(
                    label="📥 Download FN Upload",
                    data=fn_buf,
                    file_name=f"FN_Upload_{cluster.get('city', 'Route')}_{datetime.now().strftime('%m%d%Y')}.csv",
                    mime="text/csv",
                    key=f"fn_dl_{cluster_hash}",
                    use_container_width=True
                )
        with link_col:
            st.link_button(
                "🌐 Open Field Nation",
                url="https://app.fieldnation.com/projects",
                use_container_width=True
            )

        # 🌟 UNIQUE KEY
        if st.button("📢 Mark as Posted (Move to Sent)", key=f"posted_{pod_name}_{cluster_hash}", type="primary", use_container_width=True):
            with st.spinner("Moving route to Sent database..."):
                try:
                    res = requests.post(GAS_WEB_APP_URL, json={"action": "postFieldNationRoute", "cluster_hash": cluster_hash}, timeout=25).json()
                    if res.get("success"):
                        st.session_state[f"route_state_{cluster_hash}"] = "email_sent"
                        st.session_state[f"contractor_{cluster_hash}"] = "Field Nation"
                        st.session_state[f"sent_ts_{cluster_hash}"] = datetime.now().strftime('%m/%d %I:%M %p')
                        st.session_state[f"sync_{cluster_hash}"] = res.get("routeId") 
                        st.toast("🚀 Moved to Sent in Google Sheets!")
                        st.rerun()
                    else:
                        st.error(f"Sheet Error: {res.get('error')}")
                except Exception as e:
                    st.error(f"Connection Failed: {e}")

                    
def smart_sync_pod(pod_name):
    """
    Fetches only NEW tasks from Onfleet not already tracked in session state.
    - New tasks within radius of existing cluster → appended, inherit IC + pricing
    - New tasks with no nearby cluster → new standalone cluster for dispatcher
    - New task addresses flagged with is_new=True for UI badge
    """
    config = POD_CONFIGS[pod_name]
    existing_clusters = st.session_state.get(f"clusters_{pod_name}", [])

    # Build set of all task IDs already tracked
    known_ids = set()
    for c in existing_clusters:
        for t in c.get('data', []):
            known_ids.add(str(t['id']).strip())

    _bar = st.progress(0, text="🔍 Checking Onfleet for new tasks...")

    def _tick(pct, msg):
        """Update progress bar AND re-render the spin-card overlay so the timer
        actually counts up during smart sync (was previously frozen at 0:00)."""
        try: _bar.progress(pct, text=msg)
        except Exception: pass
        _ov = st.session_state.get('_loading_overlay')
        _st = st.session_state.get('_loading_start')
        _pn = st.session_state.get('_loading_pod') or pod_name
        if _ov and _st:
            import time as _t
            _el = int(_t.time() - _st); _m = _el // 60; _s = _el % 60
            _ov.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #633094;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#633094;background:#f3e8ff;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Checking New Tasks — {_pn} Pod</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>{msg}</p>
                    <div class='dcc-pill'>⏱ {_m}:{_s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

    _tick(0, "🔍 Checking Onfleet for new tasks...")

    # Fetch teams
    APPROVED_TEAMS = [
        "a - escalation", "b - boosted campaigns", "b - local campaigns",
        "c - priority nationals", "cvs kiosk removal", "digital routes", "n - national campaigns"
    ]
    teams_res = requests.get("https://onfleet.com/api/v2/teams", headers=headers, timeout=15).json()
    target_team_ids = [t['id'] for t in teams_res if any(appr in str(t.get('name', '')).lower() for appr in APPROVED_TEAMS)]
    esc_team_ids = [t['id'] for t in teams_res if 'escalation' in str(t.get('name', '')).lower()]
    # Match the same membership logic as process_pod so CVS Kiosk Removal tasks pulled
    # in via Smart Sync still get the is_removal flag (and the correct 10-stop limit).
    cvs_remov_team_ids = [t['id'] for t in teams_res if 'cvs kiosk remov' in str(t.get('name', '')).lower()]

    # Fetch all current unassigned tasks
    time_window = int(time.time()*1000) - (45*24*3600*1000)
    url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}"
    all_tasks_raw = []
    _MAX_PAGES = 200  # was 50; same loop-guard pattern as process_pod
    _page = 0
    _seen_last_ids = set()
    while url and _page < _MAX_PAGES:
        _page += 1
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 429:
            time.sleep(2); continue
        if response.status_code != 200: break
        res_json = response.json()
        all_tasks_raw.extend(res_json.get('tasks', []))
        _next_id = res_json.get('lastId')
        if _next_id and _next_id in _seen_last_ids:
            _log_err("smart_sync_pod", f"lastId loop detected at {_next_id} (page {_page})")
            break
        if _next_id:
            _seen_last_ids.add(_next_id)
        url = f"https://onfleet.com/api/v2/tasks/all?state=0&from={time_window}&lastId={_next_id}" if _next_id else None
        # Tick on every page so the timer animates during pagination.
        _pct_p = min(0.05 + 0.30 * (len(all_tasks_raw) / max(500, len(all_tasks_raw))), 0.39)
        _tick(_pct_p, f"📡 Fetching tasks... {len(all_tasks_raw)} found")
    if _page >= _MAX_PAGES:
        _log_err("smart_sync_pod", f"hit pagination cap ({_MAX_PAGES} pages)")
        st.warning(f"⚠️ Hit pagination cap of {_MAX_PAGES} pages during Smart Sync. Some new tasks may be missing.")

    _tick(0.4, "🔎 Identifying new tasks...")

    # Filter to only NEW tasks for this pod
    fresh_sent_db, _, _archived_wos, _history_db = fetch_sent_records_from_sheet()
    st.session_state['_history_db'] = _history_db
    st.session_state['archived_wos'] = _archived_wos
    new_pool = []
    unique_tasks = {t['id']: t for t in all_tasks_raw}

    for t in unique_tasks.values():
        if str(t['id']).strip() in known_ids:
            continue

        # 🚫 DRIVER-HOME PSEUDO-TASK GUARD: Onfleet auto-generates
        # "Start at driver address" / "End at driver's address" tasks for native
        # Route Plans, bound to a contractor's home address. Real kiosk tasks
        # always carry a `state` custom field; the pseudo-tasks don't. Require it
        # so the pseudo-tasks never land in the dispatchable pool.
        _has_state_cf = any(
            (str(_f.get('name', '')).strip().lower() == 'state'
             or str(_f.get('key', '')).strip().lower() == 'state')
            and str(_f.get('value', '')).strip()
            for _f in (t.get('customFields') or [])
        )
        if not _has_state_cf:
            continue

        container = t.get('container', {})
        c_type = str(container.get('type', '')).upper()
        # 🛡️ DOUBLE-ROUTING GUARD: skip already-assigned tasks (Onfleet's state=0 filter
        # sometimes leaks WORKER-container tasks). Prevents Smart Sync from pulling in
        # tasks that another dispatcher (or auto-assign) already gave to a worker.
        if c_type == 'WORKER' or t.get('worker'):
            continue
        if c_type == 'TEAM' and container.get('team') not in target_team_ids:
            continue

        addr = t.get('destination', {}).get('address', {})
        stt = normalize_state(addr.get('state', ''))
        if stt not in config['states']:
            continue

        is_esc = (c_type == 'TEAM' and container.get('team') in esc_team_ids)

        # Run classification engine
        native_details = str(t.get('taskDetails', '')).strip()
        custom_fields = t.get('customFields') or []
        custom_task_type = ""
        custom_boosted = ""
        tt_val = native_details
        venue_name = ""; venue_id = ""; client_company = ""; campaign_name = ""; location_in_venue = ""

        for f in custom_fields:
            f_name = str(f.get('name', '')).strip().lower()
            f_key  = str(f.get('key', '')).strip().lower()
            f_val  = str(f.get('value', '')).strip()
            f_val_lower = f_val.lower()
            if f_name in ['task type', 'tasktype'] or f_key in ['tasktype', 'task_type']:
                custom_task_type = f_val_lower; tt_val = f_val
            if f_name in ['boosted standard', 'boostedstandard'] or f_key in ['boostedstandard', 'boosted_standard']:
                custom_boosted = f_val_lower
            if 'escalation' in f_name or 'escalation' in f_key:
                if f_val_lower in ['1', '1.0', 'true', 'yes'] or 'escalation' in f_val_lower:
                    is_esc = True
            if f_name in ['venuename', 'venue name'] or f_key in ['venuename', 'venue_name']:
                venue_name = f_val
            if f_name in ['venueid', 'venue id'] or f_key in ['venueid', 'venue_id']:
                venue_id = f_val
            if f_name in ['clientcompany', 'client company'] or f_key in ['clientcompany', 'client_company']:
                client_company = f_val
            if f_name in ['locationinvenue', 'location in venue'] or f_key in ['locationinvenue', 'location_in_venue']:
                location_in_venue = f_val
            if f_name in ['campaignname', 'campaign name'] or f_key in ['campaignname', 'campaign_name']:
                campaign_name = f_val  # 🌟 Captured separately so Client Company can't overwrite it

        # 🌟 Campaign Name always wins over Client Company for FN Customer Name
        client_company = campaign_name or client_company

        search_string = f"{native_details} {custom_task_type}".lower()
        REGULAR_EXEMPTIONS = ["photo", "magnet", "continuity", "new ad", "pull down", "kiosk", "escalation"]
        is_exempt = any(ex in search_string for ex in REGULAR_EXEMPTIONS)
        DIGITAL_WHITELIST = ["service", "ins/rem", "offline"]
        is_digital_task = False
        if not is_exempt:
            if any(trigger in custom_task_type for trigger in DIGITAL_WHITELIST):
                is_digital_task = True
            elif "digital" in custom_boosted:
                is_digital_task = True

        t_status = fresh_sent_db.get(t['id'], {}).get('status', 'ready').lower() if t['id'] in fresh_sent_db else 'ready'
        t_wo = fresh_sent_db.get(t['id'], {}).get('wo', 'none') if t['id'] in fresh_sent_db else 'none'

        # Match process_pod's logic: a task is "removal" only if it's on the CVS Kiosk
        # Removal team AND its task type contains a removal keyword. Without this, CVS
        # removal tasks pulled in via Smart Sync would cluster with regular installs and
        # inherit the 20-stop limit instead of the 10-stop CVS limit.
        _remov_keywords = ["kiosk removal", "remove kiosk"]
        _is_cvs_team = (c_type == 'TEAM' and container.get('team') in cvs_remov_team_ids)
        _is_removal = _is_cvs_team and any(kw in f"{native_details} {custom_task_type}".lower() for kw in _remov_keywords)

        new_pool.append({
            "id": t['id'],
            "city": addr.get('city', 'Unknown'),
            "state": stt,
            "full": f"{addr.get('number','')} {addr.get('street','')}, {addr.get('city','')}, {stt}",
            "zip": addr.get('postalCode', ''),
            "lat": t['destination']['location'][1],
            "lon": t['destination']['location'][0],
            "escalated": is_esc,
            "task_type": tt_val,
            "is_digital": is_digital_task,
            "is_removal": _is_removal,
            "boosted_standard": custom_boosted,
            "db_status": t_status,
            "wo": t_wo,
            "venue_name": venue_name,
            "venue_id": venue_id,
            "client_company": client_company,
            "location_in_venue": location_in_venue,
            "is_new": True,  # 🌟 Flag for UI badge
        })

    if not new_pool:
        _bar.empty()
        st.toast("✅ No new tasks found.")
        return

    _tick(0.7, f"📦 Merging {len(new_pool)} new tasks...")

    CLUSTER_RADIUS = 25  # miles

    unmatched = []
    for new_task in new_pool:
        merged = False
        for cluster in existing_clusters:
            dist = haversine(cluster['center'][0], cluster['center'][1], new_task['lat'], new_task['lon'])
            if dist <= CLUSTER_RADIUS:
                # Inherit cluster — append task
                cluster['data'].append(new_task)
                cluster['stops'] = len(set(x['full'] for x in cluster['data']))
                cluster['inst_count'] = sum(1 for x in cluster['data'] if "install" in str(x.get('task_type', '')).lower())
                cluster['remov_count'] = sum(1 for x in cluster['data'] if str(x.get('task_type', '')).lower() in ["kiosk removal", "remove kiosk"])
                cluster['esc_count'] = sum(1 for x in cluster['data'] if x.get('escalated'))
                merged = True
                break
        if not merged:
            unmatched.append(new_task)

    # Create new standalone clusters for unmatched tasks
    while unmatched:
        anc = unmatched.pop(0)
        group = [anc]
        remaining = []
        for t in unmatched:
            if haversine(anc['lat'], anc['lon'], t['lat'], t['lon']) <= CLUSTER_RADIUS:
                group.append(t)
            else:
                remaining.append(t)
        unmatched = remaining

        # Any-match boosted-tier (see process_pod for rationale).
        _ss_boosted_vals = [str(x.get('boosted_standard', '')).lower() for x in group if x.get('boosted_standard')]
        if any('local plus' in v for v in _ss_boosted_vals):
            _ss_boosted_tag = 'local plus'
        elif any('boosted' in v for v in _ss_boosted_vals):
            _ss_boosted_tag = 'boosted'
        else:
            _ss_boosted_tag = ''
        existing_clusters.append({
            "data": group,
            "center": [anc['lat'], anc['lon']],
            "stops": len(set(x['full'] for x in group)),
            "city": anc['city'], "state": anc['state'],
            "status": "Ready",
            "has_ic": False,
            "esc_count": sum(1 for x in group if x.get('escalated')),
            "is_digital": anc.get('is_digital', False),
            "is_removal": anc.get('is_removal', False),
            "boosted_tag": _ss_boosted_tag,
            "inst_count": sum(1 for x in group if "install" in str(x.get('task_type', '')).lower()),
            "remov_count": sum(1 for x in group if str(x.get('task_type', '')).lower() in ["kiosk removal", "remove kiosk"]),
            "wo": anc['wo']
        })

    st.session_state[f"clusters_{pod_name}"] = existing_clusters
    # Re-apply bundles in case Smart Sync introduced a new cluster that should have
    # been absorbed into an existing bundled route (or rebuilt sources of past bundles).
    _replay_bundles(pod_name)
    st.session_state['_worker_counts'] = fetch_worker_task_counts()
    _bar.empty()
    st.toast(f"✅ {len(new_pool)} new task(s) merged into {pod_name} routes.")


def make_venue_details(data):
    """Build expandable venue location rows from cluster task data.

    Per-stop header pills (only shown when count > 0):
      🛠️ {N} Kiosk     — install count (green)
      🔧 {N} Ins/Rem   — digital ins/rem count (teal)
      🔥 {N}           — boosted standard task count (red)
      ⭐ {N}              — local plus task count (amber)
      ❗ {N}              — escalation count (red)
      {N} Tasks              — total task count (purple pill)

    Each campaign row: • {campaign}  {task-type badge}  {markers}
    Task-type badge is 🛠️ Install / 🔄 Continuity / ⚪ Default / 🗑️ Removal /
    🆕 New Ad / 🔧 Ins/Rem / 📵 Offline / ⚙️ Service / 📋 Custom.
    Markers are ❗ escalation, 🔥 boosted, ⭐ local plus.
    """
    u_locs = []
    for t in data:
        if t['full'] not in u_locs: u_locs.append(t['full'])
    rows = []
    for loc in u_locs:
        loc_tasks = [t for t in data if t['full'] == loc]
        venue = next((t.get('venue_name','') for t in loc_tasks if t.get('venue_name')), '')

        # Per-stop metrics — same buckets the dispatch view computes.
        n_ad = c_ad = d_ad = inst = remov = digi_ins = digi_off = digi_srv = 0
        boost_cnt = lplus_cnt = 0
        custom_types = {}
        for t in loc_tasks:
            tt = str(t.get('task_type','')).lower()
            if t.get('is_digital'):
                if 'offline' in tt: digi_off += 1
                elif 'ins/re' in tt: digi_ins += 1
                else: digi_srv += 1
            elif 'install' in tt: inst += 1
            elif any(x in tt for x in ['kiosk removal','remove kiosk']): remov += 1
            elif any(x in tt for x in ['continuity','photo retake','swap']): c_ad += 1
            elif any(x in tt for x in ['default','pull down']): d_ad += 1
            elif any(x in tt for x in ['new ad','art change','top']) or not tt: n_ad += 1
            else:
                _label = tt.title() or 'Other'
                custom_types[_label] = custom_types.get(_label, 0) + 1
            # Boosted Standard tier: 'local plus' is its own subtype, otherwise any
            # 'boosted'-containing value counts as boosted.
            _bs = str(t.get('boosted_standard','')).lower()
            if 'local plus' in _bs: lplus_cnt += 1
            elif 'boosted' in _bs: boost_cnt += 1
        t_count = len(loc_tasks)
        esc_cnt = sum(1 for t in loc_tasks if t.get('escalated'))

        # Header pills — match dispatch view layout, plus boost/local-plus counts.
        k_tag       = f" <span style='color:#16a34a;font-weight:800;font-size:10px;'>🛠️ {inst} Kiosk</span>" if inst > 0 else ""
        digi_ins_tag= f" <span style='color:#0f766e;font-weight:800;font-size:10px;'>🔧 {digi_ins} Ins/Rem</span>" if digi_ins > 0 else ""
        boost_tag   = f" <span style='color:#dc2626;font-weight:800;font-size:10px;'>🔥 {boost_cnt}</span>" if boost_cnt > 0 else ""
        lplus_tag   = f" <span style='color:#ca8a04;font-weight:800;font-size:10px;'>⭐ {lplus_cnt}</span>" if lplus_cnt > 0 else ""
        esc_tag     = f" <span style='color:#dc2626;font-weight:900;font-size:10px;'>❗ {esc_cnt}</span>" if esc_cnt > 0 else ""
        t_pill      = f" <span style='color:#633094;background:#f3e8ff;padding:1px 5px;border-radius:8px;font-weight:800;font-size:10px;'>{t_count} Tasks</span>" if t_count else ""

        venue_prefix = f"<span style='color:#94a3b8;font-size:11px;font-weight:600;'>{venue} — </span>" if venue else ""

        # Campaign expansion: aggregate by (campaign, task type) so multiple tasks for
        # the same client+type at one stop collapse into one row with COUNTS — and the
        # marker pills (escalation, boosted, local plus) carry their per-group counts
        # too, mirroring the header style.
        from collections import defaultdict
        camp_groups = defaultdict(lambda: {'count': 0, 'esc': 0, 'boost': 0, 'lplus': 0, 'tt_badge': '', 'cmp': ''})
        for t in loc_tasks:
            cmp = t.get('client_company','')
            if not cmp: continue
            tt = str(t.get('task_type','')).lower()
            if t.get('is_digital'):
                if 'offline' in tt: tt_badge = "📵 Offline"
                elif 'ins/re' in tt: tt_badge = "🔧 Ins/Rem"
                else: tt_badge = "⚙️ Service"
            elif 'install' in tt: tt_badge = "🛠️ Install"
            elif any(x in tt for x in ['kiosk removal','remove kiosk']): tt_badge = "🗑️ Removal"
            elif any(x in tt for x in ['continuity','photo retake','swap']): tt_badge = "🔄 Continuity"
            elif any(x in tt for x in ['default','pull down']): tt_badge = "⚪ Default"
            elif any(x in tt for x in ['new ad','art change','top']) or not tt: tt_badge = "🆕 New Ad"
            else: tt_badge = f"📋 {tt.title()}"
            key = (cmp, tt_badge)
            grp = camp_groups[key]
            grp['cmp'] = cmp
            grp['tt_badge'] = tt_badge
            grp['count'] += 1
            if t.get('escalated'): grp['esc'] += 1
            bs = str(t.get('boosted_standard','')).lower()
            if 'local plus' in bs: grp['lplus'] += 1
            elif 'boosted' in bs: grp['boost'] += 1

        camp_rows = []
        for (cmp, tt_badge), grp in camp_groups.items():
            cnt = grp['count']
            count_suffix = f" <span style='color:#94a3b8;font-weight:600;'>× {cnt}</span>" if cnt > 1 else ""
            esc_pill   = f" <span style='color:#dc2626;font-weight:800;'>❗ {grp['esc']}</span>" if grp['esc'] > 0 else ""
            boost_pill = f" <span style='color:#dc2626;font-weight:800;'>🔥 {grp['boost']}</span>" if grp['boost'] > 0 else ""
            lplus_pill = f" <span style='color:#ca8a04;font-weight:800;'>⭐ {grp['lplus']}</span>" if grp['lplus'] > 0 else ""
            row = (
                f"<div style='font-size:11px;color:#475569;padding:2px 4px;margin-top:3px;'>"
                f"• <span style='color:#0f172a;font-weight:600;'>{cmp}</span>"
                f"&nbsp;<span style='font-weight:700;color:#0f172a;'>{tt_badge}</span>"
                f"{count_suffix}"
                f"{esc_pill}{boost_pill}{lplus_pill}"
                f"</div>"
            )
            camp_rows.append(row)
        camp_block = f"<div style='padding:6px 8px;background:#f8fafc;border-radius:6px;margin-top:4px;'>{''.join(camp_rows)}</div>" if camp_rows else ""

        rows.append(
            f"<details class='fn-loc-row'>"
            f"<summary class='fn-loc-summary'>"
            f"<span class='fn-chevron'>›</span>"
            f"{venue_prefix}<span style='font-weight:700;color:#0f172a;'>{loc}</span>"
            f"{k_tag}{digi_ins_tag}{boost_tag}{lplus_tag}{esc_tag} &nbsp;{t_pill}"
            f"</summary>{camp_block}</details>"
        )
    return "".join(rows)

def make_venue_details_ghost(locs_list, stop_data=None):
    """Expandable accordion rows for ghost routes. Uses rich stop_data if available."""
    # Build lookup by address from stop_data
    sd_map = {}
    if stop_data:
        for sd in stop_data:
            sd_map[sd.get('addr', '')] = sd

    rows = []
    for loc in locs_list:
        if " — " in loc:
            parts = loc.split(" — ", 1)
            venue_prefix = f"<span style='color:#94a3b8;font-size:11px;font-weight:600;'>{parts[0]} — </span>"
            addr = parts[1]
        else:
            venue_prefix = ""
            addr = loc

        sd = sd_map.get(addr) or sd_map.get(loc) or {}
        venue = sd.get('venue', '')
        if venue and not venue_prefix:
            venue_prefix = f"<span style='color:#94a3b8;font-size:11px;font-weight:600;'>{venue} — </span>"

        # Build icon summary
        icon_parts = []
        if sd.get('n_ad', 0) > 0: icon_parts.append("🆕")
        if sd.get('c_ad', 0) > 0: icon_parts.append("🔄")
        if sd.get('d_ad', 0) > 0: icon_parts.append("⚪")
        if sd.get('inst', 0) > 0: icon_parts.append(f"🛠️ {sd['inst']}")
        if sd.get('remov', 0) > 0: icon_parts.append(f"🗑️ {sd['remov']}")
        icon_html = f" <span style='font-size:12px;'>{' '.join(icon_parts)}</span>" if icon_parts else ""
        esc_html = f" <span style='color:#dc2626;font-weight:900;font-size:10px;'>❗</span>" if sd.get('esc') else ""

        t_pill = f" <span style='color:#633094;background:#f3e8ff;padding:1px 5px;border-radius:8px;font-weight:800;font-size:10px;'>{sd['t_count']} Tasks</span>" if sd.get('t_count') else ""

        # Build campaign expansion
        camps = sd.get('campaigns', [])
        camp_rows = []
        seen = set()
        for cp in camps:
            cname = cp.get('name', '')
            if not cname: continue
            badges = ""
            if cp.get('esc'): badges += " ❗"
            bs = cp.get('bs', '')
            if 'local plus' in bs: badges += " ⭐"
            elif 'boosted' in bs: badges += " 🔥"
            row = f"<div style='font-size:10px;color:#64748b;padding-left:4px;margin-top:2px;'>• {cname}{badges}</div>"
            if row not in seen:
                seen.add(row)
                camp_rows.append(row)

        camp_block = f"<div style='padding:6px 8px;background:#f8fafc;border-radius:6px;margin-top:4px;'>{''.join(camp_rows)}</div>" if camp_rows else                      f"<div style='padding:6px 8px;background:#f8fafc;border-radius:6px;margin-top:4px;font-size:10px;color:#94a3b8;'>No campaign data.</div>"

        rows.append(
            f"<details class='fn-loc-row'>"
            f"<summary class='fn-loc-summary'>"
            f"<span class='fn-chevron'>›</span>"
            f"{venue_prefix}<span style='font-weight:700;color:#0f172a;font-size:12px;'>{addr}</span>{esc_html}{t_pill}{icon_html}"
            f"</summary>{camp_block}</details>"
        )
    return "".join(rows)

VENUE_SECTION_CSS = """<style>
.fn-loc-row{border-bottom:1px solid #f1f5f9;}
.fn-loc-row:last-child{border-bottom:none;}
.fn-loc-summary{display:flex;align-items:flex-start;justify-content:flex-start;gap:6px;padding:7px 4px;font-size:12px;cursor:pointer;border-radius:6px;list-style:none;user-select:none;transition:background 0.15s ease;flex-wrap:wrap;}
.fn-loc-summary::-webkit-details-marker{display:none;}
.fn-loc-summary::marker{display:none;}
.fn-loc-summary:hover{background:#f8fafc;}
.fn-chevron{font-size:13px;color:#94a3b8;font-weight:300;transition:transform 0.2s ease;flex-shrink:0;margin-right:4px;}
details[open] .fn-chevron{transform:rotate(90deg);}
</style>"""

def venue_section(inner_html):
    """Wrap venue rows in the standard section container."""
    return f'{VENUE_SECTION_CSS}<div style="border-top:1px solid #e2e8f0;padding:6px 12px 8px 12px;"><div style="font-size:9px;font-weight:800;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:4px;">Venue Locations</div>{inner_html}</div>'

def run_pod_tab(pod_name):


    # Show toast only if a route in THIS pod changed
    sent_db = st.session_state.get('sent_db', {})
    pod_clusters = st.session_state.get(f"clusters_{pod_name}", [])
    pod_task_ids = set()
    for c in pod_clusters:
        for t in c.get('data', []):
            pod_task_ids.add(str(t['id']).strip())




    auto_sync_checker(pod_name)  # 🔄 Auto-detect accepted/declined routes every 15s

    # Grab the contractor database from session state
    ic_df = st.session_state.get('ic_df', pd.DataFrame())
    
    # Grab the matching "Midnight" text color for the current pod
    text_color = {
        "Blue": "#2563eb", "Green": "#16a34a", "Orange": "#ea580c",
        "Purple": "#9333ea", "Red": "#dc2626"
    }.get(pod_name, "#633094")
    
    # Check if data exists for this pod to determine button state
    is_initialized = f"clusters_{pod_name}" in st.session_state
    
    # 🌟 HEADER ROW: Title Centered, Dynamic Button Top Right
    h_col1, h_col2, h_col3 = st.columns([2, 6, 2])
    with h_col2:
        st.markdown(f"<h2 style='color: {text_color}; text-align:center; margin-top: 0;'>{pod_name} Pod Dashboard</h2>", unsafe_allow_html=True)
    with h_col3:
        st.markdown("<div class='tab-action-btn'>", unsafe_allow_html=True)
        if not is_initialized:
            # STATE 1: Not loaded yet
            init_clicked = st.button(f"🚀 Initialize Data", key=f"init_{pod_name}", use_container_width=True)
            sync_clicked = False
        else:
            # STATE 2: Loaded — smart sync for new tasks only
            init_clicked = False
            sync_clicked = st.button("🔄 Check New Tasks", key=f"reopt_{pod_name}", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # 🌟 Check New Tasks: same full-width overlay as Initialize so the dispatcher
    # sees the spin-card + timer instead of a bare progress bar while smart_sync_pod runs.
    if is_initialized and sync_clicked:
        import time as _time
        _start = _time.time()

        def _render_sync_card(overlay, pod, start):
            elapsed = int(_time.time() - start)
            m = elapsed // 60
            s = elapsed % 60
            overlay.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #633094;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#633094;background:#f3e8ff;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Checking New Tasks — {pod} Pod</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>Scanning Onfleet for tasks not yet tracked...</p>
                    <div class='dcc-pill'>⏱ {m}:{s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

        sync_overlay = st.empty()
        _render_sync_card(sync_overlay, pod_name, _start)

        # Stash so smart_sync_pod could tick the timer if we wire it up later.
        st.session_state['_loading_overlay'] = sync_overlay
        st.session_state['_loading_start'] = _start
        st.session_state['_loading_pod'] = pod_name

        smart_sync_pod(pod_name)

        sync_overlay.empty()
        st.session_state.pop('_loading_overlay', None)
        st.session_state.pop('_loading_start', None)
        st.session_state.pop('_loading_pod', None)
        st.rerun()

    # 🌟 FULL-WIDTH LOADING UI — outside columns so bar spans the page
    if not is_initialized and init_clicked:
        import time as _time
        _start = _time.time()

        def _render_card(overlay, pod, start):
            elapsed = int(_time.time() - start)
            m = elapsed // 60
            s = elapsed % 60
            overlay.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #633094;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#633094;background:#f3e8ff;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing {pod} Pod</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>Fetching tasks from Onfleet and building routes...</p>
                    <div class='dcc-pill'>⏱ {m}:{s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

        loading_overlay = st.empty()
        _render_card(loading_overlay, pod_name, _start)

        # Store start time and overlay in session state so process_pod can tick it
        st.session_state['_loading_overlay'] = loading_overlay
        st.session_state['_loading_start'] = _start
        st.session_state['_loading_pod'] = pod_name

        _bar = st.progress(0, text=f"🔌 Connecting to Onfleet...")
        _time.sleep(0.05)
        _bar.progress(0.03, text=f"⏳ Fetching {pod_name} tasks from Onfleet...")
        process_pod(pod_name, master_bar=_bar)

        loading_overlay.empty()
        _bar.empty()
        st.session_state.pop('_loading_overlay', None)
        st.session_state.pop('_loading_start', None)
        st.session_state.pop('_loading_pod', None)
        st.rerun()

    # 🌟 THE FIX: Remove the early return and safely default to an empty list
    # Load cluster data safely so the Supercards can render 0's
    cls = st.session_state.get(f"clusters_{pod_name}", [])

    # --- KEEPING THE CLEAN AUTO-SYNC LOGIC ---
    sent_db, ghost_db, _archived_wos, _history_db = fetch_sent_records_from_sheet()
    st.session_state['_history_db'] = _history_db
    st.session_state['archived_wos'] = _archived_wos
    # Merge real-time status updates from auto_sync_checker (overrides stale cache)
    _ss_db = st.session_state.get('sent_db', {})
    for _tid, _info in _ss_db.items():
        if _tid in sent_db and _info.get('status') != sent_db[_tid].get('status'):
            sent_db[_tid]['status'] = _info['status']
            # Clear reverted flag so traffic cop picks up the new status
            for _c in st.session_state.get(f"clusters_{pod_name}", []):
                _c_tids = [str(t['id']).strip() for t in _c.get('data', [])]
                if _tid in _c_tids:
                    _c_hash = hashlib.md5("".join(sorted(_c_tids)).encode()).hexdigest()
                    st.session_state[f"reverted_{_c_hash}"] = False
                    break

    # 🌟 THE FIX: Omni-Ghost Sorter
    pod_ghosts, finalized_ghosts, sent_ghosts = [], [], []
    seen_ghosts = set() # 🛡️ THE FIX: Streamlit Crash Shield
    
    for g in ghost_db.get(pod_name, []):
        g_hash = g.get('hash')
        
        # If the Google Sheet has duplicate rows, drop the clone instantly!
        if g_hash in seen_ghosts:
            continue
        seen_ghosts.add(g_hash)
        
        g_stat = g.get("status", "")
        local_override = st.session_state.get(f"route_state_{g_hash}")
        if local_override == "finalized" or g_stat == "finalized": finalized_ghosts.append(g)
        elif g_stat == "sent": sent_ghosts.append(g)
        else: pod_ghosts.append(g)

    # 1. 📂 DEFINE BUCKETS
    ready, review, sent, accepted, declined, finalized, field_nation, digital_ready = [], [], [], [], [], [], [], []
    live_hashes = set() # 🌟 Track live routes so we don't duplicate them!

    for c in cls:
        # 🌟 FIX: Skip empty routes that were trimmed to 0 stops
        if not c.get('data') or len(c.get('data')) == 0:
            continue
            
        task_ids = [str(t['id']).strip() for t in c['data']]
        cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
        live_hashes.add(cluster_hash) # Save hash
        
        sheet_match = sent_db.get(next((tid for tid in task_ids if tid in sent_db), None))
        route_state = st.session_state.get(f"route_state_{cluster_hash}")
        local_ts = st.session_state.get(f"sent_ts_{cluster_hash}", "")
        local_contractor = st.session_state.get(f"contractor_{cluster_hash}", "Unknown")
        local_wo = st.session_state.get(f"wo_{cluster_hash}", local_contractor) # 🌟 Fetch WO
        local_comp = st.session_state.get(f"comp_{cluster_hash}", 0)
        local_due = st.session_state.get(f"due_{cluster_hash}", 'N/A')
        is_reverted = st.session_state.get(f"reverted_{cluster_hash}", False)

        # NOTE: the next block mutates `c` (the cluster dict in session state) directly
        # on every rerender — contractor_name / route_ts / wo / comp / due are refreshed
        # from the sheet each pass. Once the user types into pay_key/rate_key those
        # session-state-scoped values take over for input rendering, so the cluster-level
        # `comp` overwrite here doesn't fight with user edits. Don't rely on these fields
        # being immutable — they're effectively a sheet-driven cache.
        if sheet_match and not is_reverted:
            c['contractor_name'] = sheet_match.get('name', 'Unknown')
            c['route_ts'] = sheet_match.get('time', '') or local_ts
            c['wo'] = sheet_match.get('wo', c['contractor_name'])
            # Sheet has authoritative values, but fall back to local session state if the
            # sheet readback hasn't caught up yet (e.g., immediately post-dispatch). This
            # was previously rendering $0 / N/A on the just-dispatched card for ~15s.
            c['comp'] = sheet_match.get('comp') or local_comp or 0
            c['due'] = sheet_match.get('due') or local_due or 'N/A'
        else:
            # 🌟 Apply Fallbacks Instantly
            c['contractor_name'] = local_contractor
            c['wo'] = local_wo
            c['route_ts'] = local_ts
            c['comp'] = local_comp
            c['due'] = local_due
        
        # --- 🚦 THE NEW DIGITAL FLOW ---
        if c.get('is_digital') and not sheet_match and route_state != "email_sent" and not is_reverted:
            digital_ready.append(c)
            continue 

        # --- PRIORITY: LIVE DATABASE OVERRIDES LOCAL STATE ---
        # 🌟 THE FIX: If we just clicked Finalize, override the Google Sheet instantly!
        if route_state == "finalized":
            finalized.append(c)
        elif sheet_match and not is_reverted:
            raw_status = str(sheet_match.get('status', '')).lower()
            if raw_status == 'field_nation':
                # 🌟 Restore session state so checkbox stays checked after reload
                if not st.session_state.get(f"route_state_{cluster_hash}"):
                    st.session_state[f"route_state_{cluster_hash}"] = "field_nation"
                field_nation.append(c)
            elif raw_status == 'declined': declined.append(c) #
            elif raw_status == 'accepted': accepted.append(c) #
            elif raw_status == 'finalized': finalized.append(c) #
            else: sent.append(c) #
        
        # 🌟 Handle Local Session State (Instant UI Moves)
        elif route_state == "email_sent" and not is_reverted:
            sent.append(c) #
        elif route_state == "field_nation": 
            field_nation.append(c) #
        else:
            # Fallback to calculated status
            if c.get('status') == 'Ready': ready.append(c) #
            else: review.append(c) #

    # --- 🐛 DEBUG: bucket routing visibility (collapsed by default, no behavior change) ---
    # Shows what each cluster's bucket decision was based on. Drop the URL-param check or
    # remove the whole block once we've confirmed auto-move works.
    if st.query_params.get("debug") == "1":
        with st.expander(f"🐛 Bucket debug — {pod_name}", expanded=False):
            _fp_now = st.session_state.get(f"_auto_sync_fp_{pod_name}", "(unset)")
            st.caption(f"Last fingerprint: `{_fp_now[:12]}...`  |  sent_db rows: {len(sent_db)}  |  pod_clusters: {len(cls)}")
            _bucket_map = [("ready", ready), ("review", review), ("sent", sent), ("accepted", accepted),
                           ("declined", declined), ("finalized", finalized), ("field_nation", field_nation),
                           ("digital_ready", digital_ready)]
            for _bname, _blist in _bucket_map:
                if not _blist:
                    continue
                st.markdown(f"**{_bname}** ({len(_blist)})")
                for _bc in _blist:
                    _btids = [str(_t['id']).strip() for _t in _bc.get('data', [])]
                    _bhash = hashlib.md5("".join(sorted(_btids)).encode()).hexdigest()[:8]
                    _bsm = sent_db.get(next((tid for tid in _btids if tid in sent_db), None))
                    _brs = st.session_state.get(f"route_state_{hashlib.md5(''.join(sorted(_btids)).encode()).hexdigest()}")
                    _brev = st.session_state.get(f"reverted_{hashlib.md5(''.join(sorted(_btids)).encode()).hexdigest()}", False)
                    _bsm_status = _bsm.get('status') if _bsm else "(no sheet match)"
                    st.text(f"  {_bhash} | {_bc.get('contractor_name','?'):20} | sheet={_bsm_status:10} | route_state={_brs} | reverted={_brev}")

    # --- 📊 CATEGORIZED MATH ---
    # Routes
    ready_count = len(ready)
    flagged_count = len(review)
    
    # 🌟 THE FIX: Combine active buckets (Excludes Accepted & Finalized)
    active_cls = ready + review + sent + declined + field_nation + digital_ready
    
    # Tasks
    tasks_static = sum(len(c['data']) for c in active_cls if not c.get('is_digital'))
    tasks_digital = sum(len(c['data']) for c in active_cls if c.get('is_digital'))

    # 📊 Supercard-excluded buckets (visible in Awaiting Confirmation but not counted in TASKS card)
    _excluded_accepted = sum(len(c['data']) for c in accepted)
    _excluded_finalized = sum(len(c['data']) for c in finalized)
    
    # Stops
    stops_static = sum(c['stops'] for c in active_cls if not c.get('is_digital'))
    stops_digital = sum(c['stops'] for c in active_cls if c.get('is_digital'))
    
    # Sent Records
    accepted_count = len(accepted) + len(pod_ghosts)
    declined_count = len(declined)
    total_sent = len(sent) + accepted_count + declined_count + len(field_nation)

    # --- DASHBOARD SUPERCARDS (Standardized 4-Card Layout) ---
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1]) 

    with c1:
        # CARD 1: ROUTE STATUS (Ready | Flagged)
        st.markdown(f"""
            <div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 120px;'>
                <p style='margin:0 0 10px 0; font-size:11px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Route Status</p>
                <div style='display:flex; justify-content:space-around; align-items:center; gap:8px;'>
                    <div style='background:{TB_GREEN_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_GREEN_TEXT};'>READY</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_GREEN_TEXT};'>{ready_count}</p>
                    </div>
                    <div style='background:{TB_RED_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_RED_TEXT};'>FLAGGED</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_RED_TEXT};'>{flagged_count}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with c2:
        # CARD 2: STATIC WORKLOAD (Tasks | Stops)
        st.markdown(f"""
            <div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 120px;'>
                <p style='margin:0 0 10px 0; font-size:11px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Static Workload</p>
                <div style='display:flex; justify-content:space-around; align-items:center; gap:8px;'>
                    <div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_STATIC_TEXT};'>TASKS</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_STATIC_TEXT};'>{tasks_static}</p>
                    </div>
                    <div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_STATIC_TEXT};'>STOPS</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_STATIC_TEXT};'>{stops_static}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with c3:
        # CARD 3: DIGITAL WORKLOAD (Updated to Static Theme)
        st.markdown(f"""
            <div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 120px;'>
                <p style='margin:0 0 10px 0; font-size:11px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Digital Workload</p>
                <div style='display:flex; justify-content:space-around; align-items:center; gap:8px;'>
                    <div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_STATIC_TEXT};'>TASKS</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_STATIC_TEXT};'>{tasks_digital}</p>
                    </div>
                    <div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_STATIC_TEXT};'>STOPS</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_STATIC_TEXT};'>{stops_digital}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    with c4:
        # CARD 4: SENT RECORDS (Accepted | Declined)
        st.markdown(f"""
            <div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 120px;'>
                <p style='margin:0 0 10px 0; font-size:11px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Sent: {total_sent}</p>
                <div style='display:flex; justify-content:space-around; align-items:center; gap:8px;'>
                    <div style='background:{TB_GREEN_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_GREEN_TEXT};'>ACCEPTED</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_GREEN_TEXT};'>{accepted_count}</p>
                    </div>
                    <div style='background:{TB_RED_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'>
                        <p style='margin:0; font-size:9px; font-weight:800; color:{TB_RED_TEXT};'>DECLINED</p>
                        <p style='margin:0; font-size:24px; font-weight:800; color:{TB_RED_TEXT};'>{declined_count}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    # --- 📊 TASK ATTRITION EXPANDER (collapsed by default) ---
    _attr = st.session_state.get(f'_attrition_{pod_name}')
    if _attr:
        with st.expander(f"📊 Task attrition — {pod_name} Pod", expanded=False):
            _raw = _attr.get('raw_fetched', 0)
            _ded = _attr.get('after_dedup', 0)
            _no_cf = _attr.get('skipped_no_state_cf', 0)
            _wrk = _attr.get('skipped_assigned_worker', 0)
            _team = _attr.get('skipped_wrong_team', 0)
            _oops = _attr.get('skipped_out_of_pod_states', 0)
            _pool = _attr.get('final_pool', 0)
            _sup_total = tasks_static + tasks_digital
            st.markdown(f"""
**Raw → Pool funnel (this pod's slice of the Onfleet pull)**

| Step | Count | Drop |
|---|---:|---:|
| 1. Raw fetched from Onfleet (`tasks/all?state=0`, last 45d) | **{_raw}** | — |
| 2. After dedup by task ID | {_ded} | {max(0, _raw - _ded)} |
| 3. After driver-home filter (require `state` custom field) | {_ded - _no_cf} | {_no_cf} |
| 4. After assigned-worker leak filter | {_ded - _no_cf - _wrk} | {_wrk} |
| 5. After approved-team filter | {_ded - _no_cf - _wrk - _team} | {_team} |
| 6. After in-pod-states filter | **{_pool}** | {_oops} |

**Pool → Supercard math (across buckets)**

| Bucket | Tasks | In supercard? |
|---|---:|---|
| Ready | {sum(len(c['data']) for c in ready)} | ✅ |
| Flagged (review) | {sum(len(c['data']) for c in review)} | ✅ |
| Sent | {sum(len(c['data']) for c in sent)} | ✅ |
| Declined | {sum(len(c['data']) for c in declined)} | ✅ |
| Field Nation | {sum(len(c['data']) for c in field_nation)} | ✅ |
| Digital Ready | {sum(len(c['data']) for c in digital_ready)} | ✅ |
| Accepted | {_excluded_accepted} | ❌ excluded |
| Finalized | {_excluded_finalized} | ❌ excluded |

**Supercard TASKS total: {_sup_total}**  (static {tasks_static} + digital {tasks_digital})
— excludes {_excluded_accepted + _excluded_finalized} accepted+finalized tasks that are still in Onfleet.
            """)

            # 📍 Stop-address breakdown for every excluded route so the dispatcher
            # can quickly cross-reference and re-route stale ones if needed.
            def _excluded_section(label, clusters):
                if not clusters:
                    return
                st.markdown(f"**{label} — {len(clusters)} route(s) | {sum(len(c['data']) for c in clusters)} tasks**")
                for _c in clusters:
                    _wo = _c.get('wo') or _c.get('contractor_name') or '(no WO)'
                    _ic = _c.get('contractor_name', '?')
                    _comp = _c.get('comp', 0)
                    _due = _c.get('due', 'N/A')
                    _addrs = []
                    for _t in _c.get('data', []):
                        _full = _t.get('full', '').strip()
                        if _full and _full not in _addrs:
                            _addrs.append(_full)
                    _addr_lines = '\n'.join(f"  - {a}" for a in _addrs) or "  (no addresses)"
                    st.markdown(
                        f"<div style='font-size:12px; padding:6px 10px; margin:4px 0; "
                        f"background:#f8fafc; border-left:3px solid #94a3b8; border-radius:4px;'>"
                        f"<b>{_wo}</b> &middot; {_ic} &middot; \${_comp} &middot; Due {_due} "
                        f"&middot; {len(_c.get('data', []))} tasks @ {len(_addrs)} stops"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    with st.expander(f"  Stops ({len(_addrs)})", expanded=False):
                        st.markdown(_addr_lines)

            _excluded_section("❌ Accepted (excluded)", accepted)
            _excluded_section("🏁 Finalized (excluded)", finalized)

    # 🌟 THE FIX: Force spacing before the Map
    st.markdown("<div style='margin-bottom: 25px;'></div>", unsafe_allow_html=True)
    
    # 🌟 Halt execution HERE, right after the cards render!
    if not is_initialized:
        st.info(f"No {pod_name} tasks initialized. Click '🚀 Initialize Data' at the top right.")
        return
        
    # 🌟 THE FIX: Don't hide the tab if there are pending sent routes!
    if not cls and not pod_ghosts and not sent_ghosts and not finalized_ghosts:
        st.info(f"No active tasks pending in the {pod_name} region.")
        return

    # 🌟 THE FIX: Prevent IndexError if there are Ghost routes but no Live routes!
    map_center = cls[0]['center'] if cls else [39.8283, -98.5795]
    m = folium.Map(location=map_center, zoom_start=6 if cls else 4, tiles="cartodbpositron")
    # 🗺️ Map shows only the operational buckets the dispatcher acts on:
    # Ready, Flagged (review), Field Nation, Sent. Accepted/Declined/Finalized
    # are deliberately excluded so the map stays a working surface, not a history view.
    # Digital is excluded too — it has its own dedicated map below.
    for c in ready:        folium.CircleMarker(c['center'], radius=8, color=TB_GREEN,   fill=True, opacity=0.8).add_to(m)
    for c in review:       folium.CircleMarker(c['center'], radius=8, color="#ef4444", fill=True, opacity=0.8).add_to(m)
    for c in field_nation: folium.CircleMarker(c['center'], radius=8, color="#ca8a04", fill=True, opacity=0.8).add_to(m)
    for c in sent:         folium.CircleMarker(c['center'], radius=8, color="#3b82f6", fill=True, opacity=0.8).add_to(m)
    # 📌 returned_objects=[] disables the map's rerun-on-interaction behavior.
    # Without this, every zoom/pan/click on the Leaflet map re-runs the entire
    # Streamlit script, causing the "page keeps refreshing" experience.
    st_folium(m, height=400, use_container_width=True, key=f"map_{pod_name}", returned_objects=[])
    
    st.markdown("""
<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; padding:14px 20px; margin-bottom:20px; box-shadow:0 2px 4px rgba(0,0,0,0.04);">
    <div style="font-size:10px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:12px;">📖 Route Key</div>
    <div style="display:grid; grid-template-columns:1fr 1fr 1fr 1fr 1fr 1fr; gap:12px;">
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Status</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="Route is within distance limits and standard rate — ready to dispatch.">🟢 Ready</span>
                <span title="Rate is $25+/stop or IC is 60+ miles away. Unlock required before sending.">🔒 Action Required</span>
                <span title="Route was flagged for review — low density or pricing issue.">🔴 Flagged</span>
                <span title="Route has been assigned to Field Nation for external dispatch.">🌐 Field Nation</span>
            </div>
        </div>
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Flags</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="Closest available IC is 60+ miles from the route center.">📡 Long Distance</span>
                <span title="Route consists exclusively of CVS Kiosk Removal tasks — capped at 10 stops.">🗑️ CVS Removal</span>
            </div>
        </div>
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Priority</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="Route contains one or more escalated tasks requiring priority handling.">❗ Escalation</span>
                <span title="Local Plus campaign — higher value placements in targeted local markets.">⭐ Local Plus</span>
                <span title="Boosted campaign — premium national or regional campaign with elevated priority.">🔥 Boosted</span>
            </div>
        </div>
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Task Types</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="New Ad: Fresh creative installation at this location.">🆕 New Ad</span>
                <span title="Continuity: Replacing an existing ad with updated creative.">🔄 Continuity</span>
                <span title="Default: Pull-down or placeholder installation.">⚪ Default</span>
                <span title="Kiosk Install: Physical kiosk installation at this stop.">🛠️ Kiosk Install</span>
                <span title="Kiosk Removal: Physical kiosk removal — CVS routes only.">🗑️ Kiosk Removal</span>
                <span title="Custom task type defined in Onfleet outside of standard categories.">📋 Custom</span>
            </div>
        </div>
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Contractor</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="Number of tasks currently assigned to this contractor in Onfleet.">🔵 Current Tasks</span>
            </div>
        </div>
        <div>
            <div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:6px;">Digital</div>
            <div style="display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155;">
                <span title="Digital Offline: Screen at this location has been reported offline.">📵 Offline</span>
                <span title="Digital Ins/Rem: Installation or removal of a digital screen unit.">🔧 Ins/Rem</span>
                <span title="Digital Service: Routine maintenance or software service of a digital screen.">⚙️ Service</span>
                <span title="Digital route — IC must be digital-certified to receive this route.">🔌 Digital</span>
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

    st.markdown("---")

    col_left, col_right = st.columns([5, 5])

    with col_left:
        st.markdown(f"<div style='font-size: 1.5rem; font-weight: 800; color: {TB_PURPLE}; text-align: center;'>🚀 Dispatch</div>", unsafe_allow_html=True)
        t_ready, t_flagged, t_fn, t_digital = st.tabs(["📥 Ready", "⚠️ Flagged", "🌐 Field Nation", "🔌 Digital"])

        with t_ready:
            if not ready: st.info("No tasks ready for dispatch.")
            else:
                sorted_ready = group_and_sort_by_proximity(ready)
                current_state = None
                for i, c in enumerate(sorted_ready):
                    # 🌟 Insert State Header
                    if c['state'] != current_state:
                        current_state = c['state']
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                        
                    badges = ""
                    if not ic_df.empty:
                        lat_col = next((col for col in ic_df.columns if str(col).strip().lower() == 'lat'), 'Lat')
                        lng_col = next((col for col in ic_df.columns if str(col).strip().lower() == 'lng'), 'Lng')
                        loc_col = next((col for col in ic_df.columns if str(col).strip().lower() == 'location'), 'Location')
                        if lat_col in ic_df.columns and lng_col in ic_df.columns:
                            v_ics = ic_df[~ic_df.astype(str).apply(lambda x: x.str.contains('Field Agent', case=False, na=False).any(), axis=1)].dropna(subset=[lat_col, lng_col]).copy()
                            if not v_ics.empty:
                                v_ics['d'] = v_ics.apply(lambda x: haversine(c['center'][0], c['center'][1], x[lat_col], x[lng_col]), axis=1)
                                closest_ic = v_ics.sort_values('d').iloc[0]
                                _, hrs, _, _ = get_gmaps(closest_ic[loc_col], [t['full'] for t in c['data'][:25]])
                                est_pay = hrs * 25.0 # 🌟 STRICTLY HOURLY
                                est_rate = est_pay / c['stops'] if c['stops'] > 0 else 0
                                if closest_ic['d'] > 60: badges += " 📡"

                    esc_pill = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                    inst_pill = f" | 🛠️ {c.get('inst_count', 0)} Installs" if c.get('inst_count', 0) > 0 else "" 
                    remov_pill = f" | 🗑️ {c.get('remov_count', 0)} Removal" if (c.get('remov_count', 0) > 0 and not c.get('is_removal')) else ""
                    remov_tag = f" 🗑️ CVS Removal — {c.get('remov_count', 0)} Units" if c.get('is_removal') else ""
                    _BOOSTED_BADGES = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                    boosted_pill = f" | {next((v for k,v in _BOOSTED_BADGES.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _BOOSTED_BADGES) else ""
                    with st.expander(f"{badges} 🟢 {c['city']}, {c['state']} | {c['stops']} Stops | 🗑️ CVS Kiosk Removal") if c.get('is_removal') else st.expander(f"{badges} 🟢 {c['city']}, {c['state']} | {c['stops']} Stops{inst_pill}{remov_pill}{boosted_pill}{esc_pill}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                        render_dispatch(i, c, pod_name)
                    
        with t_flagged:
            if not review: st.info("No flagged tasks requiring review.")
            else:
                sorted_review = group_and_sort_by_proximity(review)
                current_state = None
                for i, c in enumerate(sorted_review):
                    if c['state'] != current_state:
                        current_state = c['state']
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                    
                    esc_pill = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                    inst_pill = f" | 🛠️ {c.get('inst_count', 0)} Installs" if c.get('inst_count', 0) > 0 else ""
                    remov_pill = f" | 🗑️ {c.get('remov_count', 0)} Removal" if (c.get('remov_count', 0) > 0 and not c.get('is_removal')) else ""
                    remov_tag = f" 🗑️ CVS Removal — {c.get('remov_count', 0)} Units" if c.get('is_removal') else ""
                    _BOOSTED_BADGES = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                    boosted_pill = f" | {next((v for k,v in _BOOSTED_BADGES.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _BOOSTED_BADGES) else ""
                    with st.expander(f"🔒 🔴 {c['city']}, {c['state']} | {c['stops']} Stops | 🗑️ CVS Kiosk Removal") if c.get('is_removal') else st.expander(f"🔒 🔴 {c['city']}, {c['state']} | {c['stops']} Stops{inst_pill}{remov_pill}{boosted_pill}{esc_pill}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                        render_dispatch(i+1000, c, pod_name)

        with t_fn:
            if not field_nation: st.info("No routes currently moved to Field Nation.")
            else:
                sorted_fn = group_and_sort_by_proximity(field_nation)
                current_state = None
                for i, c in enumerate(sorted_fn):
                    if c['state'] != current_state:
                        current_state = c['state']
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                    
                    esc_pill = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                    digi_pill = " 🔌" if c.get('is_digital') else ""
                    inst_pill = f" | 🛠️ {c.get('inst_count', 0)} Installs" if c.get('inst_count', 0) > 0 else ""
                    remov_pill = f" | 🗑️ {c.get('remov_count', 0)} Removal" if c.get('remov_count', 0) > 0 else ""
                    _BOOSTED_BADGES = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                    boosted_pill = f" | {next((v for k,v in _BOOSTED_BADGES.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _BOOSTED_BADGES) else ""
                    
                    with st.expander(f"🌐 FN:{digi_pill} {c['city']}, {c['state']} | {c['stops']} Stops{inst_pill}{remov_pill}{boosted_pill}{esc_pill}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                        # 🌟 Guarantee route_state is set before render so FN card shows
                        _fn_task_ids = [str(t['id']).strip() for t in c['data']]
                        _fn_hash = hashlib.md5("".join(sorted(_fn_task_ids)).encode()).hexdigest()
                        if not st.session_state.get(f"route_state_{_fn_hash}"):
                            st.session_state[f"route_state_{_fn_hash}"] = "field_nation"

                        # ── FN LOCATION SUMMARY CARD ──────────────────────────────
                        _fn_stops, _fn_tasks = len(set(t['full'] for t in c['data'])), len(c['data'])
                        _fn_venues = venue_section(make_venue_details(c['data']))
                        st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;">
    <div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;">
        <span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span>
    </div>
    <div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div>
        <div style="font-size:14px; font-weight:800; color:#0f172a;">{_fn_stops} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {_fn_tasks} Tasks</span></div></div>
        <div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Status</div>
        <div style="font-size:13px; font-weight:700; color:#854d0e;">Field Nation</div></div>
    </div>
    {_fn_venues}
</div>""", unsafe_allow_html=True)

                        render_dispatch(i+5000, c, pod_name)
                    
        with t_digital:
            if not digital_ready: st.info("No digital service tasks pending.")
            else:
                sorted_digi = group_and_sort_by_proximity(digital_ready)
                current_state = None
                for i, c in enumerate(sorted_digi):
                    if c['state'] != current_state:
                        current_state = c['state']
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                    
                    _DIG_BOOSTED = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                    _dig_boosted_pill = f" | {next((v for k,v in _DIG_BOOSTED.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _DIG_BOOSTED) else ""
                    _dig_esc_pill = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                    with st.expander(f"🔌{c['city']}, {c['state']} | {c['stops']} Stops{_dig_boosted_pill}{_dig_esc_pill}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                        render_dispatch(i+7000, c, pod_name)
                    
    with col_right:
        st.markdown(f"<div style='font-size: 1.5rem; font-weight: 800; color: {TB_GREEN}; margin-bottom: 5px; text-align: center;'>⏳ Awaiting Confirmation</div>", unsafe_allow_html=True)
        t_sent, t_acc, t_dec, t_fin = st.tabs(["✉️ Sent", "✅ Accepted", "❌ Declined", "🏁 Finalized"])
        
        with t_sent:
            unified_sent = unify_and_sort_by_date(sent, sent_ghosts, live_hashes)
            if not unified_sent: st.info("No pending routes sent.")
            
            current_date = None
            for i, item in enumerate(unified_sent):
                date_str = item['sort_date']
                if date_str != current_date:
                    current_date = date_str
                    st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 SENT: {current_date}</div>", unsafe_allow_html=True)
                
                if not item['is_ghost']:
                    c = item
                    ic_name = c.get('contractor_name', 'Unknown')
                    task_ids = [str(tid['id']).strip() for tid in c['data']]
                    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                    comp, due = c.get('comp', 0), c.get('due', 'N/A')
                    tasks_cnt, stops_cnt = len(c['data']), c['stops']
                    wo_display = c.get('wo', ic_name)
                    _pill_sent = get_task_pill(c.get('data', []))
                    
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        with st.expander(f"✉️ {wo_display} | ${comp} | Due: {due}{_pill_sent}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            _venues_html = venue_section(make_venue_details(c['data']))
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;">
    <div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;">
        <span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span>
    </div>
    <div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div>
        <div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div>
        <div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div>
        <div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div>
    </div>
    <div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div>
        <div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div>
        <div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div>
        <div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div>
    </div>
    {_venues_html}
</div>""", unsafe_allow_html=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:13px; text-align:center;'>Re-route from <b>{ic_name}</b>?</p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Re-Route", key=f"rev_sent_live_{cluster_hash}_{pod_name}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": pod_name, "action_label": "Re-Routed", "check_onfleet": True, "cluster_data": c})
                else:
                    g = item
                    g_ic_name = g.get('contractor_name', 'Unknown')
                    ghost_hash = g.get('hash', f"ghost_sent_{i}")
                    wo_display = g.get('wo', g_ic_name)
                    comp, due = g.get('pay', 0), g.get('due', 'N/A')
                    stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                    
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        with st.expander(f"✉️ {wo_display} | ${comp} | Due: {due}  ·  :gray[{tasks_cnt} tasks]"):
                            raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                            if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                            else: task_locs = raw_locs
                            u_locs = list(dict.fromkeys(task_locs))
                            _gvenues_html = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;">
    <div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;">
        <span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span>
    </div>
    <div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div>
        <div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name}</div></div>
        <div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div>
        <div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div>
    </div>
    <div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;">
        <div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div>
        <div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div>
        <div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div>
        <div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div>
    </div>
    {_gvenues_html}
</div>""", unsafe_allow_html=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:13px; text-align:center;'>Re-route from <b>{g_ic_name}</b>?</p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Re-Route", key=f"rev_ghost_sent_{ghost_hash}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": pod_name, "action_label": "Re-Routed", "check_onfleet": True, "cluster_data": g})
                            
        with t_acc:
            unified_acc = unify_and_sort_by_date(accepted, pod_ghosts, live_hashes)
            if not unified_acc: st.info("Waiting for portal acceptances...")
            
            current_date = None
            for i, item in enumerate(unified_acc):
                date_str = item['sort_date']
                if date_str != current_date:
                    current_date = date_str
                    st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 ACCEPTED: {current_date}</div>", unsafe_allow_html=True)
                
                if not item['is_ghost']:
                    c = item
                    ic_name = c.get('contractor_name', 'Unknown')
                    task_ids = [str(tid['id']).strip() for tid in c['data']]
                    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                    comp, due = c.get('comp', 0), c.get('due', 'N/A')
                    tasks_cnt, stops_cnt = len(c['data']), c['stops']
                    
                    _k_by_addr = {}
                    for _tk in c['data']:
                        if any(kw in str(_tk.get('task_type','')).lower() for kw in ['kiosk install','install']):
                            _addr = _tk['full']
                            _venue = _tk.get('venue_name', '') or _addr
                            _k_by_addr[_venue] = _k_by_addr.get(_venue, 0) + 1
                    _k_total = sum(_k_by_addr.values())
                    _k_pill = f" | 🛠️ {_k_total} Kiosk" if _k_total > 0 else ""
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        with st.expander(f"✅ {c.get('wo', ic_name)} | ${comp} | Due: {due}" + (f" | 🛠️ {_k_total}" if _k_total > 0 else "") + f"  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            u_locs = []
                            for tk in c['data']:
                                if tk['full'] not in u_locs: u_locs.append(tk['full'])
                            loc_rows = []
                            for l in u_locs:
                                _venue_key = next((_tk.get('venue_name','') for _tk in c['data'] if _tk['full'] == l and _tk.get('venue_name')), '')
                                _k_cnt = sum(1 for _tk in c['data'] if _tk['full'] == l and 'install' in str(_tk.get('task_type','')).lower())
                                _k_tag = f" <span style='color:#16a34a; font-weight:800;'>🛠️ {_k_cnt} Kiosk</span>" if _k_cnt > 0 else ""
                                _v_prefix = f"<span style='color:#94a3b8; font-weight:600;'>{_venue_key} — </span>" if _venue_key else ""
                                loc_rows.append(f"<li>{_v_prefix}{l}{_k_tag}</li>")
                            _acc_venues_html = venue_section(make_venue_details(c['data']))
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_acc_venues_html}</div>""", unsafe_allow_html=True)
                            render_finalization_checklist(cluster_hash, pod_name, "chk")
                            if _k_total > 0:
                                st.link_button("🛍️ Order Kiosks on Shopify", url="https://admin.shopify.com/store/terraboost/draft_orders/new", use_container_width=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{c.get('wo', ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Remove", key=f"rev_acc_{cluster_hash}_{pod_name}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": pod_name, "cluster_data": c, "check_completed": True})
                else:
                    g = item
                    g_ic_name = g.get('contractor_name', 'Unknown')
                    ghost_hash = g.get('hash', f"ghost_{i}")
                    comp, due = g.get('pay', 0), g.get('due', 'N/A')
                    stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                    
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        _gk_total = g.get('kCnt', 0) or 0
                        _gk_pill = f" | 🛠️ {_gk_total} Kiosk" if _gk_total > 0 else ""
                        with st.expander(f"✅ {g.get('wo', g_ic_name)} | ${comp} | Due: {due}" + (f" | 🛠️ {_gk_total}" if _gk_total > 0 else "") + f"  ·  :gray[{tasks_cnt} tasks]"):
                            raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                            if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                            else: task_locs = raw_locs
                            u_locs = list(dict.fromkeys(task_locs))
                            _gacc_venues = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_gacc_venues}</div>""", unsafe_allow_html=True)
                            render_finalization_checklist(ghost_hash, pod_name, "g_chk")
                            if _gk_total > 0:
                                st.link_button("🛍️ Order Kiosks on Shopify", url="https://admin.shopify.com/store/terraboost/draft_orders/new", use_container_width=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{g_ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{g.get('wo', g_ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Remove", key=f"rev_ghost_{ghost_hash}_{i}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": pod_name, "action_label": "Ghost Archived", "check_onfleet": True, "cluster_data": g, "check_completed": True})
                    
        with t_dec:
            unified_dec = unify_and_sort_by_date(declined, [], live_hashes)
            if not unified_dec: st.info("No declined routes.")
            
            current_date = None
            for i, item in enumerate(unified_dec):
                date_str = item['sort_date']
                if date_str != current_date:
                    current_date = date_str
                    st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 DECLINED: {current_date}</div>", unsafe_allow_html=True)
                
                c = item
                ic_name = c.get('contractor_name', 'Unknown')
                task_ids = [str(tid['id']).strip() for tid in c['data']]
                cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                with exp_col:
                    comp_dec = c.get('comp', 0)
                    due_dec = c.get('due', 'N/A')
                    stops_dec, tasks_dec = c['stops'], len(c['data'])
                    _pill_dec = get_task_pill(c.get('data', []))
                    with st.expander(f"❌ {c.get('wo', ic_name)} | ${comp_dec} | Due: {due_dec}{_pill_dec}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                        u_locs_dec = list(dict.fromkeys(t['full'] for t in c['data']))
                        _dec_venues = venue_section(make_venue_details(c['data']))
                        st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_dec} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_dec} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due_dec}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp_dec}</div></div></div>{_dec_venues}</div>""", unsafe_allow_html=True)
                with btn_col:
                    with st.popover("↩️"):
                        st.markdown(f"<p style='font-size:13px; text-align:center;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</p>", unsafe_allow_html=True)
                        st.button("🚨 Yes, Remove", key=f"rev_dec_{cluster_hash}_{pod_name}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": pod_name, "cluster_data": c})
                    
        with t_fin:
            unified_fin = unify_and_sort_by_date(finalized, finalized_ghosts, live_hashes)
            if not unified_fin: st.info("No finalized routes.") 
            
            current_date = None
            for i, item in enumerate(unified_fin):
                date_str = item['sort_date']
                if date_str != current_date:
                    current_date = date_str
                    st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 FINALIZED: {current_date}</div>", unsafe_allow_html=True)
                
                if not item['is_ghost']:
                    c = item
                    ic_name = c.get('contractor_name', 'Unknown')
                    task_ids = [str(tid['id']).strip() for tid in c['data']]
                    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                    comp, due = c.get('comp', 0), c.get('due', 'N/A')
                    tasks_cnt, stops_cnt = len(c['data']), c['stops']
                    
                    _fk_by_addr = {}
                    for _tk in c['data']:
                        if any(kw in str(_tk.get('task_type','')).lower() for kw in ['kiosk install','install']):
                            _addr = _tk['full']
                            _venue = _tk.get('venue_name', '') or _addr
                            _fk_by_addr[_venue] = _fk_by_addr.get(_venue, 0) + 1
                    _fk_total = sum(_fk_by_addr.values())
                    _fk_pill = f" | 🛠️ {_fk_total} Kiosk" if _fk_total > 0 else ""
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        with st.expander(f"🏁 {c.get('wo', ic_name)} | ${comp} | Due: {due}" + (f" | 🛠️ {_fk_total}" if _fk_total > 0 else "") + f"  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            u_locs = []
                            for tk in c['data']:
                                if tk['full'] not in u_locs: u_locs.append(tk['full'])
                            loc_rows = []
                            for l in u_locs:
                                _fvk = next((_tk.get('venue_name','') for _tk in c['data'] if _tk['full'] == l and _tk.get('venue_name')), '')
                                _k_cnt = sum(1 for _tk in c['data'] if _tk['full'] == l and 'install' in str(_tk.get('task_type','')).lower())
                                _k_tag = f" <span style='color:#16a34a; font-weight:800;'>🛠️ {_k_cnt} Kiosk</span>" if _k_cnt > 0 else ""
                                _fv_prefix = f"<span style='color:#94a3b8; font-weight:600;'>{_fvk} — </span>" if _fvk else ""
                                loc_rows.append(f"<li>{_fv_prefix}{l}{_k_tag}</li>")
                            _fin_venues = venue_section(make_venue_details(c['data']))
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_fin_venues}</div>""", unsafe_allow_html=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{c.get('wo', ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Remove", key=f"rev_fin_{cluster_hash}_{pod_name}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": pod_name, "cluster_data": c, "check_completed": True})
                else:
                    g = item
                    g_ic_name = g.get('contractor_name', 'Unknown')
                    ghost_hash = g.get('hash', f"ghost_fin_{i}")
                    wo_display = g.get('wo', g_ic_name)
                    comp, due = g.get('pay', 0), g.get('due', 'N/A')
                    stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                    
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        _gfk_total = g.get('kCnt', 0) or 0
                        _gfk_pill = f" | 🛠️ {_gfk_total} Kiosk" if _gfk_total > 0 else ""
                        with st.expander(f"🏁 {wo_display} | ${comp} | Due: {due}" + (f" | 🛠️ {_gfk_total}" if _gfk_total > 0 else "") + f"  ·  :gray[{tasks_cnt} tasks]"):
                            raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                            if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                            else: task_locs = raw_locs
                            u_locs = list(dict.fromkeys(task_locs))
                            _gfin_venues = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                            g_ic_name_fin = g.get('contractor_name', 'Unknown')
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name_fin}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_gfin_venues}</div>""", unsafe_allow_html=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{g_ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{g.get('wo', g_ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Remove", key=f"rev_ghost_fin_{ghost_hash}_{i}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": pod_name, "action_label": "Ghost Archived", "check_onfleet": True, "cluster_data": g, "check_completed": True})
                
# --- START ---
if "ic_df" not in st.session_state:
    try:
        url = f"{IC_SHEET_URL.split('/edit')[0]}/export?format=csv&gid=0"
        df = pd.read_csv(url)
        # 🌟 BULLETPROOF: Lowercase all headers the second the data is downloaded
        df.columns = [str(c).strip().lower() for c in df.columns]
        st.session_state.ic_df = df
    except: st.error("Database connection failed.")

# --- HEADER ROW ---
st.markdown("<h1 style='color: #633094;'>Terraboost Media: Dispatch Command Center</h1>", unsafe_allow_html=True)

# Updated Main Tabs
tabs = st.tabs(["Global", "Blue Pod", "Green Pod", "Orange Pod", "Purple Pod", "Red Pod", "Digital"])
# --- TAB 0: GLOBAL CONTROL ---
with tabs[0]:
    # Check if ANY pod is loaded to toggle button state
    has_global_data = any(f"clusters_{p}" in st.session_state for p in POD_CONFIGS.keys())
    
    # 🌟 NEW HEADER: Title Centered, Dynamic Button Top Right
    gh_col1, gh_col2, gh_col3 = st.columns([2, 6, 2])
    with gh_col2:
        st.markdown("<h2 style='color: #633094; text-align:center; margin-top: 0;'>🌍 Global Overview</h2>", unsafe_allow_html=True)
    with gh_col3:
        st.markdown("<div class='tab-action-btn'>", unsafe_allow_html=True)
        btn_label = "🚀 Sync Routes" if has_global_data else "🚀 Initialize All Pods"
        if st.button(btn_label, key="global_init_btn", use_container_width=True):
            st.session_state.sent_db, st.session_state.ghost_db, st.session_state['archived_wos'], st.session_state['_history_db'] = fetch_sent_records_from_sheet()
            st.session_state.trigger_pull = True
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    loading_placeholder = st.empty()
    bar_placeholder = st.empty()
    if not has_global_data:
        st.info("No operational data initialized. Click '🚀 Initialize All Pods' at the top right to fetch tasks across all pods.")

    if st.session_state.get("trigger_pull"):
        st.markdown("<style>.pod-card-pill { opacity: 0.35 !important; filter: grayscale(40%) !important; pointer-events: none !important; transition: opacity 0.3s ease !important; }</style>", unsafe_allow_html=True)

    cols = st.columns(len(POD_CONFIGS))
    pod_keys = list(POD_CONFIGS.keys())
    global_map = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles="cartodbpositron")
    current_sent_db, ghost_db, _archived_wos, _history_db = fetch_sent_records_from_sheet()
    st.session_state['_history_db'] = _history_db
    st.session_state['archived_wos'] = _archived_wos

    for i, pod in enumerate(pod_keys):
        colors = {
            "Blue":   {"border": "#3b82f6", "bg": "#f0f7ff", "text": "#1e3a8a"},
            "Green":  {"border": "#22c55e", "bg": "#f0fdf4", "text": "#064e3b"},
            "Orange": {"border": "#f97316", "bg": "#fffaf5", "text": "#7c2d12"},
            "Purple": {"border": "#a855f7", "bg": "#faf5ff", "text": "#4c1d95"},
            "Red":    {"border": "#ef4444", "bg": "#fef2f2", "text": "#7f1d1d"}
        }.get(pod)
        
        with cols[i]:
            is_loading = st.session_state.get("current_loading_pod") == pod
            has_data = f"clusters_{pod}" in st.session_state
            
            if is_loading:
                card_content = f"<p class='loading-pulse' style='color:{colors['border']}; margin-top:25px;'>📡 SYNCING...</p>"
            elif has_data:
                pod_cls = st.session_state[f"clusters_{pod}"]
                total_routes = len(pod_cls)
                total_tasks = sum(len(c['data']) for c in pod_cls)
                total_stops = sum(c['stops'] for c in pod_cls)
                
                # 🌟 THE FIX: Initialize all required lists for the Global summary
                sent, accepted, declined, field_nation, ready, review, finalized = [], [], [], [], [], [], []
                
                for c in pod_cls:
                    task_ids = [str(t['id']).strip() for t in c['data']]
                    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                    sheet_match = current_sent_db.get(next((tid for tid in task_ids if tid in current_sent_db), None))
                    route_state = st.session_state.get(f"route_state_{cluster_hash}")
                    is_reverted = st.session_state.get(f"reverted_{cluster_hash}", False)
                    
                    # --- PRIORITY: LIVE DATABASE OVERRIDES LOCAL STATE ---
                    if sheet_match and not is_reverted:
                        raw_status = str(sheet_match.get('status', '')).lower()
                        if raw_status == 'field_nation':
                            if not st.session_state.get(f"route_state_{cluster_hash}"):
                                st.session_state[f"route_state_{cluster_hash}"] = "field_nation"
                            field_nation.append(c)
                        elif raw_status == 'declined':
                            declined.append(c)
                        elif raw_status == 'accepted':
                            accepted.append(c)
                        elif raw_status == 'finalized': 
                            finalized.append(c)
                        else:
                            sent.append(c)
                    # 🌟 Handle Local Session State
                    elif route_state == "email_sent" and not is_reverted:
                        sent.append(c)
                    elif route_state == "field_nation": 
                        field_nation.append(c)
                    elif route_state == "link_generated" and not is_reverted:
                        orig = st.session_state.get(f"orig_status_{cluster_hash}")
                        if orig == "declined":
                            declined.append(c)
                        else:
                            ready.append(c)
                    else:
                        if c.get('status') == 'Ready': 
                            ready.append(c)
                        else: 
                            review.append(c)
                
                pod_ghosts = ghost_db.get(pod, [])
                total_accepted = len(accepted) + len(pod_ghosts)
                true_sent_count = len(sent) + len(field_nation) + total_accepted + len(declined)
                visual_total_routes = len(pod_cls) + len(pod_ghosts)
                
                card_content = f"""
<p style='margin: 10px 0 0 0; font-size: 26px; font-weight: 800; color: {colors['text']};'>{true_sent_count} / {visual_total_routes}</p>
<p style='margin: -5px 0 0 0; font-size: 11px; font-weight: 700; color: {colors['text']}; opacity: 0.6; text-transform: uppercase;'>Routes Sent</p>
<p style='margin: 2px 0 8px 0; font-size: 9px; font-weight: 700; color: {colors['text']}; opacity: 0.5;'>{total_accepted} ACCEPTED | {len(declined)} DECLINED</p>
<div style='display: flex; justify-content: space-around; border-top: 1px solid rgba(0,0,0,0.08); padding-top: 10px;'>
<div><p style='margin:0; font-size:9px; color: {colors['text']}; opacity: 0.8; font-weight: 800;'>TASKS</p><b style='color: {colors['text']};'>{total_tasks}</b></div>
<div style='border-left: 1px solid rgba(0,0,0,0.08); height: 20px;'></div>
<div><p style='margin:0; font-size:9px; color: {colors['text']}; opacity: 0.8; font-weight: 800;'>STOPS</p><b style='color: {colors['text']};'>{total_stops}</b></div>
</div>
"""
                for c in pod_cls: folium.CircleMarker(c['center'], radius=5, color=colors['border'], fill=True, fill_opacity=0.7).add_to(global_map)
            else:
                card_content = f"<p style='color: {colors['text']}; opacity: 0.3; font-weight: 800; margin-top: 30px;'>OFFLINE</p>"

            st.markdown(f"""
<div class="pod-card-pill" style="border: 2px solid {colors['border']}; border-radius: 30px; padding: 20px 10px; background-color: {colors['bg']}; text-align: center; height: 190px; box-shadow: 0 4px 10px rgba(0,0,0,0.03); display: flex; flex-direction: column; justify-content: center;">
<div style="margin: 0; color: {colors['text']}; font-weight: 800; font-size: 1.2rem;">{pod} Pod</div>
{card_content}
</div>
""", unsafe_allow_html=True)
            
    if st.session_state.get("trigger_pull"):
        import time as _time
        _g_start = _time.time()

        def _render_global_card(overlay, msg, start):
            elapsed = int(_time.time() - start)
            m = elapsed // 60; s = elapsed % 60
            overlay.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #633094;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#633094;background:#f3e8ff;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing All Pods</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>{msg}</p>
                    <div class='dcc-pill'>⏱ {m}:{s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

        _g_overlay = loading_placeholder.empty()
        st.session_state['_loading_overlay'] = _g_overlay
        st.session_state['_loading_start'] = _g_start
        st.session_state['_loading_pod'] = 'Global'

        _render_global_card(_g_overlay, "Loading route database...", _g_start)
        _time.sleep(0.05)
        p_bar = bar_placeholder.progress(0, text="📋 Loading route database from Google Sheets...")
        st.session_state.sent_db, st.session_state.ghost_db, st.session_state['archived_wos'], st.session_state['_history_db'] = fetch_sent_records_from_sheet()
        _render_global_card(_g_overlay, f"Fetching tasks across {len(pod_keys)} pods...", _g_start)
        p_bar.progress(0.03, text=f"⏳ Fetching tasks across {len(pod_keys)} pods...")
        for idx, p in enumerate(pod_keys):
            st.session_state.current_loading_pod = p
            process_pod(p, master_bar=p_bar, pod_idx=idx, total_pods=len(pod_keys))
        st.session_state.current_loading_pod = None
        _g_overlay.empty()
        bar_placeholder.empty()
        st.session_state.pop('_loading_overlay', None)
        st.session_state.pop('_loading_start', None)
        st.session_state.pop('_loading_pod', None)
        st.session_state.trigger_pull = False
        st.rerun()

    # 🌟 THE FIX: Inject the blue prompt right above the map if no data exists


    st.markdown("<br> 🗺️ Master Route Map", unsafe_allow_html=True)
    st_folium(global_map, height=500, use_container_width=True, key="global_master_map", returned_objects=[])

# --- INDIVIDUAL POD TABS ---
# 🌟 FIX: Using 2 instead of 1 to account for the new Digital Pool tab!
for i, pod in enumerate(["Blue", "Green", "Orange", "Purple", "Red"], 1):
    with tabs[i]: run_pod_tab(pod)

# --- TAB 6: DIGITAL POOL ---
with tabs[6]:
    # 1. 📊 GRAB DATA & INITIALIZE
    global_digital = st.session_state.get('global_digital_clusters', [])
    
    # 🌟 THE FIX: Omni-Ghost Sorter for Digital
    sent_db, ghost_db, _archived_wos, _history_db = fetch_sent_records_from_sheet()
    st.session_state['_history_db'] = _history_db
    st.session_state['archived_wos'] = _archived_wos
    digital_ghosts_list = ghost_db.get("Global_Digital", [])
    
    pod_ghosts, finalized_ghosts, sent_ghosts = [], [], []
    seen_ghosts = set() # 🛡️ THE FIX: Streamlit Crash Shield
    
    for g in digital_ghosts_list:
        g_hash = g.get('hash')
        
        # If the Google Sheet has duplicate rows, drop the clone instantly!
        if g_hash in seen_ghosts:
            continue
        seen_ghosts.add(g_hash)
        
        g_stat = g.get("status", "")
        local_override = st.session_state.get(f"route_state_{g_hash}")
        if local_override == "finalized" or g_stat == "finalized": finalized_ghosts.append(g)
        elif g_stat == "sent": sent_ghosts.append(g)
        else: pod_ghosts.append(g)
    
    # --- 🚦 TRAFFIC COP: BUCKET SORTING (Pulls WO from Sheet) ---
    d_ready, d_flagged, d_fn, d_sent, d_acc, d_dec, d_fin = [], [], [], [], [], [], []
    live_hashes = set() # 🌟 Track live routes so we don't duplicate them!
    
    for c in global_digital:
        task_ids = [str(t['id']).strip() for t in c['data']]
        cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
        live_hashes.add(cluster_hash) # Save hash
        
        route_state = st.session_state.get(f"route_state_{cluster_hash}")
        is_reverted = st.session_state.get(f"reverted_{cluster_hash}", False)
        
        # 🌟 Fetch Local Memory
        local_ts = st.session_state.get(f"sent_ts_{cluster_hash}", "")
        local_contractor = st.session_state.get(f"contractor_{cluster_hash}", "Unknown")
        local_wo = st.session_state.get(f"wo_{cluster_hash}", local_contractor)
        
        # Match live sheet data to get the Contractor Name and WO
        sheet_match = sent_db.get(next((tid for tid in task_ids if tid in sent_db), None))
        if sheet_match and not is_reverted:
            c['contractor_name'] = sheet_match.get('name', 'Unknown')
            c['wo'] = sheet_match.get('wo', c['contractor_name'])
            c['route_ts'] = sheet_match.get('time', '') or local_ts
            c['comp'] = sheet_match.get('comp', 0)    # 🌟 NEW
            c['due'] = sheet_match.get('due', 'N/A')  # 🌟 NEW
            db_stat = sheet_match.get('status', 'sent').lower()
        else:
            # 🌟 Apply Fallbacks Instantly
            c['contractor_name'] = local_contractor
            c['wo'] = local_wo
            c['route_ts'] = local_ts
            db_stat = c.get('db_status', 'ready').lower()

        # 🌟 LOGIC GATE: Every .append() target MUST start with 'd_'
        if route_state == 'finalized': d_fin.append(c) # 🌟 THE FIX: Local Finalize Override
        elif db_stat in ['sent', 'email_sent'] and not is_reverted: d_sent.append(c) 
        elif db_stat == 'accepted' and not is_reverted: d_acc.append(c) 
        elif db_stat == 'declined' and not is_reverted: d_dec.append(c) 
        elif db_stat == 'finalized' and not is_reverted: d_fin.append(c)
        elif db_stat == 'field_nation' and not is_reverted: d_fn.append(c) 
        elif route_state == 'email_sent' and not is_reverted: d_sent.append(c) 
        elif route_state == 'field_nation' and not is_reverted: d_fn.append(c) 
        # 👇 Added this safeguard back in just in case!
        elif route_state == 'link_generated' and not is_reverted:
            orig = st.session_state.get(f"orig_status_{cluster_hash}")
            if orig == "declined": d_dec.append(c)
            else: d_ready.append(c)
        else:
            if c.get('status') == 'Ready': d_ready.append(c) 
            else: d_flagged.append(c)
                
    # Supercard Counts
    pool_ready = len(d_ready)
    pool_flagged = len(d_flagged)
    pool_total_sent = len(d_sent) + len(d_acc) + len(pod_ghosts) + len(d_dec) + len(d_fn)
    
    # 🌟 THE FIX: Combine active Digital buckets (Excludes Accepted & Finalized)
    active_d_cls = d_ready + d_flagged + d_fn + d_sent + d_dec
    tasks_total = sum(len(c['data']) for c in active_d_cls)
    unique_stops_total = len(set(t['full'] for c in active_d_cls for t in c['data']))
    
    # 2. ⚡ DIGITAL HEADER & DYNAMIC BUTTON
    dh_col1, dh_col2, dh_col3 = st.columns([2, 6, 2])
    with dh_col2:
        st.markdown(f"<div style='text-align:center; padding-bottom:15px;'><h2 style='color:{TB_DIGITAL_TEXT}; margin:0;'>🔌 Digital Services Dashboard</h2></div>", unsafe_allow_html=True)
    with dh_col3:
        st.markdown("<div class='tab-action-btn'>", unsafe_allow_html=True)
        btn_label = "🚀 Sync Routes" if global_digital else "🚀 Initialize Data"
        digital_init_clicked = st.button(btn_label, key="digital_init_btn", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # 🌟 FULL-WIDTH LOADING UI — outside columns
    if digital_init_clicked:
        import time as _time
        _d_start = _time.time()
        _d_overlay = st.empty()
        _d_bar = st.progress(0, text="🔌 Connecting to Onfleet...")

        def _render_digital_card(overlay, start):
            elapsed = int(_time.time() - start)
            m = elapsed // 60; s = elapsed % 60
            overlay.markdown(f"""
                <style>
                    @keyframes spin {{0%{{transform:rotate(0deg)}}100%{{transform:rotate(360deg)}}}}
                    .dcc-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;
                        padding:36px 32px;text-align:center;margin:20px 0;}}
                    .dcc-spin{{width:44px;height:44px;border:4px solid #e2e8f0;
                        border-top:4px solid #0f766e;border-radius:50%;
                        animation:spin 0.8s linear infinite;margin:0 auto 16px auto;}}
                    .dcc-pill{{display:inline-block;font-size:13px;font-weight:700;
                        color:#0f766e;background:#ccfbf1;border-radius:20px;
                        padding:4px 14px;margin-top:12px;}}
                </style>
                <div class='dcc-card'>
                    <div class='dcc-spin'></div>
                    <p style='font-size:16px;font-weight:800;color:#0f172a;margin:0 0 4px 0;'>Initializing Digital Pool</p>
                    <p style='font-size:13px;color:#64748b;margin:0 0 8px 0;'>Fetching Digital tasks from Onfleet...</p>
                    <div class='dcc-pill'>⏱ {m}:{s:02d}</div>
                </div>
            """, unsafe_allow_html=True)

        st.session_state['_loading_overlay'] = _d_overlay
        st.session_state['_loading_start'] = _d_start
        st.session_state['_loading_pod'] = 'Digital'
        _render_digital_card(_d_overlay, _d_start)
        _time.sleep(0.05)
        _d_bar.progress(0.03, text="⏳ Fetching Digital tasks from Onfleet...")
        process_digital_pool(master_bar=_d_bar)
        _d_overlay.empty()
        _d_bar.empty()
        st.session_state.pop('_loading_overlay', None)
        st.session_state.pop('_loading_start', None)
        st.session_state.pop('_loading_pod', None)
        st.rerun()

    # 3. 🃏 SUPERCARDS
    dc1, dc2, dc3 = st.columns([1, 1, 1])
    with dc1:
        st.markdown(f"<div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 110px;'><p style='margin:0 0 8px 0; font-size:10px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Status</p><div style='display:flex; justify-content:space-around; gap:8px;'><div style='background:{TB_GREEN_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_GREEN_TEXT};'>READY</p><p style='margin:0; font-size:22px; font-weight:800;'>{pool_ready}</p></div><div style='background:{TB_RED_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_RED_TEXT};'>FLAGGED</p><p style='margin:0; font-size:22px; font-weight:800;'>{pool_flagged}</p></div></div></div>", unsafe_allow_html=True)
    with dc2:
        # 🌟 UPDATED: Uses tasks_total instead of len(pool)
        st.markdown(f"<div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height: 110px;'><p style='margin:0 0 8px 0; font-size:10px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Workload</p><div style='display:flex; justify-content:space-around; gap:8px;'><div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_STATIC_TEXT};'>TASKS</p><p style='margin:0; font-size:22px; font-weight:800;'>{tasks_total}</p></div><div style='background:{TB_STATIC_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_STATIC_TEXT};'>STOPS</p><p style='margin:0; font-size:22px; font-weight:800;'>{unique_stops_total}</p></div></div></div>", unsafe_allow_html=True)
    with dc3:
        st.markdown(f"<div class='dashboard-supercard' style='background:#ffffff; border:1px solid #cbd5e1; border-radius:12px; padding:12px; height:110px;'><p style='margin:0 0 8px 0; font-size:10px; font-weight:800; color:#64748b; text-transform:uppercase; text-align:center;'>Sent: {pool_total_sent}</p><div style='display:flex; justify-content:space-around; gap:8px;'><div style='background:{TB_GREEN_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_GREEN_TEXT};'>ACCEPTED</p><p style='margin:0; font-size:22px; font-weight:800;'>{len(d_acc)}</p></div><div style='background:{TB_RED_FILL}; flex:1; padding:8px; border-radius:8px; text-align:center;'><p style='margin:0; font-size:8px; font-weight:800; color:{TB_RED_TEXT};'>DECLINED</p><p style='margin:0; font-size:22px; font-weight:800;'>{len(d_dec)}</p></div></div></div>", unsafe_allow_html=True)
    # 🌟 THE FIX: Force spacing after the cards
    st.markdown("<div style='margin-bottom: 25px;'></div>", unsafe_allow_html=True)
    
    # 🌟 THE FIX: Make sure the UI still loads if there are digital ghosts but no live digital routes
    if not global_digital and not pod_ghosts:
        st.info("Click '🚀 Initialize Data' at the top right to fetch data.")
    else:
        # 4. 🗺️ MAP & LEGEND
        # 🌟 THE FIX: Safe coordinate extraction
        map_center_digi = global_digital[0]['center'] if global_digital else [39.8283, -98.5795]
        m_digi = folium.Map(location=map_center_digi, zoom_start=4, tiles="cartodbpositron")
        for c in global_digital: folium.CircleMarker(c['center'], radius=8, color="#0f766e", fill=True, opacity=0.8).add_to(m_digi)
        st_folium(m_digi, height=400, use_container_width=True, key="digital_pool_map", returned_objects=[])
        
        # 5. 🚀 TWO-COLUMN DISPATCH (Parity with Pods)
        st.markdown("""
<div style="display:flex; justify-content:center; flex-wrap:wrap; gap:8px 20px; background:#ffffff; padding:12px 20px; border-radius:12px; border:1px solid #99f6e4; margin-bottom:20px; box-shadow:0 2px 4px rgba(0,0,0,0.05);">
    <div style="font-size:11px; font-weight:900; color:#0f766e; text-transform:uppercase; letter-spacing:0.08em; align-self:center; margin-right:8px;">📖 Route Key</div>
    <span style="font-size:11px; color:#0f766e; font-weight:600; align-self:center; margin-right:4px; border-right:1px solid #99f6e4; padding-right:12px;">Status:</span>
    <span style="font-size:13px;" title="Ready to dispatch">🟢 Ready</span>
    <span style="font-size:13px;" title="Requires unlock">🔒 Action Req.</span>
    <span style="font-size:13px;" title="Flagged for review">🔴 Flagged</span>
    <span style="font-size:13px;" title="Field Nation">🌐 FN</span>
    <span style="font-size:11px; color:#0f766e; font-weight:600; align-self:center; margin-left:4px; margin-right:4px; border-right:1px solid #99f6e4; padding-right:12px;">Flags:</span>
    <span style="font-size:13px;" title="IC 40+ miles away">📡 Distance</span>
    <span style="font-size:13px;" title="Contains escalated tasks requiring priority handling">❗ Escalation</span>
    <span style="font-size:11px; color:#0f766e; font-weight:600; align-self:center; margin-left:4px; margin-right:4px; border-right:1px solid #99f6e4; padding-right:12px;">Tasks:</span>
    <span style="font-size:13px;" title="Screen offline">📵 Offline</span>
    <span style="font-size:13px;" title="Install / Removal">🔧 Ins/Rem</span>
    <span style="font-size:13px;" title="Digital maintenance">⚙️ Service</span>
    <span style="font-size:13px;" title="Certified digital IC">🔌 Digital</span>
</div>
""", unsafe_allow_html=True)
        st.markdown("---")
        col_left, col_right = st.columns([5, 5])
        
        with col_left:
            st.markdown(f"<div style='font-size: 1.5rem; font-weight: 800; color: {TB_DIGITAL_TEXT}; text-align: center;'>🚀 Dispatch</div>", unsafe_allow_html=True)
            t_ready, t_flagged, t_fn = st.tabs(["📥 Ready", "⚠️ Flagged", "🌐 Field Nation"])
            
            with t_ready:
                if not d_ready: st.info("No digital tasks ready for dispatch.")
                else:
                    sorted_d_ready = group_and_sort_by_proximity(d_ready)
                    current_state = None
                    for i, c in enumerate(sorted_d_ready):
                        if c['state'] != current_state:
                            current_state = c['state']
                            st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                        _GD_BOOSTED = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                        _gd_boost = f" | {next((v for k,v in _GD_BOOSTED.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _GD_BOOSTED) else ""
                        _gd_esc = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                        with st.expander(f"{get_digi_badges(c['data'])} {c['city']}, {c['state']} | {c['stops']} Stops{_gd_boost}{_gd_esc}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            render_dispatch(i+8000, c, "Global_Digital")
                            
            with t_flagged:
                if not d_flagged: st.info("No flagged tasks requiring review.")
                else:
                    sorted_d_flagged = group_and_sort_by_proximity(d_flagged)
                    current_state = None
                    for i, c in enumerate(sorted_d_flagged):
                        if c['state'] != current_state:
                            current_state = c['state']
                            st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                        _GDF_BOOSTED = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                        _gdf_boost = f" | {next((v for k,v in _GDF_BOOSTED.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _GDF_BOOSTED) else ""
                        _gdf_esc = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                        with st.expander(f"🔴 {get_digi_badges(c['data'])} {c['city']}, {c['state']} | {c['stops']} Stops{_gdf_boost}{_gdf_esc}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            render_dispatch(i+9000, c, "Global_Digital")
                            
            with t_fn:
                if not d_fn: st.info("No tasks in Field Nation.")
                else:
                    sorted_d_fn = group_and_sort_by_proximity(d_fn)
                    current_state = None
                    for i, c in enumerate(sorted_d_fn):
                        if c['state'] != current_state:
                            current_state = c['state']
                            st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📍 {current_state}</div>", unsafe_allow_html=True)
                        _GDFN_BOOSTED = {'local plus': '⭐ LOCAL PLUS', 'boosted': '🔥 BOOSTED'}
                        _gdfn_boost = f" | {next((v for k,v in _GDFN_BOOSTED.items() if k in c.get('boosted_tag','')), '')}" if c.get('boosted_tag') and any(k in c.get('boosted_tag','') for k in _GDFN_BOOSTED) else ""
                        _gdfn_esc = f" | ❗ {c.get('esc_count', 0)}" if c.get('esc_count', 0) > 0 else ""
                        with st.expander(f"🌐 FN {get_digi_badges(c['data'])} {c['city']}, {c['state']} | {c['stops']} Stops{_gdfn_boost}{_gdfn_esc}  ·  :gray[{len(c['data'])} tasks]{_bundle_pill(c)}"):
                            render_dispatch(i+9500, c, "Global_Digital")

        with col_right:
            st.markdown(f"<div style='font-size: 1.5rem; font-weight: 800; color: {TB_GREEN}; text-align: center;'>⏳ Awaiting Confirmation</div>", unsafe_allow_html=True)
            t_sent, t_acc, t_dec, t_fin = st.tabs(["✉️ Sent", "✅ Accepted", "❌ Declined", "🏁 Finalized"])
            
            with t_sent:
                unified_sent = unify_and_sort_by_date(d_sent, sent_ghosts, live_hashes)
                if not unified_sent: st.info("No pending routes sent.")
                
                current_date = None
                for i, item in enumerate(unified_sent):
                    date_str = item['sort_date']
                    if date_str != current_date:
                        current_date = date_str
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 SENT: {current_date}</div>", unsafe_allow_html=True)
                    
                    if not item['is_ghost']:
                        c = item
                        task_ids = [str(t['id']).strip() for t in c['data']]
                        cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                        ic_name = c.get('contractor_name', 'Unknown')
                        comp, due = c.get('comp', 0), c.get('due', 'N/A')
                        tasks_cnt, stops_cnt = len(c['data']), c['stops']
                        wo_display = c.get('wo', ic_name)
                        
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            with st.expander(f"✉️ {wo_display} | ${comp} | Due: {due}  ·  :gray[{tasks_cnt} tasks]{_bundle_pill(c)}"):
                                u_locs, _dslv = [], []
                                for tk in c['data']:
                                    if tk['full'] not in u_locs:
                                        u_locs.append(tk['full'])
                                        _v = tk.get('venue_name', '')
                                        _dslv.append(f"{_v} — {tk['full']}" if _v else tk['full'])
                                _ds_venues = venue_section(make_venue_details(c['data']))
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_ds_venues}</div>""", unsafe_allow_html=True)
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:13px; text-align:center;'>Re-route from <b>{ic_name}</b>?</p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Re-Route", key=f"rev_d_sent_live_{cluster_hash}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": "Global_Digital", "action_label": "Re-Routed", "check_onfleet": True, "cluster_data": c})
                    else:
                        g = item
                        g_ic_name = g.get('contractor_name', 'Unknown')
                        ghost_hash = g.get('hash', f"ghost_d_sent_{i}")
                        wo_display = g.get('wo', g_ic_name)
                        comp, due = g.get('pay', 0), g.get('due', 'N/A')
                        stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                        
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            with st.expander(f"✉️ {wo_display} | ${comp} | Due: {due}  ·  :gray[{tasks_cnt} tasks]"):
                                raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                                if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                                else: task_locs = raw_locs
                                u_locs = list(dict.fromkeys(task_locs))
                                _dsg_venues = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_dsg_venues}</div>""", unsafe_allow_html=True)
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:13px; text-align:center;'>Re-route from <b>{g_ic_name}</b>?</p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Re-Route", key=f"rev_ghost_d_sent_{ghost_hash}_{i}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": "Global_Digital", "action_label": "Re-Routed", "check_onfleet": True, "cluster_data": g})
            
            with t_acc:
                unified_acc = unify_and_sort_by_date(d_acc, pod_ghosts, live_hashes)
                if not unified_acc: st.info("Waiting for portal acceptances...")
                
                current_date = None
                for i, item in enumerate(unified_acc):
                    date_str = item['sort_date']
                    if date_str != current_date:
                        current_date = date_str
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 ACCEPTED: {current_date}</div>", unsafe_allow_html=True)
                    
                    if not item['is_ghost']:
                        c = item
                        task_ids = [str(t['id']).strip() for t in c['data']]
                        cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                        ic_name = c.get('contractor_name', 'Unknown')
                        comp, due = c.get('comp', 0), c.get('due', 'N/A')
                        tasks_cnt, stops_cnt = len(c['data']), c['stops']
                        
                        _dins_cnt = sum(1 for tk in c['data'] if 'ins' in str(tk.get('task_type','')).lower() or 'rem' in str(tk.get('task_type','')).lower())
                        _dins_pill = f" | 🔧 {_dins_cnt} Ins/Rem" if _dins_cnt > 0 else ""
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            with st.expander(f"✅ {c.get('wo', ic_name)} | ${comp} | Due: {due}" + (f" | 🛠️ {sum(1 for tk in c['data'] if 'install' in str(tk.get('task_type','')).lower())}" if any('install' in str(tk.get('task_type','')).lower() for tk in c['data']) else "") + _bundle_pill(c)):
                                u_locs, _dalv = [], []
                                for tk in c['data']:
                                    if tk['full'] not in u_locs:
                                        u_locs.append(tk['full'])
                                        _v = tk.get('venue_name','')
                                        _dalv.append(f"{_v} — {tk['full']}" if _v else tk['full'])
                                _dal_venues = venue_section(make_venue_details(c['data']))
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_dal_venues}</div>""", unsafe_allow_html=True)
                                render_finalization_checklist(cluster_hash, "Global_Digital", "d_chk")
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{c.get('wo', ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Remove", key=f"rev_d_acc_{cluster_hash}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": "Global_Digital", "cluster_data": c})
                    else:
                        g = item
                        g_ic_name = g.get('contractor_name', 'Unknown')
                        ghost_hash = g.get('hash', f"ghost_digi_{i}")
                        comp, due = g.get('pay', 0), g.get('due', 'N/A')
                        stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                        
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            _gins_cnt = g.get('digi_ins', 0) or 0
                        _gins_pill = f" | 🔧 {_gins_cnt} Ins/Rem" if _gins_cnt > 0 else ""
                        with st.expander(f"✅ {g.get('wo', g_ic_name)} | ${comp} | Due: {due}"):
                                raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                                if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                                else: task_locs = raw_locs
                                u_locs = list(dict.fromkeys(task_locs))
                                _dag_venues = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_dag_venues}</div>""", unsafe_allow_html=True)
                                render_finalization_checklist(ghost_hash, "Global_Digital", "g_chk_d")
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{g_ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{g.get('wo', g_ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Remove", key=f"rev_ghost_digi_{ghost_hash}_{i}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": "Global_Digital", "action_label": "Ghost Archived", "check_onfleet": True, "cluster_data": g})

            with t_dec:
                unified_dec = unify_and_sort_by_date(d_dec, [], live_hashes)
                if not unified_dec: st.info("No declined routes.")
                
                current_date = None
                for i, item in enumerate(unified_dec):
                    date_str = item['sort_date']
                    if date_str != current_date:
                        current_date = date_str
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 DECLINED: {current_date}</div>", unsafe_allow_html=True)
                    
                    c = item
                    task_ids = [str(t['id']).strip() for t in c['data']]
                    cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                    ic_name = c.get('contractor_name', 'Unknown')
                    exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                    with exp_col:
                        comp_ddec = c.get('comp', 0); due_ddec = c.get('due', 'N/A')
                        stops_ddec, tasks_ddec = c['stops'], len(c['data'])
                        with st.expander(f"❌ {c.get('wo', ic_name)} | ${comp_ddec} | Due: {due_ddec}{_bundle_pill(c)}"):
                            _ddec_venues = venue_section(make_venue_details(c['data']))
                            st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_ddec} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_ddec} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due_ddec}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp_ddec}</div></div></div>{_ddec_venues}</div>""", unsafe_allow_html=True)
                    with btn_col:
                        with st.popover("↩️"):
                            st.markdown(f"<p style='font-size:13px; text-align:center;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</p>", unsafe_allow_html=True)
                            st.button("🚨 Yes, Remove", key=f"rev_d_dec_{cluster_hash}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": "Global_Digital", "cluster_data": c})
                    
            with t_fin:
                unified_fin = unify_and_sort_by_date(d_fin, finalized_ghosts, live_hashes)
                if not unified_fin: st.info("No finalized digital routes.") 
                
                current_date = None
                for i, item in enumerate(unified_fin):
                    date_str = item['sort_date']
                    if date_str != current_date:
                        current_date = date_str
                        st.markdown(f"<div style='font-size: 12px; font-weight: 800; color: #94a3b8; margin-top: 15px; margin-bottom: 5px; border-bottom: 1px solid #e2e8f0; padding-bottom: 2px; text-transform: uppercase; letter-spacing: 1px;'>📅 FINALIZED: {current_date}</div>", unsafe_allow_html=True)
                    
                    if not item['is_ghost']:
                        c = item
                        task_ids = [str(t['id']).strip() for t in c['data']]
                        cluster_hash = hashlib.md5("".join(sorted(task_ids)).encode()).hexdigest()
                        ic_name = c.get('contractor_name', 'Unknown')
                        comp, due = c.get('comp', 0), c.get('due', 'N/A')
                        tasks_cnt, stops_cnt = len(c['data']), c['stops']
                        
                        _dfins_cnt = sum(1 for tk in c['data'] if 'ins' in str(tk.get('task_type','')).lower() or 'rem' in str(tk.get('task_type','')).lower())
                        _dfins_pill = f" | 🔧 {_dfins_cnt} Ins/Rem" if _dfins_cnt > 0 else ""
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            with st.expander(f"🏁 {c.get('wo', ic_name)} | ${comp} | Due: {due}" + (f" | 🛠️ {sum(1 for tk in c['data'] if 'install' in str(tk.get('task_type','')).lower())}" if any('install' in str(tk.get('task_type','')).lower() for tk in c['data']) else "") + _bundle_pill(c)):
                                u_locs, _dflv = [], []
                                for tk in c['data']:
                                    if tk['full'] not in u_locs:
                                        u_locs.append(tk['full'])
                                        _v = tk.get('venue_name','')
                                        _dflv.append(f"{_v} — {tk['full']}" if _v else tk['full'])
                                _dfl_venues = venue_section(make_venue_details(c['data']))
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_dfl_venues}</div>""", unsafe_allow_html=True)
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{c.get('wo', ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Remove", key=f"rev_d_fin_{cluster_hash}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": cluster_hash, "ic_name": ic_name, "pod_name": "Global_Digital", "cluster_data": c, "check_completed": True})
                    else:
                        g = item
                        g_ic_name = g.get('contractor_name', 'Unknown')
                        ghost_hash = g.get('hash', f"ghost_fin_digi_{i}")
                        wo_display = g.get('wo', g_ic_name)
                        comp, due = g.get('pay', 0), g.get('due', 'N/A')
                        stops_cnt, tasks_cnt = g.get('stops', 0), g.get('tasks', 0)
                        
                        exp_col, btn_col = st.columns([9.5, 0.5], vertical_alignment="center")
                        with exp_col:
                            _gdfins_cnt = g.get('digi_ins', 0) or 0
                        _gdfins_pill = f" | 🔧 {_gdfins_cnt} Ins/Rem" if _gdfins_cnt > 0 else ""
                        with st.expander(f"🏁 {wo_display} | ${comp} | Due: {due}"):
                                raw_locs = [s.strip() for s in g.get('locs', '').split('|') if s.strip()]
                                if len(raw_locs) >= 3: task_locs = raw_locs[1:-1]
                                else: task_locs = raw_locs
                                u_locs = list(dict.fromkeys(task_locs))
                                _dgf_venues = venue_section(make_venue_details_ghost(u_locs, stop_data=g.get('stop_data', []))) if u_locs else ""
                                st.markdown(f"""<div style="background:#ffffff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; margin-bottom:10px;"><div style="background:#f8fafc; border-bottom:1px solid #e2e8f0; padding:8px 12px;"><span style="font-size:9px; font-weight:900; color:#94a3b8; text-transform:uppercase; letter-spacing:0.1em;">Route Summary</span></div><div style="padding:12px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Contractor</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{g_ic_name}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Stops / Tasks</div><div style="font-size:14px; font-weight:800; color:#0f172a;">{stops_cnt} <span style="color:#94a3b8; font-size:11px; font-weight:500;">Stops / {tasks_cnt} Tasks</span></div></div></div><div style="padding:10px 14px; display:flex; justify-content:space-between; align-items:flex-start; border-bottom:1px solid #f1f5f9;"><div><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Due Date</div><div style="font-size:13px; font-weight:700; color:#0f172a;">{due}</div></div><div style="text-align:right;"><div style="font-size:9px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:2px;">Total Compensation</div><div style="font-size:18px; font-weight:900; color:#16a34a;">${comp}</div></div></div>{_dgf_venues}</div>""", unsafe_allow_html=True)
                        with btn_col:
                            with st.popover("↩️"):
                                st.markdown(f"<p style='font-size:11px; text-align:center; margin:0 0 4px 0; line-height:1.3;'><span style='color:#475569; font-weight:700;'>Are you sure you want to remove this route from <b>{g_ic_name}</b>?</span><br><span style='color:#dc2626; font-size:10px; font-weight:500;'>All remaining tasks in <b>{g.get('wo', g_ic_name)}</b> will be removed from OnFleet.</span></p>", unsafe_allow_html=True)
                                st.button("🚨 Yes, Remove", key=f"rev_ghost_d_fin_{ghost_hash}_{i}", type="primary", use_container_width=True, on_click=move_to_dispatch, kwargs={"cluster_hash": ghost_hash, "ic_name": g_ic_name, "pod_name": "Global_Digital", "action_label": "Ghost Archived", "check_onfleet": True, "cluster_data": g, "check_completed": True})
                        
# --- FINAL FOOTER (End of File) ---
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #94a3b8; font-size: 12px; padding: 20px;">
        Tactical Workspace Master • 2026 Digital Logistics Interface • <b>v2.4.0</b><br>
        <i>All digital and static route data is synced in real-time.</i>
    </div>
    """, 
    unsafe_allow_html=True
    )
