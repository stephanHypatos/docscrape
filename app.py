import io
import re
import time
from typing import Dict, Optional, Tuple, List, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- Constants ----------
BASE_URL = "https://www.medi-learn.de/pruefungsprotokolle/facharztpruefung/detailed.php?ID={page_id}"

# Map site labels -> normalized keys in our dataset
TARGET_LABELS = {
    "Fach": "fach",
    "Ort/Uni": "ort_uni",
    "Prüfer": "pruefer",
    "Atmosphäre": "atmosphaere",
    "Dauer": "dauer",
    "Note": "note",
    "Vorgespräch": "vorgespraech",
    "Kleidung": "kleidung",
    "Gespräch": "gespraech",
}

# ---------- Streamlit config ----------
st.set_page_config(page_title="Medi-Learn Protokolle Scraper", layout="wide")
st.title("Medi-Learn Facharztprüfungen — Scraper")

# ---------- Helpers ----------
def normalize_label(raw_label: str) -> str:
    """Normalize label text: trim, drop trailing colon, collapse spaces."""
    if raw_label is None:
        return ""
    text = raw_label.strip()
    text = re.sub(r"[:：]\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text

def extract_fields_from_table(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """
    Parse table.diensttabelle.
    - Rows with 2+ cells: label in first cell, value in second.
    - A following <td colspan="2"> row is a continuation of the last label's value
      (used by long 'Gespräch' / 'Vorgespräch' text blocks).
    """
    table = soup.select_one("table.diensttabelle")
    if not table:
        return None

    data = {v: "" for v in TARGET_LABELS.values()}
    current_key = None

    def clean_text(el) -> str:
        return el.get_text(separator="\n", strip=True)

    for row in table.select("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        # Case 1: label/value on the same row
        if len(cells) >= 2:
            label = normalize_label(cells[0].get_text(separator=" ", strip=True))
            if label in TARGET_LABELS:
                key = TARGET_LABELS[label]
                value = clean_text(cells[1])
                data[key] = (data[key] + ("\n" if data[key] and value else "") + value).strip()
                current_key = key
                continue

        # Case 2: continuation row spanning both columns
        if len(cells) == 1 and cells[0].name == "td" and cells[0].has_attr("colspan"):
            cont_text = clean_text(cells[0])
            if current_key and cont_text:
                data[current_key] = (data[current_key] + ("\n" if data[current_key] else "") + cont_text).strip()
            continue

    # Accept only if something was captured
    if any(v.strip() for v in data.values()):
        return data
    return None

def fetch_page(page_id: int, session: Optional[requests.Session] = None) -> Tuple[bool, Optional[Dict[str, str]], int]:
    """Fetch and parse a page. Returns (found, record, http_status)."""
    url = BASE_URL.format(page_id=page_id)
    s = session or requests.Session()
    try:
        resp = s.get(url, timeout=15)
    except requests.RequestException:
        return False, None, 0

    if resp.status_code != 200:
        return False, None, resp.status_code

    soup = BeautifulSoup(resp.text, "html.parser")
    parsed = extract_fields_from_table(soup)
    if parsed is None:
        return False, None, 200

    parsed["page_id"] = page_id
    parsed["url"] = url
    return True, parsed, 200

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="protokolle")
    return buf.getvalue()

def _norm(s: Optional[str]) -> str:
    return (s or "").casefold().strip()

def matches_filters(record: Dict[str, str], uni_filter: str, fach_filter: str) -> bool:
    """True iff record matches all provided filters (case-insensitive)."""
    ok_uni = True
    ok_fach = True
    if uni_filter.strip():
        ok_uni = _norm(uni_filter) in _norm(record.get("ort_uni", ""))
    if fach_filter.strip():
        ok_fach = _norm(fach_filter) in _norm(record.get("fach", ""))
    return ok_uni and ok_fach

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Scrape Settings")
    start_id = st.number_input("Start pageId", min_value=0, value=0, step=1)
    max_pages = st.number_input("Max pages to try", min_value=1, value=500, step=1)
    miss_streak_limit = st.number_input(
        "Stop after N consecutive misses", min_value=1, value=15, step=1,
        help="Stop early if this many IDs in a row have no usable data."
    )
    delay_ms = st.number_input("Delay between requests (ms)", min_value=0, value=300, step=50)

    st.divider()
    st.header("Filters (optional)")
    uni_filter = st.text_input("Uni/Ort contains …", placeholder="e.g., Dresden")
    fach_filter = st.text_input("Fach contains …", placeholder="e.g., Innere Medizin")
    st.caption("Only records matching all provided filters will be saved & exported.")

# ---------- Session state ----------
if "records" not in st.session_state:
    st.session_state.records: List[Dict[str, str]] = []
if "seen_ids" not in st.session_state:
    st.session_state.seen_ids: Set[int] = set()

# ---------- Controls ----------
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    run = st.button("Start Scraping", type="primary")
with col2:
    clear = st.button("Clear Results")

if clear:
    st.session_state.records = []
    st.session_state.seen_ids = set()
    st.success("Cleared in-session results.")

# ---------- Scrape loop ----------
if run:
    progress = st.progress(0, text="Starting…")
    status = st.empty()
    table_placeholder = st.empty()

    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; StreamlitScraper/1.0)",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })

    found_count = 0
    miss_streak = 0

    for i, page_id in enumerate(range(start_id, start_id + int(max_pages))):
        pct = int(((i + 1) / max_pages) * 100)
        progress.progress(pct, text=f"Scraping pageId={page_id} ({pct}%)")

        found, record, code = fetch_page(page_id, session=s)
        if found and record:
            # Page found ⇒ reset miss streak
            miss_streak = 0

            # Filter & dedupe by page_id
            if matches_filters(record, uni_filter, fach_filter):
                if page_id not in st.session_state.seen_ids:
                    st.session_state.seen_ids.add(page_id)
                    st.session_state.records.append(record)
                    found_count += 1
                    status.info(f"✅ Saved pageId={page_id} (HTTP {code})")
                else:
                    status.info(f"↺ Skipped duplicate pageId={page_id}")
            else:
                status.info(f"➖ Skipped pageId={page_id} (doesn't match filters)")
        else:
            miss_streak += 1
            status.warning(f"❌ Missing/empty pageId={page_id} (HTTP {code}) | Miss streak: {miss_streak}")

        # Live table
        if st.session_state.records:
            df_live = pd.DataFrame(st.session_state.records)
            ordered_cols = [
                "page_id", "url",
                "fach", "ort_uni", "pruefer", "atmosphaere",
                "dauer", "note", "vorgespraech", "kleidung", "gespraech",
            ]
            for c in ordered_cols:
                if c not in df_live.columns:
                    df_live[c] = ""
            df_live = df_live[ordered_cols].sort_values("page_id")
            table_placeholder.dataframe(df_live, use_container_width=True, hide_index=True)
        else:
            table_placeholder.info("No results yet…")

        # Stop early on consecutive misses
        if miss_streak >= int(miss_streak_limit):
            status.error(f"Stopped early after {miss_streak} consecutive missing pages.")
            break

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    progress.empty()
    st.success(f"Done. Saved {found_count} matching pages.")

# ---------- Results & Export ----------
active_filters = []
if uni_filter.strip():
    active_filters.append(f"Uni/Ort contains “{uni_filter}”")
if fach_filter.strip():
    active_filters.append(f"Fach contains “{fach_filter}”")
if active_filters:
    st.info("Active filters → " + " | ".join(active_filters))

st.subheader("Results")
if st.session_state.records:
    df = pd.DataFrame(st.session_state.records)
    ordered_cols = [
        "page_id", "url",
        "fach", "ort_uni", "pruefer", "atmosphaere",
        "dauer", "note", "vorgespraech", "kleidung", "gespraech",
    ]
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[ordered_cols].sort_values("page_id")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Download as Excel
    excel_bytes = to_excel_bytes(df)
    st.download_button(
        label="⬇️ Download as Excel (.xlsx)",
        data=excel_bytes,
        file_name="medi_learn_protokolle.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Optional: also write to file on disk (local runs)
    if st.checkbox("Also write XLSX to app folder"):
        out_path = "medi_learn_protokolle.xlsx"
        with open(out_path, "wb") as f:
            f.write(excel_bytes)
        st.success(f"Wrote file: {out_path}")
else:
    st.info("No data yet. Click **Start Scraping** to begin.")
