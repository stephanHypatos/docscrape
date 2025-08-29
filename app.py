import time
import io
import re
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

BASE_URL = "https://www.medi-learn.de/pruefungsprotokolle/facharztpruefung/detailed.php?ID={page_id}"

# Labels we want to extract (German originals as seen on page)
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

# --- Utilities ----------------------------------------------------------------

def normalize_label(raw_label: str) -> str:
    """Strip colon and spaces, return original label text as used on the site."""
    if raw_label is None:
        return ""
    text = raw_label.strip()
    # Remove trailing colon variants
    text = re.sub(r"[:：]\s*$", "", text)
    # Collapse internal whitespace
    text = re.sub(r"\s+", " ", text)
    return text

def extract_fields_from_table(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    """
    Parse table.diensttabelle.
    - Rows with two cells (label, value) are captured normally.
    - A following row with a single <td colspan="2"> is treated as a continuation
      of the previous label's value (e.g., the long 'Gespräch' text).
    """
    table = soup.select_one("table.diensttabelle")
    if not table:
        return None

    data = {v: "" for v in TARGET_LABELS.values()}
    current_key = None  # the normalized key in TARGET_LABELS we’re currently appending to

    def clean_text(el) -> str:
        # preserve line breaks between <br> and block nodes
        return el.get_text(separator="\n", strip=True)

    for row in table.select("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        # Case 1: label + value in the same row (>=2 cells)
        if len(cells) >= 2:
            label = normalize_label(cells[0].get_text(separator=" ", strip=True))
            if label in TARGET_LABELS:
                key = TARGET_LABELS[label]
                value = clean_text(cells[1])
                data[key] = (data[key] + ("\n" if data[key] and value else "") + value).strip()
                current_key = key  # remember for possible continuation row
                continue

        # Case 2: a single full-width cell (e.g., <td colspan="2">) — continuation
        if len(cells) == 1 and cells[0].name == "td" and cells[0].has_attr("colspan"):
            cont_text = clean_text(cells[0])
            # append only if we have a previous labeled field; most often 'gespraech' or 'vorgespraech'
            if current_key and cont_text:
                data[current_key] = (data[current_key] + ("\n" if data[current_key] else "") + cont_text).strip()
            continue

        # If the row doesn’t match either pattern, don’t change current_key.

    # accept only if we captured something meaningful
    if any(v.strip() for v in data.values()):
        return data
    return None

def fetch_page(page_id: int, session: Optional[requests.Session] = None) -> Tuple[bool, Optional[Dict[str, str]], int]:
    """
    Returns (found, record, status_code).
    found=False when page is not available or table missing.
    """
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

    # Attach metadata
    parsed["page_id"] = page_id
    parsed["url"] = url
    return True, parsed, 200

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="protokolle")
    return output.getvalue()

def _norm(s: Optional[str]) -> str:
    return (s or "").casefold().strip()

def matches_filters(record: Dict[str, str], uni_filter: str, fach_filter: str) -> bool:
    """Return True if record matches all non-empty filters."""
    ok_uni = True
    ok_fach = True
    if uni_filter.strip():
        ok_uni = _norm(uni_filter) in _norm(record.get("ort_uni", ""))
    if fach_filter.strip():
        ok_fach = _norm(fach_filter) in _norm(record.get("fach", ""))
    return ok_uni and ok_fach


# --- Streamlit UI -------------------------------------------------------------

st.set_page_config(page_title="Medi-Learn Protokolle Scraper", layout="wide")
st.title("Medi-Learn Facharztprüfungs-Protokolle Scraper")

with st.sidebar:
    st.header("Scrape Settings")
    start_id = st.number_input("Start pageId", min_value=0, value=0, step=1)
    max_pages = st.number_input("Max pages to try", min_value=1, value=500, step=1)
    miss_streak_limit = st.number_input("Stop after N consecutive misses", min_value=1, value=15, step=1)
    delay_ms = st.number_input("Delay between requests (ms)", min_value=0, value=300, step=50)

    st.divider()
    st.header("Filters (optional)")
    uni_filter = st.text_input("Uni/Ort contains …", placeholder="e.g., Dresden")
    fach_filter = st.text_input("Fach contains …", placeholder="e.g., Innere Medizin")
    st.caption("Only records matching all provided filters will be saved/exported.")


# session state init
if "records" not in st.session_state:
    st.session_state.records: List[Dict[str, str]] = []
if "last_run_params" not in st.session_state:
    st.session_state.last_run_params = {}

col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    run = st.button("Start Scraping", type="primary")
with col2:
    clear = st.button("Clear Results")

if clear:
    st.session_state.records = []
    st.session_state.last_run_params = {}
    st.success("Cleared in-session results.")

if run:
    st.session_state.last_run_params = {
        "start_id": start_id,
        "max_pages": int(max_pages),
        "miss_streak_limit": int(miss_streak_limit),
        "delay_ms": int(delay_ms),
    }
    progress = st.progress(0, text="Starting…")
    status = st.empty()
    table_placeholder = st.empty()

    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; StreamlitScraper/1.0; +https://example.com)",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })

    found_count = 0
    miss_streak = 0

    for i, page_id in enumerate(range(start_id, start_id + int(max_pages))):
        pct = int(((i + 1) / max_pages) * 100)
        progress.progress(pct, text=f"Scraping pageId={page_id} ({pct}%)")

        found, record, code = fetch_page(page_id, session=s)
        if found and record:
            if matches_filters(record, uni_filter, fach_filter):
                st.session_state.records.append(record)
                found_count += 1
                miss_streak = 0
                status.info(f"✅ Saved pageId={page_id} (HTTP {code})")
            else:
                # Do not save—doesn’t match filters
                miss_streak = 0  # still a valid page, so reset the miss streak
                status.info(f"➖ Skipped pageId={page_id} (doesn't match filters)")
        else:
            miss_streak += 1
            status.warning(f"❌ Missing/empty pageId={page_id} (HTTP {code}) | Miss streak: {miss_streak}")

        # Show live table
        if st.session_state.records:
            df_live = pd.DataFrame(st.session_state.records)
            ordered_cols = [
                "page_id", "url",
                "fach", "ort_uni", "pruefer", "atmosphaere",
                "dauer", "note", "vorgespraech", "kleidung", "gespraech",
            ]
            # ensure columns exist even if missing in early rows
            for c in ordered_cols:
                if c not in df_live.columns:
                    df_live[c] = ""
            df_live = df_live[ordered_cols].sort_values("page_id")
            table_placeholder.dataframe(df_live, use_container_width=True, hide_index=True)
        else:
            table_placeholder.info("No results yet…")

        # stop early on many consecutive misses
        if miss_streak >= int(miss_streak_limit):
            status.error(f"Stopped early after {miss_streak} consecutive missing pages.")
            break

        # be nice to the server
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    progress.empty()
    st.success(f"Done. Found {found_count} pages. "
               f"(Tried up to pageId {start_id + int(max_pages) - 1})")

# Results + Export
active_filters = []
if uni_filter.strip(): active_filters.append(f"Uni/Ort contains: “{uni_filter}”")
if fach_filter.strip(): active_filters.append(f"Fach contains: “{fach_filter}”")
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

    excel_bytes = to_excel_bytes(df)
    st.download_button(
        label="⬇️ Download as Excel (.xlsx)",
        data=excel_bytes,
        file_name="medi_learn_protokolle.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("No data yet. Use **Start Scraping** to begin.")
