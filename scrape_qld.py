#!/usr/bin/env python3
import os, re, json, time, logging, random
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- QLD (QTenders) ----
BASE = "https://qtenders.epw.qld.gov.au"
# We’ll try these list URLs in order until we get links:
LIST_URL_CANDIDATES = [
    # Common “open tenders” listing
    f"{BASE}/qtenders/tender/search/tender-search.do?openTenders=true",
    # Alternate listings seen historically
    f"{BASE}/qtenders/tender/search/tender-search.do?type=OPEN",
    f"{BASE}/qtenders/",
]

# ---- Runtime knobs (via workflow inputs / env) ----
PAGES          = int(os.getenv("PAGES", "2") or "2")            # listing pages to scan
DETAIL_LIMIT   = int(os.getenv("DETAIL_LIMIT", "40") or "40")   # how many detail pages to peek (text only)
TIMEOUT        = int(os.getenv("TIMEOUT", "20") or "20")        # HTTP timeout
ONLY_FILTERED  = os.getenv("ONLY_FILTERED", "1").lower() in ("1","true","yes","y")

# Require a drone signal (like NSW strict)
DRONE_REQUIRED = True

# ---- Drone & Energy filters (same as NSW strict) ----
PRIMARY_DRONE_PATTERNS = [
    r"\bdrone(s)?\b",
    r"\buas\b",
    r"\buav(s)?\b",
    r"\brpas\b",
    r"\bremotely[- ]?piloted\b",
    r"\bLiDAR\b",
    r"\bthermograph(?:y|ic)\b",
    r"\binfrared\b",
    r"\bIR\b",
    r"\baerial\s+(?:survey|inspection|mapping|photography)\b",
]
ENERGY_PATTERNS = [
    r"\bsolar\b", r"\bPV\b", r"\bphotovoltaic\b",
    r"\bwind\b", r"\bturbine(s)?\b",
    r"\brenewable(s)?\b", r"\benergy\b",
    r"\bsubstation\b", r"\btransmission\b", r"\bdistribution\b",
    r"\bbattery(?:\s+storage)?\b", r"\bBESS\b",
    r"\bO&M\b", r"\boperations?\s+and\s+maintenance\b", r"\bfarm\b"
]
NEGATIVE_PATTERNS = [
    r"\bconnecting with country\b",
    r"\bdesign review panel\b",
    r"\bacoustic advisory\b",
    r"\bstrategy\b",
    r"\bheritage\b",
    r"\bcommunity (?:consultation|engagement)\b",
    r"\bbrand(?:ing)?\b",
    r"\bICT strategy\b",
    r"\bpolicy\b"
]

DRONE_RE    = re.compile("|".join(PRIMARY_DRONE_PATTERNS), re.I)
ENERGY_RE   = re.compile("|".join(ENERGY_PATTERNS), re.I)
NEGATIVE_RE = re.compile("|".join(NEGATIVE_PATTERNS), re.I)

# ---- HTTP helpers ----
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0 Safari/537.36 GridTender/QLD-0.1")
    })
    return s

def fetch(url: str, session: requests.Session, timeout: int = TIMEOUT) -> str:
    """GET with simple retry and SSL verify fallback."""
    for i in range(2):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, verify=(i == 0))
            if r.ok:
                return r.text
        except requests.RequestException:
            pass
    return ""

# ---- Parsing ----
def normalize_url(base: str, href: str) -> str:
    """Make absolute URL and strip fragments."""
    if not href:
        return ""
    if not href.startswith("http"):
        href = urljoin(base, href)
    parts = list(urlparse(href))
    parts[5] = ""  # remove fragment
    return urlunparse(parts)

def parse_listing_for_detail_links(html: str) -> List[Dict]:
    """
    Return [{'href': '/qtenders/tender/display/tender-details.do?...', 'text': '...'}].
    We accept detail links that contain both 'tender' and 'detail' to avoid nav links.
    """
    out, seen = [], set()
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        h = a.get("href", "")
        h_abs = normalize_url(BASE, h)
        h_low = h_abs.lower()
        # Heuristics for QTenders detail pages:
        # - contains "/qtenders/" AND "tender" AND ("detail" or "details")
        if ("/qtenders/" in h_low) and ("tender" in h_low) and ("detail" in h_low):
            if h_abs not in seen:
                seen.add(h_abs)
                txt = (a.get_text(strip=True) or "")
                out.append({"href": h_abs, "text": txt})
    return out

def add_pagination(url: str, page_num: int) -> str:
    """
    Try common QTenders paging params. We’ll generate a few candidate URLs for page N.
    """
    if page_num <= 1:
        return url
    u = urlparse(url)
    qs = parse_qs(u.query)
    # Try currentPage
    qs["currentPage"] = [str(page_num)]
    url1 = u._replace(query=urlencode(qs, doseq=True))
    return urlunparse(url1)

def parse_detail_title_and_body(html: str) -> Tuple[str, str]:
    """Basic, no-JS parse."""
    if not html:
        return "", ""
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1") or soup.find(attrs={"role": "heading"}) or soup.title
    title = (h1.get_text(strip=True) if h1 else "") or (soup.title.get_text(strip=True) if soup.title else "")
    main = soup.find("main")
    body = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
    return title, body

def link_title_from_href(href: str, text_hint: str) -> str:
    t = (text_hint or "").strip()
    if t and t.lower() not in ("details", "view tender", "view details"):
        return t
    try:
        last = urlparse(href).path.rstrip("/").split("/")[-1]
        return last or "Untitled"
    except Exception:
        return "Untitled"

# ---- Scoring ----
def count_hits(regex: re.Pattern, text: str) -> int:
    return len(regex.findall(text or ""))

def score_from(title: str, body: str) -> Tuple[int, Dict[str,int]]:
    # Drone (title x3, body x2) ; Energy (title x2, body x1)
    t_drone  = count_hits(DRONE_RE, title)
    b_drone  = count_hits(DRONE_RE, body)
    t_energy = count_hits(ENERGY_RE, title)
    b_energy = count_hits(ENERGY_RE, body)
    score = (3 * t_drone + 2 * b_drone) + (2 * t_energy + 1 * b_energy)
    return score, {
        "drone_title": t_drone, "drone_body": b_drone,
        "energy_title": t_energy, "energy_body": b_energy
    }

# ---- Main ----
def run():
    session = make_session()

    # 1) Find a listing URL that yields detail links
    chosen_url = None
    first_links: List[Dict] = []
    for cand in LIST_URL_CANDIDATES:
        html = fetch(cand, session)
        links = parse_listing_for_detail_links(html)
        logging.info(f"Probe '{cand}' -> {len(links)} detail link(s)")
        if links:
            chosen_url = cand
            first_links = links
            break

    if not chosen_url:
        # Write empty outputs with note and return
        rows_all: List[Dict] = []
        with open("qld-raw.json", "w", encoding="utf-8") as f:
            json.dump(rows_all, f, ensure_ascii=False, indent=2)
        with open("qld-links.json", "w", encoding="utf-8") as f:
            json.dump(rows_all, f, ensure_ascii=False, indent=2)
        logging.warning("No QLD listing produced links (site layout may have changed).")
        print("OK (no links)")
        return

    # 2) Collect across pages
    all_items: List[Dict] = []
    for i in range(1, max(1, PAGES) + 1):
        url_i = chosen_url if i == 1 else add_pagination(chosen_url, i)
        logging.info(f"Listing page {i}: {url_i}")
        html = fetch(url_i, session)
        items = parse_listing_for_detail_links(html)
        # If page 1 and probe had links, reuse to avoid re-parsing
        if i == 1 and first_links:
            items = first_links
            first_links = []
        logging.info(f"  found {len(items)} detail anchor(s)")
        all_items.extend(items)
        time.sleep(0.3 + random.uniform(0, 0.3))

    # Deduplicate
    seen, uniq = set(), []
    for it in all_items:
        href = it["href"]
        if href not in seen:
            seen.add(href)
            uniq.append(it)

    # 3) Peek at a limited number of detail pages
    details_text: Dict[str, Dict[str, str]] = {}
    for it in uniq[:DETAIL_LIMIT]:
        url = it["href"]
        html = fetch(url, session)
        title, body = parse_detail_title_and_body(html)
        details_text[url] = {"title": title, "body": body}
        time.sleep(0.2 + random.uniform(0, 0.2))

    # 4) Build rows and filter
    rows_all: List[Dict] = []
    for it in uniq:
        url = it["href"]
        dt = details_text.get(url, {})
        title_text = dt.get("title") or link_title_from_href(url, it.get("text", ""))
        body_text  = dt.get("body", "")
        text_combo = f"{title_text} {body_text}"

        score, hits = score_from(title_text, body_text)
        drone_hits_total = hits["drone_title"] + hits["drone_body"]
        negative_hit = bool(NEGATIVE_RE.search(text_combo))

        include = (drone_hits_total > 0) and (True if drone_hits_total > 0 else not negative_hit)

        rows_all.append({
            "title": title_text,
            "source_url": url,
            "state": "QLD",
            "score": score,
            "hits": hits,
            "include": include
        })

    # Sort and choose filtered output
    rows_all.sort(key=lambda r: (-r["score"], r["title"].lower()))
    rows_out = [r for r in rows_all if r["include"]] if ONLY_FILTERED else rows_all

    # 5) Write files
    with open("qld-raw.json", "w", encoding="utf-8") as f:
        json.dump(rows_all, f, ensure_ascii=False, indent=2)
    with open("qld-links.json", "w", encoding="utf-8") as f:
        json.dump(rows_out, f, ensure_ascii=False, indent=2)

    logging.info(f"QLD: wrote {len(rows_out)} rows (filtered={ONLY_FILTERED}, drone_required={DRONE_REQUIRED}).")
    print("OK")

if __name__ == "__main__":
    run()
