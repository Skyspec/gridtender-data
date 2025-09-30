#!/usr/bin/env python3
import os, re, json, time, logging, random
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

BASE = "https://buy.nsw.gov.au"
LIST_URL = f"{BASE}/opportunity/search?types=Tenders"

# ---- Runtime knobs (via workflow inputs / env) ----
PAGES          = int(os.getenv("PAGES", "1") or "1")            # listing pages to scan
DETAIL_LIMIT   = int(os.getenv("DETAIL_LIMIT", "40") or "40")   # how many detail pages to peek (text only)
TIMEOUT        = int(os.getenv("TIMEOUT", "20") or "20")        # HTTP timeout
ONLY_FILTERED  = os.getenv("ONLY_FILTERED", "1").lower() in ("1","true","yes","y")

# ---- Broadened filters (drone-first, energy boosted) ----
DRONE_PATTERNS = [
    r"\bdrone(s)?\b", r"\buas\b", r"\buav(s)?\b", r"\brpas\b",
    r"\bremotely[- ]?piloted\b",
    r"\bthermal\b", r"\bthermograph(y|ic)\b", r"\binfrared\b", r"\bIR\b",
    r"\bLiDAR\b", r"\bphotogrammetr(y|ic)\b",
    r"\baerial (survey|inspection|mapping|photography)\b",
    r"\binspection(s)?\b", r"\bsurvey(s|ing)?\b", r"\bmapping\b",
    r"\bcondition assessment\b", r"\bvegetation (management|encroachment|clearance)\b",
    r"\bimag(e|ing)\b", r"\bcamera\b", r"\bUAV pilot\b"
]

ENERGY_PATTERNS = [
    r"\bsolar\b", r"\bPV\b", r"\bphotovoltaic\b",
    r"\bwind\b", r"\bturbine(s)?\b",
    r"\brenewable(s)?\b", r"\benergy\b",
    r"\bsubstation\b", r"\btransmission\b", r"\bdistribution\b",
    r"\bbattery( storage)?\b", r"\bBESS\b",
    r"\bO&M\b", r"\boperations? and maintenance\b", r"\bfarm\b"
]

DRONE_RE = re.compile("|".join(DRONE_PATTERNS), re.I)
ENERGY_RE = re.compile("|".join(ENERGY_PATTERNS), re.I)

def score_text(txt: str) -> Tuple[int, Dict[str,int]]:
    """Return (score, breakdown). Energy is weighted higher to prioritize energy-sector drone work."""
    txt = txt or ""
    drone_hits = len(re.findall(DRONE_RE, txt))
    energy_hits = len(re.findall(ENERGY_RE, txt))
    score = 2 * drone_hits + 3 * energy_hits
    return score, {"drone": drone_hits, "energy": energy_hits}

# ---- HTTP helpers ----
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0 Safari/537.36 GridTender/0.1")
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
def parse_listing(html: str) -> List[Dict]:
    """Return [{'href': '/prcOpportunity/...', 'text': '...'}] from listing HTML."""
    out, seen = [], set()
    if not html: 
        return out
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        h = a.get("href", "")
        if ("/prcOpportunity/" in h) and not h.endswith("/opportunity/search/"):
            if h not in seen:
                seen.add(h)
                txt = (a.get_text(strip=True) or "")
                out.append({"href": h, "text": txt})
    return out

def parse_detail_title_and_body(html: str) -> Tuple[str, str]:
    """H1/heading/title and main text (no JS execution)."""
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
    if t and t.lower() not in ("see details","details","open opportunities"):
        return t
    try:
        last = urlparse(href).path.rstrip("/").split("/")[-1]
        return last or "Untitled"
    except Exception:
        return "Untitled"

# ---- Main ----
def run():
    session = make_session()

    # 1) Collect listing links across pages
    all_items = []
    for i in range(1, max(1, PAGES) + 1):
        url = LIST_URL if i == 1 else f"{LIST_URL}&page={i}"
        logging.info(f"Listing {i}: {url}")
        html = fetch(url, session)
        items = parse_listing(html)
        logging.info(f"  found {len(items)} links")
        all_items.extend(items)
        time.sleep(0.3 + random.uniform(0, 0.3))

    # Deduplicate by href
    seen, uniq = set(), []
    for it in all_items:
        if it["href"] not in seen:
            seen.add(it["href"])
            uniq.append(it)

    # 2) Peek at a limited number of detail pages to improve filtering
    details_text: Dict[str, Dict[str, str]] = {}
    for it in uniq[:DETAIL_LIMIT]:
        url = urljoin(BASE, it["href"])
        html = fetch(url, session)
        title, body = parse_detail_title_and_body(html)
        details_text[it["href"]] = {"title": title, "body": body}
        time.sleep(0.2 + random.uniform(0, 0.2))

    # 3) Build rows and score/filter
    rows_all: List[Dict] = []
    for it in uniq:
        href = it["href"]
        url = urljoin(BASE, href)
        dt = details_text.get(href, {})
        title = dt.get("title") or link_title_from_href(href, it.get("text", ""))
        score, breakdown = score_text(" ".join([it.get("text", ""), dt.get("title", ""), dt.get("body", "")]))
        rows_all.append({
            "title": title,
            "source_url": url,
            "state": "NSW",
            "score": score,
            "hits": breakdown
        })

    # Sort: highest score first, then title
    rows_all.sort(key=lambda r: (-r["score"], r["title"].lower()))

    # Filtered view; fallback to top 20 if empty so it's never blank
    if ONLY_FILTERED:
        rows_filtered = [r for r in rows_all if r["score"] > 0]
        if not rows_filtered:
            rows_filtered = rows_all[:20]
    else:
        rows_filtered = rows_all

    # 4) Write files
    with open("nsw-raw.json", "w", encoding="utf-8") as f:
        json.dump(rows_all, f, ensure_ascii=False, indent=2)

    with open("nsw-links.json", "w", encoding="utf-8") as f:
        json.dump(rows_filtered, f, ensure_ascii=False, indent=2)

    logging.info(f"Wrote {len(rows_filtered)} filtered rows (out of {len(rows_all)} total).")
    print("OK")

if __name__ == "__main__":
    run()
