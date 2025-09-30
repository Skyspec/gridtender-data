#!/usr/bin/env python3
import os, re, json, time, logging, random
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

BASE = "https://buy.nsw.gov.au"
LIST_URL = f"{BASE}/opportunity/search?types=Tenders"

# ---- Runtime knobs (env) ----
PAGES          = int(os.getenv("PAGES", "1") or "1")          # listing pages to scan
DETAIL_LIMIT   = int(os.getenv("DETAIL_LIMIT", "20") or "20") # cap detail fetches (keep low = faster)
TIMEOUT        = int(os.getenv("TIMEOUT", "20") or "20")
ONLY_FILTERED  = os.getenv("ONLY_FILTERED", "1") in ("1","true","yes","y")

# ---- Drone + Energy filters (in-file for MVP, easy to edit later) ----
DRONE_PATTERNS = [
    r"\bdrone(s)?\b", r"\buas\b", r"\buav(s)?\b", r"\brpas\b", r"\bremotely piloted\b",
    r"\bremotely[- ]piloted\b", r"\bthermal\b", r"\bthermograph(y|ic)\b",
    r"\bir\s*(inspection|survey|imaging)\b", r"\binfrared\b", r"\bLiDAR\b", r"\bphotogrammetr(y|ic)\b",
    r"\baerial (survey|inspection|mapping)\b"
]
ENERGY_PATTERNS = [
    r"\bsolar\b", r"\bwind\b", r"\brenewable(s)?\b", r"\benergy\b", r"\bphotovoltaic\b",
    r"\bPV\b", r"\bfarm\b", r"\bturbine(s)?\b", r"\bsubstation\b", r"\btransmission\b", r"\bO&M\b",
    r"\boperations? and maintenance\b"
]

DRONE_RE = re.compile("|".join(DRONE_PATTERNS), re.I)
ENERGY_RE = re.compile("|".join(ENERGY_PATTERNS), re.I)

def score_text(txt: str) -> Tuple[int, Dict[str,int]]:
    """Return (score, breakdown) where score>0 means keep. Energy gets a boost."""
    txt = txt or ""
    drone_hits = len(re.findall(DRONE_RE, txt))
    energy_hits = len(re.findall(ENERGY_RE, txt))
    # basic scoring: 2 per drone hit, +3 per energy hit (prioritize energy)
    score = 2*drone_hits + 3*energy_hits
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
    """GET with retry + ssl fallback."""
    for i in range(2):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, verify=(i==0))
            if r.ok:
                # Some pages block without a referrer
                if "text/html" in r.headers.get("Content-Type",""):
                    return r.text
                return r.text  # good enough
        except requests.RequestException:
            pass
    return ""

# ---- Parsing ----
def parse_listing(html: str) -> List[Dict]:
    """Return [{'href': '/prcOpportunity/...', 'text': '...'}] from listing."""
    out, seen = [], set()
    if not html: return out
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        h = a.get("href","")
        if ("/prcOpportunity/" in h) and not h.endswith("/opportunity/search/"):
            if h not in seen:
                seen.add(h)
                txt = (a.get_text(strip=True) or "")
                out.append({"href": h, "text": txt})
    return out

def parse_detail_title_and_body(html: str) -> Tuple[str, str]:
    """Best-effort: H1 / title tag + main text (no JS)."""
    if not html: return "", ""
    soup = BeautifulSoup(html, "html.parser")
    # H1 or role=heading
    h1 = soup.find("h1")
    if not h1:
        h1 = soup.find(attrs={"role": "heading"}) or soup.title
    title = (h1.get_text(strip=True) if h1 else "") or (soup.title.get_text(strip=True) if soup.title else "")
    # Main-ish text
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

    # 1) listing pages
    all_items = []
    for i in range(1, max(1, PAGES)+1):
        url = LIST_URL if i == 1 else f"{LIST_URL}&page={i}"
        logging.info(f"Listing {i}: {url}")
        html = fetch(url, session)
        items = parse_listing(html)
        logging.info(f"  found {len(items)} links")
        all_items.extend(items)
        time.sleep(0.3 + random.uniform(0,0.3))

    # dedupe by href
    seen, uniq = set(), []
    for it in all_items:
        if it["href"] not in seen:
            seen.add(it["href"])
            uniq.append(it)

    # 2) lightweight detail fetch (capped) to improve filtering
    details_text: Dict[str, Dict[str,str]] = {}
    for it in uniq[:DETAIL_LIMIT]:
        url = urljoin(BASE, it["href"])
        html = fetch(url, session)
        title, body = parse_detail_title_and_body(html)
        details_text[it["href"]] = {"title": title, "body": body}
        time.sleep(0.2 + random.uniform(0,0.2))

    # 3) build rows + filter
    rows_all: List[Dict] = []
    for it in uniq:
        href = it["href"]
        url = urljoin(BASE, href)
        # get best text we have to score
        dt = details_text.get(href, {})
        score, breakdown = score_text(" ".join([
            it.get("text",""), dt.get("title",""), dt.get("body","")
        ]))
        # title to display
        title = dt.get("title") or link_title_from_href(href, it.get("text",""))

        row = {
            "title": title,
            "source_url": url,
            "state": "NSW",
            "score": score,
            "hits": breakdown
        }
        rows_all.append(row)

    # sort: highest score first, then by title
    rows_all.sort(key=lambda r: (-r["score"], r["title"].lower()))

    # filtered view
    if ONLY_FILTERED:
        rows_filtered = [r for r in rows_all if r["score"] > 0]
    else:
        rows_filtered = rows_all

    # 4) write files
    with open("nsw-raw.json", "w", encoding="utf-8") as f:
        json.dump(rows_all, f, ensure_ascii=False, indent=2)

    with open("nsw-links.json", "w", encoding="utf-8") as f:
        json.dump(rows_filtered, f, ensure_ascii=False, indent=2)

    logging.info(f"Wrote {len(rows_filtered)} filtered rows (out of {len(rows_all)}).")
    print("OK")

if __name__ == "__main__":
    run()
