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
PAGES          = int(os.getenv("PAGES", "2") or "2")            # listing pages to scan
DETAIL_LIMIT   = int(os.getenv("DETAIL_LIMIT", "40") or "40")   # how many detail pages to peek (text only)
TIMEOUT        = int(os.getenv("TIMEOUT", "20") or "20")        # HTTP timeout
ONLY_FILTERED  = os.getenv("ONLY_FILTERED", "1").lower() in ("1","true","yes","y")
DRONE_REQUIRED = True  # hard-on now: must have a drone signal; set False to relax

# ---- Drone & Energy filters (strict) ----
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

# Common non-drone advisory/strategy keywords we want to EXCLUDE unless a drone signal is present
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

def count_hits(regex: re.Pattern, text: str) -> int:
    return len(regex.findall(text or ""))

def score_from(title: str, body: str) -> Tuple[int, Dict[str,int]]:
    """
    Title hits are worth more. Energy boosts ranking but cannot qualify alone.
    """
    t_drone  = count_hits(DRONE_RE, title)
    b_drone  = count_hits(DRONE_RE, body)
    t_energy = count_hits(ENERGY_RE, title)
    b_energy = count_hits(ENERGY_RE, body)

    # Drone signals (title weighted 3, body 2)
    drone_score  = 3 * t_drone + 2 * b_drone
    # Energy bonus (title weighted 2, body 1)
    energy_score = 2 * t_energy + 1 * b_energy

    score = drone_score + energy_score
    hits = {
        "drone_title": t_drone, "drone_body": b_drone,
        "energy_title": t_energy, "energy_body": b_energy
    }
    return score, hits

# ---- HTTP helpers ----
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0 Safari/537.36 GridTender/0.3")
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
    if t and t.lower() not in ("see details", "details", "open opportunities"):
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
        title_text = dt.get("title") or link_title_from_href(href, it.get("text", ""))
        body_text  = dt.get("body", "")

        score, hits = score_from(title_text, body_text)
        drone_hits_total = hits["drone_title"] + hits["drone_body"]
        negative_hit = bool(NEGATIVE_RE.search(f"{title_text} {body_text}"))

        # Include rules:
        # - Require explicit drone signal
        # - If negative pattern is present with zero drone signal, exclude
        include = (drone_hits_total > 0) and (True if drone_hits_total > 0 else not negative_hit)

        rows_all.append({
            "title": title_text,
            "source_url": url,
            "state": "NSW",
            "score": score,
            "hits": hits,
            "include": include
        })

    # Sort: highest score first, then title
    rows_all.sort(key=lambda r: (-r["score"], r["title"].lower()))

    # 4) Output
    if ONLY_FILTERED:
        rows_out = [r for r in rows_all if r["include"]]
    else:
        rows_out = rows_all

    with open("nsw-raw.json", "w", encoding="utf-8") as f:
        json.dump(rows_all, f, ensure_ascii=False, indent=2)

    with open("nsw-links.json", "w", encoding="utf-8") as f:
        json.dump(rows_out, f, ensure_ascii=False, indent=2)

    logging.info(f"Wrote {len(rows_out)} rows (filtered={ONLY_FILTERED}, drone_required={DRONE_REQUIRED}).")
    print("OK")

if __name__ == "__main__":
    run()
