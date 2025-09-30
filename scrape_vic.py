#!/usr/bin/env python3
import os, re, json, time, logging, random
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- VIC endpoints ----
BASES = [
    "https://www.tenders.vic.gov.au",
    # Same platform, alternate host (often used behind the scenes)
    "https://vic.consolidatedtenders.com",
]

LIST_URL_CANDIDATES = [
    "/tender/search?preset=open",
    "/tender/search?tenderState=OPEN&groupBy=NONE&openThisWeek=false&closeThisWeek=false&awardedThisWeek=false",
    "/tender/search",
    "/",  # last resort
]

# ---- Runtime knobs ----
PAGES          = int(os.getenv("PAGES", "2") or "2")
DETAIL_LIMIT   = int(os.getenv("DETAIL_LIMIT", "40") or "40")
TIMEOUT        = int(os.getenv("TIMEOUT", "20") or "20")
ONLY_FILTERED  = os.getenv("ONLY_FILTERED", "1").lower() in ("1","true","yes","y")
WRITE_NEAR     = os.getenv("WRITE_NEAR", "1").lower() in ("1","true","yes","y")

# ---- Filters (same logic as NSW/QLD) ----
PRIMARY_DRONE_PATTERNS = [
    r"\bdrone(s)?\b", r"\buas\b", r"\buav(s)?\b", r"\brpas\b",
    r"\bremotely[- ]?piloted\b", r"\bLiDAR\b",
    r"\bthermograph(?:y|ic)\b", r"\binfrared\b", r"\bIR\b",
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
GENERIC_WORK_PATTERNS = [
    r"\binspect(?:ion|ions)?\b",
    r"\bsurvey(?:s|ing)?\b",
    r"\bmapping\b",
    r"\bmaintenance\b",
    r"\bcondition\s+assessment\b",
    r"\bvegetation\b",
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
GENERIC_RE  = re.compile("|".join(GENERIC_WORK_PATTERNS), re.I)
NEGATIVE_RE = re.compile("|".join(NEGATIVE_PATTERNS), re.I)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0 Safari/537.36 GridTender/VIC-0.3")
    })
    return s

def fetch(url: str, session: requests.Session, timeout: int = TIMEOUT) -> str:
    for i in range(2):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, verify=(i == 0))
            if r.ok:
                return r.text
        except requests.RequestException:
            pass
    return ""

def normalize_url(base: str, href: str) -> str:
    if not href:
        return ""
    if not href.startswith("http"):
        href = urljoin(base, href)
    parts = list(urlparse(href)); parts[5] = ""  # strip fragment
    return urlunparse(parts)

def collect_link_candidates(html: str, base: str) -> List[Dict]:
    """
    Return raw link-like candidates from anchors + common non-anchor patterns.
    """
    out = []
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    # 1) Regular anchors
    for a in soup.find_all("a", href=True):
        href = normalize_url(base, a.get("href", ""))
        txt  = (a.get_text(strip=True) or "")
        out.append({"href": href, "text": txt, "src": "a[href]"})
    # 2) data-href attributes (rows clickable by JS)
    for el in soup.find_all(attrs={"data-href": True}):
        href = normalize_url(base, el.get("data-href", ""))
        if href:
            txt = (el.get_text(strip=True) or "")
            out.append({"href": href, "text": txt, "src": "data-href"})
    # 3) onclick="window.location='...'"
    for el in soup.find_all(onclick=True):
        oc = el.get("onclick") or ""
        m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", oc)
        if not m:
            m = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", oc)
        if m:
            href = normalize_url(base, m.group(1))
            txt  = (el.get_text(strip=True) or "")
            out.append({"href": href, "text": txt, "src": "onclick"})
    return out

DETAIL_PATTERNS = (
    "/tender/view", "/tender/details", "/tender/display",
    "/tender/detail",  # just in case
)

def is_detail_link(url: str) -> bool:
    low = (url or "").lower()
    if not low:
        return False
    if any(host in low for host in ["tenders.vic.gov.au", "consolidatedtenders.com"]):
        if "/tender/" in low and ("id=" in low or any(p in low for p in DETAIL_PATTERNS)):
            return True
    return False

def add_pagination(url: str, page_num: int) -> str:
    if page_num <= 1:
        return url
    u = urlparse(url)
    qs = parse_qs(u.query)
    qs["page"] = [str(page_num)]
    return urlunparse(u._replace(query=urlencode(qs, doseq=True)))

def parse_detail_title_and_body(html: str) -> Tuple[str, str]:
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
    if t and t.lower() not in ("details", "view tender", "view details", "see details"):
        return t
    try:
        last = urlparse(href).path.rstrip("/").split("/")[-1]
        return last or "Untitled"
    except Exception:
        return "Untitled"

def count_hits(regex: re.Pattern, text: str) -> int:
    return len(regex.findall(text or ""))

def score_from(title: str, body: str) -> Tuple[int, Dict[str,int]]:
    t_drone  = count_hits(DRONE_RE, title)
    b_drone  = count_hits(DRONE_RE, body)
    t_energy = count_hits(ENERGY_RE, title)
    b_energy = count_hits(ENERGY_RE, body)
    score = (3 * t_drone + 2 * b_drone) + (2 * t_energy + 1 * b_energy)
    return score, {
        "drone_title": t_drone, "drone_body": b_drone,
        "energy_title": t_energy, "energy_body": b_energy
    }

def run():
    session = make_session()

    chosen_abs = None
    page1_links: List[Dict] = []
    page1_html_dump = ""

    # probe bases x list candidates
    for base in BASES:
        for path in LIST_URL_CANDIDATES:
            list_url = normalize_url(base, path)
            html = fetch(list_url, session)
            if not page1_html_dump:
                page1_html_dump = html or ""
            candidates = collect_link_candidates(html, base)
            # debug anchors (first page only, first base that returns anything)
            if candidates and not page1_links:
                page1_links = [c for c in candidates if is_detail_link(c["href"])]
                chosen_abs = list_url if page1_links else None
            logging.info(f"Probe {list_url} -> {len(candidates)} anchor-like; "
                         f"{len([c for c in candidates if is_detail_link(c['href'])])} detail-like")
            if page1_links:
                break
        if page1_links:
            break

    # Always save the first listing HTML so we can see what the site returned
    try:
        with open("vic-listing.html", "w", encoding="utf-8") as f:
            f.write(page1_html_dump or "")
    except Exception:
        pass

    # Save a compact dump of the link-like things we saw
    try:
        dbg = [{"href": c["href"], "text": c.get("text",""), "src": c.get("src","")}
               for c in (page1_links or [])][:200]
        with open("vic-debug-anchors.json", "w", encoding="utf-8") as f:
            json.dump(dbg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    if not page1_links:
        # No detail links found
        for name in ("vic-raw.json", "vic-links.json", "vic-near.json"):
            with open(name, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        logging.warning("VIC: No detail links found (see vic-listing.html and vic-debug-anchors.json).")
        print("OK (no links)")
        return

    # gather across pages (reuse chosen_abs)
    all_items: List[Dict] = []
    for i in range(1, max(1, PAGES) + 1):
        url_i = chosen_abs if i == 1 else add_pagination(chosen_abs, i)
        html = fetch(url_i, session)
        cand_i = collect_link_candidates(html, urlparse(url_i).scheme + "://" + urlparse(url_i).netloc)
        detail_i = [c for c in cand_i if is_detail_link(c["href"])]
        logging.info(f"Page {i}: {len(cand_i)} anchor-like; {len(detail_i)} detail-like")
        all_items.extend(detail_i)
        time.sleep(0.3 + random.uniform(0, 0.3))

    # dedupe on href
    seen, uniq = set(), []
    for it in all_items:
        h = it["href"]
        if h not in seen:
            seen.add(h)
            uniq.append(it)

    # peek at some detail pages (helps filtering/scoring)
    details_text: Dict[str, Dict[str, str]] = {}
    for it in uniq[:DETAIL_LIMIT]:
        url = it["href"]
        html = fetch(url, session)
        title, body = parse_detail_title_and_body(html)
        details_text[url] = {"title": title, "body": body}
        time.sleep(0.2 + random.uniform(0, 0.2))

    # classify
    rows_all: List[Dict] = []
    rows_near: List[Dict] = []

    for it in uniq:
        url = it["href"]
        dt = details_text.get(url, {})
        title_text = dt.get("title") or link_title_from_href(url, it.get("text", ""))
        body_text  = dt.get("body", "")
        text_combo = f"{title_text} {body_text}"

        score, hits = score_from(title_text, body_text)
        drone_hits_total  = hits["drone_title"] + hits["drone_body"]
        energy_hits_total = hits["energy_title"] + hits["energy_body"]
        negative_hit = bool(NEGATIVE_RE.search(text_combo))
        generic_hits = len(GENERIC_RE.findall(text_combo))

        include_strict = (drone_hits_total > 0) and (True if drone_hits_total > 0 else not negative_hit)
        include_near   = (drone_hits_total == 0) and (energy_hits_total > 0) and (generic_hits > 0) and (not negative_hit)

        row = {
            "title": title_text,
            "source_url": url,
            "state": "VIC",
            "score": score,
            "hits": hits,
            "include": include_strict
        }
        rows_all.append(row)

        if include_near:
            rows_near.append({
                "title": title_text,
                "source_url": url,
                "state": "VIC",
                "score": score,
                "hits": {**hits, "generic": generic_hits},
                "note": "Near-miss: energy + inspection/survey terms, no explicit drone keyword"
            })

    # sort & write
    rows_all.sort(key=lambda r: (-r["score"], r["title"].lower()))
    rows_near.sort(key=lambda r: (-r["score"], r["title"].lower()))
    rows_out = [r for r in rows_all if r["include"]] if ONLY_FILTERED else rows_all

    with open("vic-raw.json", "w", encoding="utf-8") as f:
        json.dump(rows_all, f, ensure_ascii=False, indent=2)
    with open("vic-links.json", "w", encoding="utf-8") as f:
        json.dump(rows_out, f, ensure_ascii=False, indent=2)
    if WRITE_NEAR:
        with open("vic-near.json", "w", encoding="utf-8") as f:
            json.dump(rows_near, f, ensure_ascii=False, indent=2)

    logging.info(f"VIC: wrote {len(rows_out)} strict rows; {len(rows_near)} near-miss rows.")
    print("OK")

if __name__ == "__main__":
    run()
