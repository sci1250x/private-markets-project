"""
london_cafe_pipeline.py
=======================
Stages:
  1. Google Places API  → all cafes in London (no limit)
  2. Smart brand clean  → rapidfuzz + prefix normalisation (free, unlimited)
  3. Parent lookup      → brand → (parent company, ticker candidate)
  4. Wikipedia pass     → ONE batch call per company fetches BOTH
                          subsidiaries AND private valuation estimates
                          (revenue / funding fields from infobox)
                          Zero extra calls vs fetching separately.
  5. yfinance           → validate ticker, live price + financials
  6. Final DataFrame    → one row per place + all enriched columns

Cost summary:
  Google Places  ~$6.60 one-off (cached forever after first run)
  Wikipedia      free, unlimited, no key
  yfinance       free, unlimited, no key
  Everything else free
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from rapidfuzz import fuzz, process

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────

LONDON_CENTER   = (51.5074, -0.1278)
GRID_SPAN_KM    = 28
RADIUS_M        = 1800          # 1.8 km circles, ~33% overlap
PLACES_DELAY_S  = 0.12
NEARBY_COST_USD = 0.032

DATA_DIR        = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

PLACES_CACHE    = DATA_DIR / "places_cache.json"
WIKI_CACHE      = DATA_DIR / "wiki_cache.json"     # subsidiaries + valuations together

WIKI_API        = "https://en.wikipedia.org/w/api.php"
WIKI_HEADERS    = {"User-Agent": "LondonCafeMapper/1.0 (research; wikipedia.org)"}
WIKI_BATCH      = 10    # titles per MediaWiki request

# ─────────────────────────────────────────────────────────────────
# KNOWLEDGE BASE — brand → parent
# ─────────────────────────────────────────────────────────────────

CHAIN_KEYWORDS: list[str] = [
    # ── Major international chains ───────────────────────────────
    "Starbucks", "Costa Coffee", "Caffè Nero", "Caffe Nero",
    "Pret A Manger", "Pret", "McCafé", "McDonald's", "Greggs",
    "Tim Hortons", "Peet's Coffee", "JDE Peet", "Nespresso",
    "Blue Bottle", "Black Sheep Coffee", "Upper Crust",
    "Caffè Ritazza", "Caffe Ritazza", "Gail's Bakery", "Gail's",
    "Benugo", "Leon", "AMT Coffee", "Esquires Coffee",
    "Coffee Republic", "Le Pain Quotidien", "Paul Bakery",
    "Harris + Hoole", "Patisserie Valerie", "Itsu",
    "Boston Tea Party", "Aroma", "Lavazza", "EAT",
    "Blank Street", "Blank Street Coffee",
    # ── London / UK independents with multiple sites ─────────────
    "Joe & the Juice", "Joe and the Juice",
    "Ole & Steen",
    "WatchHouse", "Watch House",
    "Grind",
    "Caravan",
    "Daisy Green",
    "Notes Coffee",
    "Workshop Coffee",
    "Allpress Espresso", "Allpress",
    "Ozone Coffee",
    "Federation Coffee",
    "Redemption Roasters",
    "Origin Coffee",
    "Milk Beach",
    "Vagabond",
    "Foxcroft & Ginger",
    "Crussh",
]

# Canonical brand names — short/variant forms collapse to the full official name.
# Applied as a final step in normalise_brand so both API and scraped data benefit.
BRAND_CANONICAL: dict[str, str] = {
    "Pret":                "Pret A Manger",
    "Gail's":              "Gail's Bakery",
    "Caffe Nero":          "Caffè Nero",
    "Caffe Ritazza":       "Caffè Ritazza",
    "Blank Street":        "Blank Street Coffee",
    "Joe and the Juice":   "Joe & the Juice",
    "Watch House":         "WatchHouse",
    "Allpress":            "Allpress Espresso",
}

BRAND_TO_PARENT: dict[str, tuple[str, str | None]] = {
    # ── Listed parents ────────────────────────────────────────────
    "Starbucks":           ("Starbucks Corporation",                "SBUX"),
    "Costa Coffee":        ("The Coca-Cola Company",                "KO"),
    "McCafé":              ("McDonald's Corporation",               "MCD"),
    "McDonald's":          ("McDonald's Corporation",               "MCD"),
    "Greggs":              ("Greggs plc",                           "GRG.L"),
    "Tim Hortons":         ("Restaurant Brands International Inc.", "QSR"),
    "Peet's Coffee":       ("JDE Peet's N.V.",                      "JDEP.AS"),
    "JDE Peet":            ("JDE Peet's N.V.",                      "JDEP.AS"),
    "Nespresso":           ("Nestle S.A.",                          "NESN.SW"),
    "Blue Bottle":         ("Nestle S.A.",                          "NESN.SW"),
    "Upper Crust":         ("SSP Group plc",                        "SSPG.L"),
    "Caffè Ritazza":       ("SSP Group plc",                        "SSPG.L"),
    "Caffe Ritazza":       ("SSP Group plc",                        "SSPG.L"),
    "Esquires Coffee":     ("Cooks Coffee Company Ltd",             "COOK.NZ"),
    "Lavazza":             ("Luigi Lavazza S.p.A.",                 None),
    # ── Private parents ───────────────────────────────────────────
    "Caffè Nero":          ("Caffè Nero Group Ltd",                 None),
    "Caffe Nero":          ("Caffè Nero Group Ltd",                 None),
    "Pret A Manger":       ("JAB Holding Company",                  None),
    "Pret":                ("JAB Holding Company",                  None),
    "EAT":                 ("JAB Holding Company",                  None),
    "Black Sheep Coffee":  ("Black Sheep Coffee Ltd",               None),
    "Gail's Bakery":       ("Gail's Bakery Ltd",                    None),
    "Gail's":              ("Gail's Bakery Ltd",                    None),
    "Leon":                ("Leon Restaurants Ltd",                  None),
    "Benugo":              ("Benugo Ltd",                           None),
    "AMT Coffee":          ("AMT Coffee Ltd",                       None),
    "Coffee Republic":     ("Coffee Republic Ltd",                  None),
    "Le Pain Quotidien":   ("LPQ International SA",                 None),
    "Paul Bakery":         ("Groupe Holder",                        None),
    "Harris + Hoole":      ("Harris + Hoole Coffee Ltd",            None),
    "Patisserie Valerie":  ("Patisserie Holdings",                  None),
    "Itsu":                ("Itsu Limited",                         None),
    "Boston Tea Party":    ("Boston Tea Party Cafes Ltd",           None),
    "Blank Street":        ("Blank Street Coffee Ltd",              None),
    "Blank Street Coffee": ("Blank Street Coffee Ltd",              None),
    # ── New London / UK chains ────────────────────────────────────
    "Joe & the Juice":     ("Joe & the Juice ApS",                  None),
    "Joe and the Juice":   ("Joe & the Juice ApS",                  None),
    "Ole & Steen":         ("Lagkagehuset A/S",                     None),
    "WatchHouse":          ("WatchHouse Coffee Ltd",                None),
    "Watch House":         ("WatchHouse Coffee Ltd",                None),
    "Grind":               ("Grind Coffee Bar Ltd",                 None),
    "Caravan":             ("Caravan Restaurants Ltd",              None),
    "Daisy Green":         ("Daisy Green Collection Ltd",           None),
    "Notes Coffee":        ("Notes Coffee Roasters Ltd",            None),
    "Workshop Coffee":     ("Workshop Coffee Co. Ltd",              None),
    "Allpress Espresso":   ("Allpress Espresso Ltd",                None),
    "Allpress":            ("Allpress Espresso Ltd",                None),
    "Ozone Coffee":        ("Ozone Coffee Roasters Ltd",            None),
    "Federation Coffee":   ("Federation Coffee Ltd",                None),
    "Redemption Roasters": ("Redemption Roasters Ltd",              None),
    "Origin Coffee":       ("Origin Coffee Ltd",                    None),
    "Milk Beach":          ("Milk Beach Ltd",                       None),
    "Vagabond":            ("Vagabond Wines Ltd",                   None),
    "Foxcroft & Ginger":   ("Foxcroft & Ginger Ltd",                None),
    "Crussh":              ("Crussh Fit Food Bar Ltd",               None),
}

# ─────────────────────────────────────────────────────────────────
# CACHE HELPERS
# ─────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                    encoding="utf-8")

# ─────────────────────────────────────────────────────────────────
# STAGE 1 — COST ESTIMATE (no API calls)
# ─────────────────────────────────────────────────────────────────

def estimate_cost() -> dict:
    lat_per_km = 1 / 111.0
    lon_per_km = 1 / (111.0 * math.cos(math.radians(LONDON_CENTER[0])))
    step_km    = (RADIUS_M / 1000) * 1.5
    steps      = math.ceil(GRID_SPAN_KM / step_km)
    grid_pts   = steps * steps
    api_calls  = math.ceil(grid_pts * 1.4)
    cost       = round(api_calls * NEARBY_COST_USD, 2)
    return {
        "grid_points": grid_pts,
        "api_calls":   api_calls,
        "cost_usd":    cost,
        "free_pct":    round((cost / 200) * 100, 1),
    }

# ─────────────────────────────────────────────────────────────────
# STAGE 2 — GOOGLE PLACES FETCH (full London, no limit)
# ─────────────────────────────────────────────────────────────────

def _generate_grid() -> list[tuple[float, float]]:
    lat_c, lon_c = LONDON_CENTER
    latkm  = 1 / 111.0
    lonkm  = 1 / (111.0 * math.cos(math.radians(lat_c)))
    step   = (RADIUS_M / 1000) * 1.5
    half   = GRID_SPAN_KM / 2
    pts: list[tuple[float, float]] = []
    lat = lat_c - half * latkm
    while lat <= lat_c + half * latkm:
        lon = lon_c - half * lonkm
        while lon <= lon_c + half * lonkm:
            pts.append((round(lat, 5), round(lon, 5)))
            lon += step * lonkm
        lat += step * latkm
    return pts


def _nearby_page(lat: float, lon: float, api_key: str,
                 token: str | None = None) -> tuple[list, str | None]:
    url    = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params: dict = {"key": api_key}
    if token:
        params["pagetoken"] = token
    else:
        params.update({
            "location": f"{lat},{lon}",
            "radius":   RADIUS_M,
            "type":     "cafe",
        })
    try:
        r    = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            return [], None
        return data.get("results", []), data.get("next_page_token")
    except Exception:
        return [], None


def fetch_places(api_key: str, use_cache: bool = True,
                 progress_cb=None) -> pd.DataFrame:
    """
    Fetch ALL cafés in London. No limit — full coverage.
    Results cached to disk; repeat runs load from cache instantly.
    """
    cache   = _load_json(PLACES_CACHE) if use_cache else {}
    seen    = set(cache.keys())
    records = list(cache.values())
    grid    = _generate_grid()

    for i, (lat, lon) in enumerate(grid):
        token = None
        page  = 0
        while True:
            results, token = _nearby_page(lat, lon, api_key, token)
            for p in results:
                pid = p.get("place_id")
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                rec = {
                    "place_id":        pid,
                    "name":            p.get("name", ""),
                    "address":         p.get("vicinity", ""),
                    "lat":             p.get("geometry", {}).get("location", {}).get("lat"),
                    "lon":             p.get("geometry", {}).get("location", {}).get("lng"),
                    "rating":          p.get("rating"),
                    "review_count":    p.get("user_ratings_total"),
                    "price_level":     p.get("price_level"),
                    "business_status": p.get("business_status", "OPERATIONAL"),
                    "types":           "|".join(p.get("types", [])),
                }
                records.append(rec)
                cache[pid] = rec
            page += 1
            if not token or page >= 3:
                break
            time.sleep(2)

        if progress_cb:
            progress_cb(i + 1, len(grid), len(records))
        else:
            time.sleep(PLACES_DELAY_S)

    if use_cache:
        _save_json(PLACES_CACHE, cache)

    return pd.DataFrame(records)


def load_cached_places() -> pd.DataFrame | None:
    cache = _load_json(PLACES_CACHE)
    return pd.DataFrame(list(cache.values())) if cache else None


def clear_places_cache() -> None:
    if PLACES_CACHE.exists():
        PLACES_CACHE.unlink()

# ─────────────────────────────────────────────────────────────────
# STAGE 3 — SMART BRAND NORMALISATION
#
# Strategy (free, unlimited, runs locally):
#   1. Strip location suffixes with regex
#      "Starbucks - Canary Wharf" → "Starbucks"
#   2. Exact prefix match against CHAIN_KEYWORDS
#   3. Rapidfuzz token_sort_ratio (handles word order variation)
#   4. If no match → independent café
#
# Why not a transformer / spaCy NER?
#   NER models for brand extraction need fine-tuning on café names.
#   Off-the-shelf NER labels "Starbucks Canary Wharf" as ORG+LOC
#   but won't tell you the brand name is just "Starbucks". Rapidfuzz
#   with a curated keyword list is faster, free, and more accurate
#   for this specific domain.
# ─────────────────────────────────────────────────────────────────

# Patterns that indicate a location suffix after the brand name
_SUFFIX_PATTERNS = re.compile(
    r"""
    \s*[-–|/\\(]\s*.*$     |   # anything after - – | / \ (
    \s+(at|in|near|@)\s+.*$    # "at", "in", "near", "@" location
    """,
    re.VERBOSE | re.IGNORECASE,
)

def _strip_location_suffix(name: str) -> str:
    """Remove location qualifiers from a place name."""
    cleaned = _SUFFIX_PATTERNS.sub("", name).strip()
    return cleaned if len(cleaned) >= 3 else name


def normalise_brand(place_name: str) -> str | None:
    """
    Extract the chain brand from a raw Google Places name.
    Returns the canonical brand string from CHAIN_KEYWORDS, or None.
    """
    # Step 1: strip suffix to isolate brand token
    stripped = _strip_location_suffix(place_name)

    # Step 2: exact prefix match (fast path)
    upper = stripped.upper()
    for kw in CHAIN_KEYWORDS:
        if upper.startswith(kw.upper()):
            return BRAND_CANONICAL.get(kw, kw)

    # Step 3: rapidfuzz token_sort_ratio on stripped name
    # token_sort_ratio handles "Nero Caffè" == "Caffè Nero"
    match, score, _ = process.extractOne(
        stripped, CHAIN_KEYWORDS, scorer=fuzz.token_sort_ratio
    )
    if score >= 82:
        return BRAND_CANONICAL.get(match, match)

    # Step 4: try original name in case stripping removed too much
    match2, score2, _ = process.extractOne(
        place_name, CHAIN_KEYWORDS, scorer=fuzz.partial_ratio
    )
    if score2 >= 88:
        return BRAND_CANONICAL.get(match2, match2)

    return None

# ─────────────────────────────────────────────────────────────────
# STAGE 4 — WIKIPEDIA SINGLE PASS
#
# One batch call per company fetches BOTH:
#   a) subsidiaries  — from infobox "subsidiaries" field
#   b) private valuation estimate — from infobox "revenue",
#      "assets", "equity" or "num_employees" fields
#      (used only for private companies; listed ones use yfinance)
#
# Call count: 1 search + 1 batch wikitext per WIKI_BATCH companies.
# With ~15 parent companies that's 2 total HTTP requests.
# Everything cached — re-runs cost zero.
# ─────────────────────────────────────────────────────────────────

# Infobox fields to extract in a single pass
_SUBS_FIELDS    = {"subsidiaries", "subsidiary"}
_REVENUE_FIELDS = {"revenue", "net_income", "assets",
                   "equity", "total_equity", "valuation"}

# Revenue/valuation figure regex — parses wikitext amounts
_WIKI_VAL_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*"
    r"(?:×\s*10\^?\d+\s*)?"          # scientific notation e.g. 1.2×10^9
    r"(billion|million|trillion|bn|m|b|tr)?\b",
    re.IGNORECASE,
)


def _clean_wiki_field(raw: str) -> list[str]:
    """
    Parse a wikitext infobox field value into a list of clean names.
    Handles [[Link|Label]], {{plainlist}}, <br>, bullets, commas.
    """
    if not raw:
        return []
    raw = re.sub(r"\{\{(?:plain|unbulleted\s+)?list\s*\|(.*?)\}\}",
                 r"\1", raw, flags=re.IGNORECASE | re.DOTALL)
    raw = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", raw)
    raw = re.sub(r"\{\{[^}]*\}\}", "", raw)
    raw = re.sub(r"<[^>]+>", "\n", raw)
    parts = re.split(r"[\n\r;,*•]+", raw)
    results = []
    for p in parts:
        p = re.sub(r"\[\d+\]", "", p.strip(" |\t'\"")).strip()
        if len(p) >= 3:
            results.append(p)
    return results


def _parse_wiki_amount(raw: str) -> float | None:
    """
    Extract a financial figure from a wikitext amount string.
    Returns USD billions (best-effort; Wikipedia uses mixed currencies).
    e.g. "{{increase}} £2.1 billion" → 2.1
         "US$450 million"            → 0.45
    """
    # Strip wiki markup first
    clean = re.sub(r"\{\{[^}]*\}\}", "", raw)
    clean = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", clean)
    clean = re.sub(r"<[^>]+>", "", clean)

    for m in _WIKI_VAL_RE.finditer(clean):
        try:
            value = float(m.group(1).replace(",", ""))
            unit  = (m.group(2) or "").lower()
            if unit in ("trillion", "tr"):
                return round(value * 1000, 1)
            if unit in ("billion", "bn", "b"):
                return round(value, 2)
            if unit in ("million", "m"):
                return round(value / 1000, 3)
            # bare number — skip (could be year, employee count, etc.)
        except ValueError:
            continue
    return None


def _wiki_search_title(company_name: str) -> str | None:
    """One search call to find the Wikipedia article title."""
    try:
        r = requests.get(WIKI_API, headers=WIKI_HEADERS, timeout=10, params={
            "action": "query", "format": "json",
            "list": "search", "srsearch": company_name, "srlimit": 3,
        })
        results = r.json().get("query", {}).get("search", [])
        return results[0]["title"] if results else None
    except Exception:
        return None


def _wiki_batch_fetch(titles: list[str]) -> dict[str, dict]:
    """
    Batch-fetch wikitext for up to WIKI_BATCH titles per request.
    Returns {title: {"subsidiaries": [...], "est_val_bn": float|None}}
    This is the single HTTP call that replaces what used to be two
    separate calls (one for subs, one for valuation).
    """
    out: dict[str, dict] = {}

    for i in range(0, len(titles), WIKI_BATCH):
        batch  = titles[i: i + WIKI_BATCH]
        try:
            r = requests.get(WIKI_API, headers=WIKI_HEADERS, timeout=15, params={
                "action": "query", "format": "json",
                "prop": "revisions", "rvprop": "content",
                "rvslots": "main", "titles": "|".join(batch),
            })
            pages = r.json().get("query", {}).get("pages", {})
        except Exception:
            continue

        for page in pages.values():
            title    = page.get("title", "")
            wikitext = (page.get("revisions", [{}])[0]
                        .get("slots", {}).get("main", {}).get("*", ""))
            subs:    list[str]   = []
            est_val: float | None = None

            for line in wikitext.splitlines():
                stripped = line.strip()
                if not stripped.startswith("|"):
                    continue
                # Extract field name and value
                m = re.match(r"\|\s*(\w+)\s*=\s*(.*)", stripped)
                if not m:
                    continue
                field = m.group(1).lower()
                value = m.group(2).strip()

                if field in _SUBS_FIELDS and not subs:
                    subs = _clean_wiki_field(value)

                if field in _REVENUE_FIELDS and est_val is None:
                    est_val = _parse_wiki_amount(value)

            out[title] = {"subsidiaries": subs, "est_val_bn": est_val}

        time.sleep(0.3)   # polite pause between batches

    return out


def wiki_enrich_companies(company_names: list[str]) -> dict[str, dict]:
    """
    For each company name return:
      {"subsidiaries": [...], "est_val_bn": float|None}

    Uses a single Wikipedia pass — one search + one batch wikitext
    request covers both subsidiaries AND valuation estimates.
    Results cached to disk in wiki_cache.json — fetched once, free forever.

    Cache key: company name
    Cache miss: triggers search + batch fetch
    Cache hit:  returned instantly, zero network calls
    """
    cache = _load_json(WIKI_CACHE)
    to_fetch: list[tuple[str, str]] = []   # (company_name, wiki_title)

    for name in company_names:
        if name in cache:
            continue
        title = _wiki_search_title(name)
        if title:
            to_fetch.append((name, title))
        else:
            cache[name] = {"subsidiaries": [], "est_val_bn": None}

    if to_fetch:
        wiki_titles  = [t for _, t in to_fetch]
        wiki_results = _wiki_batch_fetch(wiki_titles)

        for company_name, wiki_title in to_fetch:
            result = wiki_results.get(wiki_title)
            if not result:
                # Fuzzy title match fallback
                for k, v in wiki_results.items():
                    if (k.lower() in wiki_title.lower()
                            or wiki_title.lower() in k.lower()):
                        result = v
                        break
            cache[company_name] = result or {"subsidiaries": [], "est_val_bn": None}

        _save_json(WIKI_CACHE, cache)

    return cache


# ─────────────────────────────────────────────────────────────────
# STAGE 5 — YFINANCE ENRICHMENT
# ─────────────────────────────────────────────────────────────────

_FX_CACHE:   dict[str, float] = {}
_INFO_CACHE: dict[str, dict]  = {}


def _get_fx_to_usd(currency: str) -> float:
    if not currency or currency == "USD":
        return 1.0
    if currency in _FX_CACHE:
        return _FX_CACHE[currency]
    try:
        if currency == "GBp":
            rate   = yf.Ticker("GBPUSD=X").info.get("regularMarketPrice", 0)
            result = round(rate / 100, 6)
        else:
            result = (yf.Ticker(f"{currency}USD=X").info
                      .get("regularMarketPrice", 1.0) or 1.0)
        _FX_CACHE[currency] = result
        return result
    except Exception:
        return 1.0


def resolve_ticker(ticker: str | None) -> dict:
    """
    Validate ticker via yfinance. Cached in module-level dict.
    Returns enriched dict; is_listed=False if private/invalid.
    """
    key   = ticker or "__none__"
    empty = dict(
        ticker=ticker or "PRIVATE", resolved_name=None,
        exchange=None, currency=None, price_local=None,
        price_usd=None, market_cap_bn=None, pe_ratio=None,
        dividend_yield=None, sector=None, industry=None,
        week52_high=None, week52_low=None,
        is_listed=False, display_label="Private / Unlisted",
    )
    if key in _INFO_CACHE:
        return _INFO_CACHE[key]
    if not ticker:
        _INFO_CACHE[key] = empty
        return empty
    try:
        info  = yf.Ticker(ticker).info
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price:
            _INFO_CACHE[key] = empty
            return empty
        currency = info.get("currency", "USD")
        fx       = _get_fx_to_usd(currency)
        mkt_cap  = info.get("marketCap")
        pe       = info.get("trailingPE")
        div      = info.get("dividendYield")
        name     = info.get("longName") or info.get("shortName", ticker)
        p_usd    = round(price * fx, 2)
        result   = dict(
            ticker         = ticker,
            resolved_name  = name,
            exchange       = info.get("exchange") or info.get("fullExchangeName"),
            currency       = currency,
            price_local    = round(price, 2),
            price_usd      = p_usd,
            market_cap_bn  = round(mkt_cap / 1e9, 2) if mkt_cap else None,
            pe_ratio       = round(pe, 2)             if pe      else None,
            dividend_yield = round(div * 100, 2)      if div     else None,
            sector         = info.get("sector"),
            industry       = info.get("industry"),
            week52_high    = info.get("fiftyTwoWeekHigh"),
            week52_low     = info.get("fiftyTwoWeekLow"),
            is_listed      = True,
            display_label  = f"{name}  ({ticker}  ${p_usd:.2f})",
        )
        _INFO_CACHE[key] = result
        return result
    except Exception:
        _INFO_CACHE[key] = empty
        return empty

# ─────────────────────────────────────────────────────────────────
# STAGE 6 — PRIVATE COMPANY VALUATION via Crunchbase (free tier)
#
# Strategy — three layers, each free, no API key required:
#
#  Layer 1: Crunchbase public organisation page
#           Scrape the HTML of crunchbase.com/organization/<slug>
#           The page embeds JSON-LD and meta tags with valuation /
#           last funding round data for many companies.
#
#  Layer 2: Crunchbase autocomplete API
#           crunchbase.com/v4/data/searches/organizations is
#           publicly accessible without a key for simple queries.
#           Returns funding_total and last_funding_type.
#
#  Layer 3: Regex extraction from any page text
#           Parses £/$/€ valuation figures from whatever HTML
#           was returned — catches informal disclosures.
#
# All results cached to disk — each company scraped once only.
# Rate: 1 request per company, ~1s delay between companies.
# ─────────────────────────────────────────────────────────────────

_VAL_RE = re.compile(
    r"(?:valued?\s*(?:at)?|valuation\s*(?:of)?|worth|raised?|funding)\s*"
    r"[£$€]?\s*(\d+(?:[.,]\d+)?)\s*(billion|million|bn|m|b)\b",
    re.IGNORECASE,
)

_CB_SLUG_CLEAN = re.compile(r"[^a-z0-9]+")


def _to_cb_slug(company_name: str) -> str:
    """Convert a company name to a Crunchbase URL slug."""
    # e.g. "Caffè Nero Group Ltd" → "caffe-nero-group"
    name = company_name.lower()
    name = re.sub(r"\b(ltd|plc|inc|llc|group|company|co|corp|s\.?a\.?|n\.?v\.?)\b",
                  "", name)
    name = _CB_SLUG_CLEAN.sub("-", name).strip("-")
    return name


def _parse_valuation_from_text(text: str) -> float | None:
    """Extract the first £/$ valuation figure from arbitrary text. Returns USD bn."""
    for m in _VAL_RE.finditer(text):
        try:
            raw   = m.group(1).replace(",", "")
            value = float(raw)
            unit  = m.group(2).lower()
            if unit in ("billion", "bn", "b"):
                return round(value, 2)
            if unit in ("million", "m"):
                return round(value / 1000, 3)
        except ValueError:
            continue
    return None


def _cb_fetch_org_page(slug: str) -> float | None:
    """
    Scrape Crunchbase public org page for valuation data.
    Looks for JSON-LD structured data first, then falls back
    to regex over visible page text.
    """
    url = f"{CRUNCHBASE_BASE}/{slug}"
    try:
        r = requests.get(url, headers=CB_HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        text = r.text

        # Try JSON-LD embedded in page
        ld_blocks = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            text, re.DOTALL | re.IGNORECASE,
        )
        for block in ld_blocks:
            try:
                data = json.loads(block)
                # Crunchbase sometimes embeds fundingTotal
                for key in ("fundingTotal", "valuation", "totalFunding"):
                    val_raw = data.get(key)
                    if val_raw:
                        v = _parse_valuation_from_text(str(val_raw))
                        if v:
                            return v
            except Exception:
                pass

        # Fallback: regex over raw page text
        # Strip HTML tags for cleaner matching
        clean = re.sub(r"<[^>]+>", " ", text)
        return _parse_valuation_from_text(clean)

    except Exception:
        return None


def _cb_search_api(company_name: str) -> float | None:
    """
    Use Crunchbase's public search endpoint (no key needed for
    basic queries) to find funding_total for a company.
    """
    try:
        payload = {
            "field_ids": ["identifier", "short_description",
                          "funding_total", "last_funding_type",
                          "last_funding_at"],
            "query": [{"type": "predicate", "field_id": "facet_ids",
                       "operator_id": "includes", "values": ["company"]}],
            "order": [{"field_id": "rank_org", "sort": "asc"}],
            "limit": 5,
        }
        params = {"q": company_name}
        r = requests.post(
            CRUNCHBASE_SEARCH,
            headers={**CB_HEADERS, "Content-Type": "application/json"},
            params=params,
            json=payload,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data    = r.json()
        entries = data.get("entities", [])
        if not entries:
            return None
        props = entries[0].get("properties", {})
        ft    = props.get("funding_total", {})
        if ft:
            val_usd = ft.get("value_usd")
            if val_usd:
                return round(float(val_usd) / 1e9, 3)
    except Exception:
        pass
    return None


def get_private_valuation(company_name: str) -> float | None:
    """
    Attempt to find an estimated valuation for a private company
    using Crunchbase free tier (no API key).

    Returns estimated valuation in USD billions, or None.
    Results cached to disk — each company fetched once only.

    Approach:
      1. Crunchbase search API  (structured funding_total field)
      2. Crunchbase org page    (JSON-LD + regex on page text)
      3. None if both fail
    """
    cache = _load_json(VALUATION_CACHE)
    # Cache stores None explicitly as False to distinguish
    # "cached miss" from "not yet looked up"
    if company_name in cache:
        v = cache[company_name]
        return float(v) if v else None

    # Layer 1: search API
    val = _cb_search_api(company_name)

    # Layer 2: org page
    if val is None:
        slug = _to_cb_slug(company_name)
        val  = _cb_fetch_org_page(slug)
        time.sleep(1.0)   # polite delay; Crunchbase is not rate-limited
                           # for occasional requests but be respectful

    cache[company_name] = val if val is not None else False
    _save_json(VALUATION_CACHE, cache)
    return val

# ─────────────────────────────────────────────────────────────────
# STAGE 7 — BUILD ENRICHED DATAFRAME
# ─────────────────────────────────────────────────────────────────

STOCK_FIELDS = [
    "resolved_name", "exchange", "currency", "price_usd", "price_local",
    "market_cap_bn", "pe_ratio", "dividend_yield", "sector", "industry",
    "week52_high", "week52_low", "is_listed", "display_label",
]


def build_enriched_df(places_df: pd.DataFrame,
                      progress_cb=None) -> pd.DataFrame:
    """
    Full enrichment pipeline on a raw places DataFrame.
    progress_cb(step: str, pct: float) — optional UI callback.
    """
    df = places_df.copy()

    # ── Stage 3: brand normalisation ─────────────────────────────
    if progress_cb: progress_cb("Normalising brand names…", 0.05)
    df["brand"]            = df["name"].apply(normalise_brand)
    df["parent_company"]   = df["brand"].map(
        lambda b: BRAND_TO_PARENT.get(b, (None, None))[0] if b else None
    )
    df["ticker_candidate"] = df["brand"].map(
        lambda b: BRAND_TO_PARENT.get(b, (None, None))[1] if b else None
    )

    # ── Stage 4: Wikipedia single pass (subs + private valuation) ─
    if progress_cb: progress_cb("Wikipedia: fetching subsidiaries and valuations…", 0.20)
    # ALL parents in one go — listed ones get subsidiaries,
    # private ones also get revenue/valuation estimates.
    # Same batch call, zero extra requests.
    all_parents = df["parent_company"].dropna().unique().tolist()
    wiki_data   = wiki_enrich_companies(all_parents)

    # ── Stage 5: yfinance ─────────────────────────────────────────
    if progress_cb: progress_cb("Fetching live stock data via yfinance…", 0.45)
    for field in STOCK_FIELDS:
        df[field] = df["ticker_candidate"].apply(
            lambda t: resolve_ticker(t).get(field)
        )
    df["ticker"]         = df["ticker_candidate"].fillna("PRIVATE")
    df["parent_company"] = df["parent_company"].fillna("Independent")

    # ── Private valuation from Wikipedia (no extra calls) ─────────
    if progress_cb: progress_cb("Applying private valuation estimates…", 0.65)
    df["est_private_val_bn"] = df["parent_company"].map(
        lambda p: (wiki_data.get(p) or {}).get("est_val_bn")
    )
    # Unified market cap column: real for listed, wiki estimate for private
    df["display_mktcap_bn"] = df.apply(
        lambda r: r["market_cap_bn"] if r["is_listed"]
        else r.get("est_private_val_bn"),
        axis=1,
    )

    # ── Subsidiary expansion rows ─────────────────────────────────
    if progress_cb: progress_cb("Expanding subsidiary brands…", 0.80)
    sub_rows:         list[dict] = []
    expanded_parents: set[str]   = set()

    for _, row in df[df["is_listed"] == True].iterrows():
        pname = row["resolved_name"]
        if not pname or pname in expanded_parents:
            continue
        expanded_parents.add(pname)
        stock = resolve_ticker(row["ticker_candidate"])
        subs  = (wiki_data.get(row["parent_company"]) or {}).get("subsidiaries", [])

        for sub in subs:
            if not sub or sub == row["name"]:
                continue
            # Skip if this sub is already a first-class brand with its own
            # parent mapping (e.g. Wikipedia lists Starbucks as an SSP sub
            # because SSP operates licensed Starbucks in travel hubs — but
            # Starbucks Corporation is the real owner).
            canonical_sub = normalise_brand(sub)
            if canonical_sub and canonical_sub in BRAND_TO_PARENT:
                continue
            sub_rows.append({
                "place_id":           None,
                "name":               sub,
                "address":            f"Subsidiary of {pname}",
                "lat":                None, "lon": None,
                "rating":             None, "review_count": None,
                "price_level":        None,
                "business_status":    "SUBSIDIARY",
                "types":              "subsidiary",
                "brand":              sub,
                "parent_company":     pname,
                "ticker_candidate":   row["ticker_candidate"],
                "ticker":             row["ticker_candidate"],
                "est_private_val_bn": None,
                "display_mktcap_bn":  stock.get("market_cap_bn"),
                **{f: stock.get(f) for f in STOCK_FIELDS},
            })

    if sub_rows:
        df = pd.concat([df, pd.DataFrame(sub_rows)], ignore_index=True)

    if progress_cb: progress_cb("Done.", 1.0)
    return df.drop_duplicates(subset=["name", "address"])

# ─────────────────────────────────────────────────────────────────
# DEMO DATA — full pipeline without API key
# ─────────────────────────────────────────────────────────────────

def demo_places() -> pd.DataFrame:
    """
    Realistic London café records matching the Places API schema.
    Exercises every pipeline stage without any API key.
    """
    return pd.DataFrame([
        {"place_id":"d01","name":"Starbucks Canary Wharf",         "address":"Canada Sq, E14",       "lat":51.5054,"lon":-0.0196,"rating":4.1,"review_count":1240,"price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d02","name":"Starbucks Reserve Covent Garden", "address":"Long Acre, WC2",        "lat":51.5127,"lon":-0.1232,"rating":4.4,"review_count":680, "price_level":3,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d03","name":"Costa Coffee - Liverpool Street", "address":"Liverpool St, EC2",     "lat":51.5176,"lon":-0.0823,"rating":4.2,"review_count":980, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d04","name":"Costa Coffee Canary Wharf",       "address":"Reuters Plaza, E14",    "lat":51.5041,"lon":-0.0182,"rating":4.0,"review_count":540, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d05","name":"Caffè Nero Soho",                 "address":"Old Compton St, W1",    "lat":51.5133,"lon":-0.1312,"rating":4.3,"review_count":760, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d06","name":"Pret A Manger at Victoria",       "address":"Victoria St, SW1",      "lat":51.4965,"lon":-0.1441,"rating":4.0,"review_count":1100,"price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d07","name":"Greggs (Oxford Street)",          "address":"Oxford St, W1",         "lat":51.5154,"lon":-0.1434,"rating":3.9,"review_count":2300,"price_level":1,"business_status":"OPERATIONAL","types":"bakery"},
        {"place_id":"d08","name":"McCafé Waterloo",                 "address":"Waterloo Rd, SE1",      "lat":51.5031,"lon":-0.1132,"rating":3.7,"review_count":870, "price_level":1,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d09","name":"Tim Hortons / Leicester Square",  "address":"Leicester Sq, WC2",     "lat":51.5113,"lon":-0.1300,"rating":4.0,"review_count":540, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d10","name":"Black Sheep Coffee Shoreditch",   "address":"Curtain Rd, EC2",       "lat":51.5234,"lon":-0.0815,"rating":4.5,"review_count":430, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d11","name":"Nespresso Boutique Regent St",    "address":"Regent St, W1",         "lat":51.5101,"lon":-0.1382,"rating":4.6,"review_count":320, "price_level":3,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d12","name":"Upper Crust in St Pancras",       "address":"Euston Rd, NW1",        "lat":51.5313,"lon":-0.1233,"rating":3.5,"review_count":1500,"price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d13","name":"Gail's Bakery Notting Hill",      "address":"Westbourne Gv, W11",    "lat":51.5152,"lon":-0.2002,"rating":4.5,"review_count":890, "price_level":2,"business_status":"OPERATIONAL","types":"bakery"},
        {"place_id":"d14","name":"The Neighbourhood Coffee",        "address":"Brixton Rd, SW9",       "lat":51.4624,"lon":-0.1133,"rating":4.7,"review_count":210, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d15","name":"Leon Kings Cross",                "address":"King's Cross, N1",      "lat":51.5300,"lon":-0.1234,"rating":4.1,"review_count":610, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d16","name":"Pret - Paddington",               "address":"Praed St, W2",          "lat":51.5154,"lon":-0.1755,"rating":3.9,"review_count":720, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d17","name":"Blank Street Coffee Soho",        "address":"Berwick St, W1",        "lat":51.5132,"lon":-0.1342,"rating":4.6,"review_count":380, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d18","name":"Blank Street Coffee Canary Wharf","address":"Bank St, E14",          "lat":51.5042,"lon":-0.0190,"rating":4.5,"review_count":290, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
        {"place_id":"d19","name":"Blank Street Coffee Shoreditch",  "address":"Shoreditch High St, E1","lat":51.5235,"lon":-0.0790,"rating":4.7,"review_count":440, "price_level":2,"business_status":"OPERATIONAL","types":"cafe"},
    ])


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--demo",    action="store_true")
    parser.add_argument("--no-cache",action="store_true")
    args    = parser.parse_args()
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")

    est = estimate_cost()
    print(f"\nCost estimate: ~${est['cost_usd']} ({est['free_pct']}% of $200 free tier)")
    if args.dry_run:
        raise SystemExit(0)

    raw = demo_places() if (args.demo or not api_key) else \
          fetch_places(api_key, use_cache=not args.no_cache)

    df = build_enriched_df(raw, progress_cb=lambda s, p: print(f"  [{p:.0%}] {s}"))
    print(df[["name","brand","parent_company","ticker","price_usd"]].to_string())
