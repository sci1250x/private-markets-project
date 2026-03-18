"""
app.py — London Cafe Industry Mapping
Pure UI. All data logic imported from london_cafe_pipeline.py.

Run:
  pip install streamlit pandas yfinance requests rapidfuzz
  streamlit run app.py
"""

from __future__ import annotations

import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from london_cafe_pipeline import (
    build_enriched_df,
    clear_places_cache,
    demo_places,
    estimate_cost,
    fetch_places,
    load_cached_places,
)

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="London Cafe Industry Mapping",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────
# STYLES
# Root cause of sizing bug: Streamlit injects its own CSS that sets
# font-size on .css-* class wrappers AFTER our stylesheet loads,
# overriding plain `h1 { font-size }` rules.
# Fix: target the actual data-testid wrappers Streamlit renders into
# AND use a JS snippet to inject a <style> tag into <head> directly
# (which has higher specificity than Streamlit's injected styles).
# ─────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;600;700&display=swap');

/* ── Hide sidebar completely ────────────────────────────────── */
section[data-testid="stSidebar"]    { display: none !important; }
[data-testid="collapsedControl"]    { display: none !important; }
.main .block-container {
    max-width: 100% !important;
    padding-left: 2.5rem !important;
    padding-right: 2.5rem !important;
}

/* ── Font: every Streamlit element ─────────────────────────── */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stVerticalBlock"],
[data-testid="stMarkdownContainer"],
[data-testid="stMarkdownContainer"] *,
[data-testid="metric-container"] *,
[data-testid="stDataFrame"] *,
[data-baseweb="select"] *, [data-baseweb="input"] *,
.stTabs *, .stMultiSelect *,
div[class*="css"], p, span, label, li, button, input, textarea {
    font-family: 'Source Sans 3', sans-serif !important;
    letter-spacing: 0.01em;
}

/* ── Title: very high specificity chain ─────────────────────── */
[data-testid="stAppViewContainer"] h1,
[data-testid="stMain"] h1,
[data-testid="stVerticalBlock"] h1,
[data-testid="stMarkdownContainer"] h1,
[data-testid="stHeadingWithActionElements"] h1,
.main h1, body h1, h1 {
    font-family: 'Source Sans 3', sans-serif !important;
    font-size: 45px !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
    line-height: 1.1 !important;
    margin-bottom: 6px !important;
}

/* ── Sub-headings ────────────────────────────────────────────── */
[data-testid="stAppViewContainer"] h2,
[data-testid="stMain"] h2,
[data-testid="stMarkdownContainer"] h2,
.main h2, h2 {
    font-family: 'Source Sans 3', sans-serif !important;
    font-size: 22px !important;
    font-weight: 700 !important;
    letter-spacing: -0.01em !important;
}

[data-testid="stAppViewContainer"] h3,
[data-testid="stMain"] h3,
[data-testid="stMarkdownContainer"] h3,
.main h3, h3 {
    font-family: 'Source Sans 3', sans-serif !important;
    font-size: 18px !important;
    font-weight: 600 !important;
}

p, span, label,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span {
    font-size: 14px !important;
    line-height: 1.6;
}

/* ── Dark mode ───────────────────────────────────────────────── */
@media (prefers-color-scheme: dark) {
    html, body,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"] {
        background: #0d0f16 !important;
        color: #e6e3db !important;
    }
    h1, h2, h3, h4, p, span, label,
    [data-testid="stMarkdownContainer"] * {
        color: #e6e3db !important;
    }
    [data-testid="metric-container"] {
        background: #171923 !important;
        border: 1px solid #22263a !important;
    }
    [data-testid="metric-container"] * { color: #e6e3db !important; }
    [data-baseweb="input"] > div,
    [data-baseweb="select"] > div,
    input, textarea {
        background: #171923 !important;
        border-color: #2a2e42 !important;
        color: #e6e3db !important;
    }
}

/* ── Light mode ──────────────────────────────────────────────── */
@media (prefers-color-scheme: light) {
    html, body { background: #f7f7fa; }
    [data-testid="metric-container"] {
        background: #ffffff;
        border: 1px solid #e0e0ea;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }
}

/* ── Metric cards ─────────────────────────────────────────────── */
[data-testid="metric-container"] {
    border-radius: 16px;
    padding: 28px 30px !important;
    transition: box-shadow 0.2s, transform 0.15s;
}
[data-testid="metric-container"]:hover {
    box-shadow: 0 10px 32px rgba(0,0,0,0.13);
    transform: translateY(-2px);
}
[data-testid="stMetricValue"] {
    font-size: 2.6rem !important;
    font-weight: 700 !important;
    line-height: 1.05 !important;
    margin-top: 8px !important;
}
[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 0.11em;
    opacity: 0.45;
}

/* ── Tabs ─────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1.5px solid rgba(128,128,128,0.16);
    margin-bottom: 24px;
}
.stTabs [data-baseweb="tab"] {
    font-size: 11px !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 10px 24px !important;
    border-radius: 0;
    color: rgba(128,128,128,0.55) !important;
    border-bottom: 2.5px solid transparent;
    margin-bottom: -1.5px;
}
.stTabs [aria-selected="true"] {
    color: #2563eb !important;
    border-bottom: 2.5px solid #2563eb !important;
    background: transparent !important;
}

/* ── Info bar ─────────────────────────────────────────────────── */
.info-bar {
    padding: 14px 18px;
    border-radius: 10px;
    font-size: 14px !important;
    line-height: 1.7;
    margin-bottom: 22px;
}
@media (prefers-color-scheme: light) {
    .info-bar { background: #eff6ff; border: 1px solid #bfdbfe; color: #1e3a8a; }
}
@media (prefers-color-scheme: dark) {
    .info-bar { background: #151e35; border: 1px solid #2563eb55; color: #93c5fd; }
}

/* ── Metrics section label ───────────────────────────────────── */
.metrics-header {
    font-size: 12px !important;
    font-weight: 700 !important;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    opacity: 0.45;
    display: block;
    margin-bottom: 14px;
    margin-top: 4px;
}

/* ── Cost chip ────────────────────────────────────────────────── */
.cost-chip {
    display: inline-block;
    border-radius: 20px;
    padding: 3px 12px;
    font-size: 12px !important;
    font-weight: 600;
    margin-top: 6px;
}
@media (prefers-color-scheme: light) {
    .cost-chip { background: #f0fdf4; border: 1px solid #bbf7d0; color: #14532d; }
}
@media (prefers-color-scheme: dark) {
    .cost-chip { background: #0f2018; border: 1px solid #16a34a55; color: #86efac; }
}

/* ── Primary buttons — calm blue ─────────────────────────────── */
[data-testid="stBaseButton-primary"] > button,
button[data-testid="baseButton-primary"],
.stButton button[kind="primary"] {
    background: #2563eb !important;
    border-color: #2563eb !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
}
[data-testid="stBaseButton-primary"] > button:hover,
button[data-testid="baseButton-primary"]:hover {
    background: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
}

hr { border: none; border-top: 1px solid rgba(128,128,128,0.13); margin: 20px 0; }

[data-testid="stDownloadButton"] button {
    font-size: 13px !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
    padding: 7px 18px !important;
}
</style>

<script>
// Streamlit's React re-renders headings after any <style> tag we inject,
// overriding font-size. Fix: watch the DOM with a MutationObserver and
// apply inline styles directly — inline !important beats all stylesheets.
(function() {
  function applyHeadingStyles() {
    document.querySelectorAll('h1').forEach(el => {
      el.style.setProperty('font-size',      '45px',                           'important');
      el.style.setProperty('font-weight',    '700',                            'important');
      el.style.setProperty('font-family',    "'Source Sans 3', sans-serif",    'important');
      el.style.setProperty('letter-spacing', '-0.02em',                        'important');
      el.style.setProperty('line-height',    '1.1',                            'important');
    });
  }
  // Run immediately, then again after Streamlit's first render pass
  applyHeadingStyles();
  setTimeout(applyHeadingStyles, 300);
  setTimeout(applyHeadingStyles, 1000);
  // Keep watching — Streamlit can re-render on interaction
  new MutationObserver(applyHeadingStyles)
    .observe(document.documentElement, { childList: true, subtree: true });
})();
</script>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────

for k, v in [
    ("places_df", None), ("enriched_df", None),
    ("api_key", ""), ("listing_filter", []),
    ("chain_search", ""),
]:
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────

st.title("London Cafe Industry Mapping")
st.markdown(
    '<div class="info-bar">'
    '<strong>Summary:</strong> This tool maps every cafe chain across London, '
    'traces each brand to its parent company, and enriches the data with live '
    'stock prices and market cap figures — sourced from Google Places, Wikipedia, '
    'and Yahoo Finance. Private company valuations are estimated from Wikipedia '
    'financial disclosures. The ownership tree visualises how cafe brands relate '
    'to their listed or private parent groups.'
    '</div>',
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────
# GET STARTED
# ─────────────────────────────────────────────────────────────────

st.markdown("### Get Started")

c1, c2, c3 = st.columns([4, 1.4, 1.4])

with c1:
    api_key = st.text_input(
        "Google Places API Key",
        value=st.session_state["api_key"],
        type="password",
        placeholder="Paste your API key here — or leave blank to use demo data",
        help="Free key at console.cloud.google.com ($200/month free tier).",
    )
    st.session_state["api_key"] = api_key
    if api_key:
        est = estimate_cost()
        st.markdown(
            f'<span class="cost-chip">Estimated cost: ~${est["cost_usd"]:.2f} '
            f'· {est["free_pct"]}% of $200 free tier '
            f'· ~{est["api_calls"]} API calls</span>',
            unsafe_allow_html=True,
        )

with c2:
    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    if api_key:
        fetch_btn = st.button("Load Data", use_container_width=True, type="primary")
    else:
        fetch_btn = False
        if st.button("Load Demo Data", use_container_width=True,
                     type="primary", key="demo_inline"):
            st.session_state["places_df"]   = demo_places()
            st.session_state["enriched_df"] = None
            st.rerun()

with c3:
    st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
    if st.button("Clear Cache", use_container_width=True):
        clear_places_cache()
        st.session_state.update(
            places_df=None, enriched_df=None
        )
        st.session_state.pop("ticker_cache", None)
        st.success("Cache cleared.")

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# FETCH LOGIC
# ─────────────────────────────────────────────────────────────────

if fetch_btn and api_key:
    existing = load_cached_places()
    if existing is not None and "refetch_confirmed" not in st.session_state:
        cost = estimate_cost()
        st.warning(
            f"**Cache Already Exists** — {len(existing)} cafes saved locally.\n\n"
            f"Re-fetching will spend another **~${cost['cost_usd']:.2f}** "
            f"({cost['free_pct']}% of your $200 free tier)."
        )
        cy, cn = st.columns(2)
        with cy:
            if st.button("Re-fetch and Update", key="confirm_refetch",
                         use_container_width=True, type="primary"):
                st.session_state["refetch_confirmed"] = True
                st.rerun()
        with cn:
            if st.button("Keep Existing Cache", key="cancel_refetch",
                         use_container_width=True):
                st.session_state.update(places_df=existing, enriched_df=None)
                st.info(f"Using cached data — {len(existing)} cafes.")
                st.rerun()
        st.stop()

    st.session_state.pop("refetch_confirmed", None)
    pb   = st.progress(0.0)
    stat = st.empty()

    def _pp(i: int, total: int, found: int) -> None:
        pb.progress(i / total)
        stat.caption(f"Grid {i}/{total} · {found} cafes found")

    with st.spinner("Fetching cafes from Google Places..."):
        st.session_state["places_df"] = fetch_places(
            api_key, use_cache=True, progress_cb=_pp
        )
        st.session_state["enriched_df"] = None

    pb.empty()
    stat.empty()
    st.success(f"Done — {len(st.session_state['places_df'])} cafes fetched.")

# ─────────────────────────────────────────────────────────────────
# DATA RESOLUTION
# ─────────────────────────────────────────────────────────────────

if st.session_state["places_df"] is None:
    disk = load_cached_places()
    if disk is not None:
        st.session_state["places_df"] = disk
        st.info(f"Loaded {len(disk)} cafes from local cache.")

if st.session_state["places_df"] is None:
    st.markdown(
        "Enter your Google Places API key above and click **Load Data**, "
        "or click **Load Demo Data** to explore with sample data."
    )
    st.stop()

# ─────────────────────────────────────────────────────────────────
# ENRICHMENT
# ─────────────────────────────────────────────────────────────────

if st.session_state["enriched_df"] is None:
    pb2  = st.progress(0.0)
    stat2 = st.empty()

    def _ep(step: str, pct: float) -> None:
        pb2.progress(pct)
        stat2.caption(step)

    with st.spinner("Enriching data..."):
        st.session_state["enriched_df"] = build_enriched_df(
            st.session_state["places_df"], progress_cb=_ep
        )
    pb2.empty()
    stat2.empty()

# Rename Independent → Private Listing using chaining
raw_df = (
    st.session_state["enriched_df"]
    .copy()
    .assign(parent_company=lambda d: d["parent_company"].replace(
        "Independent", "Private Listing"
    ))
)

# ─────────────────────────────────────────────────────────────────
# BUILD BRAND-LEVEL TABLE
# Collapse multiple cafe locations of the same brand into one row.
# One row per unique brand. Subsidiaries stay as individual rows.
# Uses pandas chaining throughout.
# ─────────────────────────────────────────────────────────────────

def build_brand_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a brand-level DataFrame:
      - Chain cafes: grouped by `brand`, one row per brand.
        Aggregates rating (mean), review_count (sum), location_count (count).
      - Subsidiaries: kept as individual rows.
    """
    financial_cols = [
        "parent_company", "ticker", "exchange", "price_usd",
        "market_cap_bn", "display_mktcap_bn", "pe_ratio",
        "est_private_val_bn", "sector", "industry",
        "is_listed", "display_label",
    ]

    # ── Subsidiaries: keep as-is ──────────────────────────────────
    subs = (
        df
        .loc[df["business_status"] == "SUBSIDIARY"]
        .copy()
        .assign(location_count=1)
        .reset_index(drop=True)
    )

    # ── Chain cafes: group by brand ───────────────────────────────
    cafes = df.loc[
        (df["business_status"] != "SUBSIDIARY") & df["brand"].notna()
    ].copy()

    if cafes.empty:
        return subs

    # Build agg dict only from columns that exist in the frame
    existing_financial = [c for c in financial_cols if c in cafes.columns]

    agg_spec = {c: "first" for c in existing_financial}
    if "rating"       in cafes.columns: agg_spec["rating"]       = "mean"
    if "review_count" in cafes.columns: agg_spec["review_count"] = "sum"

    grouped = (
        cafes
        .groupby("brand", as_index=False)
        .agg(agg_spec)
        .rename(columns={"brand": "name"})
        .assign(
            business_status="CHAIN",
            location_count=cafes.groupby("brand")["brand"].transform("count")
                                .drop_duplicates()
                                .reindex(
                                    cafes.groupby("brand", as_index=False)["brand"].first().index
                                )
                                .values,
            rating=lambda d: d["rating"].round(1) if "rating" in d.columns else None,
        )
        .reset_index(drop=True)
    )

    # location_count via a separate clean count series, merge in
    location_counts = (
        cafes
        .groupby("brand", as_index=False)
        .size()
        .rename(columns={"brand": "name", "size": "location_count"})
    )
    grouped = (
        grouped
        .drop(columns=["location_count"], errors="ignore")
        .merge(location_counts, on="name", how="left")
    )

    return (
        pd.concat([grouped, subs], ignore_index=True)
        .reset_index(drop=True)
    )


brand_df = build_brand_table(raw_df)

# ─────────────────────────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────────────────────────

listed_df  = raw_df.loc[raw_df["is_listed"] == True]
private_df = raw_df.loc[raw_df["ticker"] == "PRIVATE"]

st.markdown(
    '<span class="metrics-header">Cafe Industry Listing Summary</span>',
    unsafe_allow_html=True,
)

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total Locations",
          int(raw_df.loc[raw_df["business_status"] != "SUBSIDIARY"].shape[0]))
m2.metric("Unique Brands",   int(raw_df["brand"].nunique()))
m3.metric("Listed Parents",  int(listed_df["ticker"].nunique()))
m4.metric("Private Listings",int(private_df["parent_company"].nunique()))
m5.metric("Subsidiaries",    int(raw_df.loc[raw_df["business_status"] == "SUBSIDIARY"].shape[0]))

st.markdown("---")

# ─────────────────────────────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────────────────────────────

f1, f2, f3 = st.columns([2, 2, 3])
with f1:
    brand_filter = st.selectbox(
        "Brand",
        ["All"] + sorted(brand_df["name"].dropna().unique().tolist()),
    )
with f2:
    parent_filter = st.selectbox(
        "Parent Company",
        ["All"] + sorted(brand_df["parent_company"].dropna().unique().tolist()),
    )
with f3:
    listing_filter = st.multiselect(
        "Listing Type",
        options=["Listed", "Private Listing"],
        default=st.session_state["listing_filter"],
        placeholder="All listing types",
    )
    st.session_state["listing_filter"] = listing_filter

chain_search = st.text_input(
    "Search brands, parent companies or tickers",
    placeholder="e.g. Blank Street, KO, Coca-Cola",
)

# Apply filters using chaining
view = brand_df.copy()

if listing_filter:
    mask = pd.Series(False, index=view.index)
    if "Listed"          in listing_filter: mask |= (view["is_listed"] == True)
    if "Private Listing" in listing_filter: mask |= (view["ticker"] == "PRIVATE")
    view = view.loc[mask]

if brand_filter != "All":
    view = view.loc[view["name"] == brand_filter]

if parent_filter != "All":
    view = view.loc[view["parent_company"] == parent_filter]

if chain_search:
    view = view.loc[
        view["name"].str.contains(chain_search, case=False, na=False)
        | view["parent_company"].str.contains(chain_search, case=False, na=False)
        | view["ticker"].str.contains(chain_search, case=False, na=False)
    ]

# ─────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["Table", "Visual Mapping"])

# ── TABLE ─────────────────────────────────────────────────────────
with tab1:
    st.caption(f"Showing {len(view)} brands")

    show_cols = [c for c in [
        "name", "parent_company", "ticker", "exchange",
        "price_usd", "display_mktcap_bn", "pe_ratio",
        "est_private_val_bn", "sector", "industry",
        "rating", "review_count", "location_count",
    ] if c in view.columns]

    st.dataframe(
        view[show_cols].rename(columns={
            "name":               "Brand",
            "parent_company":     "Parent Company",
            "ticker":             "Ticker",
            "exchange":           "Exchange",
            "price_usd":          "Price (USD)",
            "display_mktcap_bn":  "Mkt Cap ($bn)",
            "pe_ratio":           "P/E",
            "est_private_val_bn": "Est. Private Val ($bn)",
            "sector":             "Sector",
            "industry":           "Industry",
            "rating":             "Avg Rating",
            "review_count":       "Total Reviews",
            "location_count":     "Locations",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Price (USD)":            st.column_config.NumberColumn(format="$%.2f"),
            "Mkt Cap ($bn)":          st.column_config.NumberColumn(format="$%.1fbn"),
            "Est. Private Val ($bn)": st.column_config.NumberColumn(format="~$%.1fbn"),
            "P/E":                    st.column_config.NumberColumn(format="%.1fx"),
            "Avg Rating":             st.column_config.NumberColumn(format="%.1f ⭐"),
            "Total Reviews":          st.column_config.NumberColumn(format="%d"),
            "Locations":              st.column_config.NumberColumn(format="%d"),
        },
    )
    st.download_button(
        "Download CSV",
        view.to_csv(index=False).encode("utf-8"),
        "london_cafes.csv",
        "text/csv",
    )

# ── VISUAL MAPPING ────────────────────────────────────────────────
with tab2:
    st.markdown(
        '<div class="info-bar">'
        '<strong>Summary:</strong> Each branch represents a parent company. '
        'Multiple locations of the same brand collapse into one node. '
        '<b style="text-decoration:underline;text-decoration-color:#2563eb">Underlined bold</b> nodes are actual London cafe chains; '
        'underlined nodes in other colours are corporate subsidiaries. '
        'Use +/− or scroll to zoom, drag to pan, click to expand or collapse.'
        '</div>',
        unsafe_allow_html=True,
    )

    # Build deduplicated brand tree for the map.
    # Only include rows with a recognised chain brand — independent cafes
    # (brand=NaN) have no parent and would otherwise create a spurious
    # "Private Listing" node in the tree.
    brand_groups: dict[str, dict] = {}
    for _, row in raw_df.iterrows():
        raw_brand = row.get("brand")
        if not raw_brand or str(raw_brand) in ("nan", "None", ""):
            continue
        parent    = str(row.get("parent_company") or "")
        ticker    = str(row.get("ticker") or "")
        price     = row.get("price_usd")
        is_listed = bool(row.get("is_listed", False))
        if is_listed and ticker and ticker not in ("PRIVATE", "nan", "") and price is not None:
            label = f"{parent} ({ticker} ${float(price):.2f})"
        elif parent and parent not in ("Independent", "Private Listing", "nan", ""):
            label = f"{parent} (PRIVATE)"
        else:
            continue   # skip anything still unresolved
        brand   = str(raw_brand)
        mkt_cap = float(row.get("display_mktcap_bn") or row.get("market_cap_bn") or 0)
        is_sub  = str(row.get("business_status", "")) == "SUBSIDIARY"
        if label not in brand_groups:
            brand_groups[label] = {}
        if brand not in brand_groups[label] or mkt_cap > brand_groups[label][brand]["mkt_cap"]:
            brand_groups[label][brand] = {"mkt_cap": mkt_cap, "is_sub": is_sub}

    # Deduplication: if a brand is an explicit SUBSIDIARY under one parent
    # (e.g. Starbucks under SSP Group per Wikipedia), remove it from any other
    # parent group where it appears as a regular chain brand.
    # This means the operating relationship (Wikipedia) wins over the default
    # brand→parent mapping — no hardcoding required.
    _sub_brands = {
        brand
        for brands in brand_groups.values()
        for brand, info in brands.items()
        if info["is_sub"]
    }
    for _lbl in list(brand_groups.keys()):
        for _brand in list(brand_groups[_lbl].keys()):
            if _brand in _sub_brands and not brand_groups[_lbl][_brand]["is_sub"]:
                del brand_groups[_lbl][_brand]
        if not brand_groups[_lbl]:
            del brand_groups[_lbl]

    def _psort(lbl: str) -> tuple:
        if "Private Listing" in lbl: return (2, lbl)   # catch-all independents last
        if "(PRIVATE)" in lbl:       return (1, lbl)   # known private companies
        return (0, lbl)                                 # listed (have ticker)

    sorted_parents = sorted(brand_groups.keys(), key=_psort)
    HUES = [210, 158, 28, 286, 338, 176, 56, 258, 14, 128, 50, 194]
    parent_hue = {p: HUES[i % len(HUES)] for i, p in enumerate(sorted_parents)}

    import re as _re
    _corp_re = _re.compile(
        r"\(.*?\)"                                           # strip ticker/price
        r"|\b(plc|ltd|llc|inc|corp|corporation|group|"
        r"holding|company|co|s\.a\.|n\.v\.)\b"
        r"|[^a-z0-9 ]",
        _re.IGNORECASE,
    )
    def _clean(s: str) -> str:
        return _re.sub(r"\s+", " ", _corp_re.sub(" ", s)).strip().lower()

    def _is_self_parent(brand: str, parent_label: str) -> bool:
        """True when brand and parent are the same entity (e.g. Greggs / Greggs plc)."""
        cb, cp = _clean(brand), _clean(parent_label)
        return bool(cb and cp and (cb in cp or cp in cb))

    md_lines = ["# London Cafes"]
    for pl in sorted_parents:
        brands    = brand_groups[pl]
        # Keep $ — md_escaped replaces $ with \$ so markmap renders it as a literal $
        safe_p    = pl.replace("|", "-").replace("`", "'")
        solo_same = (
            len(brands) == 1
            and _is_self_parent(next(iter(brands)), pl)
        )
        if solo_same:
            # Show brand name but carry over the (TICKER $XX.XX) / (PRIVATE) note
            paren_m = _re.search(r'\([^)]+\)\s*$', pl)
            paren   = f" {paren_m.group(0)}" if paren_m else ""
            safe_b  = next(iter(brands)).replace("|", "-").replace("`", "'")
            md_lines.append(f"## {safe_b}{paren}")
        else:
            md_lines.append(f"## {safe_p}")
            for bn in sorted(brands.keys(), key=lambda x: -brands[x]["mkt_cap"]):
                safe_b = bn.replace("|", "-").replace("`", "'")
                md_lines.append(f"### {safe_b}")
    markmap_md = "\n".join(md_lines)

    colour_map_json = json.dumps({p: parent_hue[p] for p in sorted_parents})

    # cafe_brands: actual London cafe chains (not Wikipedia subsidiaries)
    # loc_count:   how many locations each cafe brand has (for edge width)
    cafe_brands_list = [
        brand
        for brands in brand_groups.values()
        for brand, info in brands.items()
        if not info["is_sub"]
    ]
    loc_count_map: dict[str, int] = {}
    for _, _row in raw_df.iterrows():
        _b = str(_row.get("brand") or "")
        if _b and _b not in ("nan", "None", ""):
            loc_count_map[_b] = loc_count_map.get(_b, 0) + 1
    cafe_brands_json = json.dumps(cafe_brands_list)
    loc_count_json   = json.dumps(loc_count_map)

    # sub_parent_map: {subsidiary_brand: parent_label} for Wikipedia subsidiaries
    sub_parent_map = {
        bn: pl
        for pl, brands in brand_groups.items()
        for bn, info in brands.items()
        if info["is_sub"]
    }
    sub_parent_json = json.dumps(sub_parent_map)

    # cafe_parent_map: {cafe_brand: parent_label} for actual London cafes
    cafe_parent_map = {
        bn: pl
        for pl, brands in brand_groups.items()
        for bn, info in brands.items()
        if not info["is_sub"]
    }
    cafe_parent_json = json.dumps(cafe_parent_map)

    md_escaped = (
        markmap_md
        .replace("\\", "\\\\")
        .replace("`", "\\`")
        .replace("$", "\\$")
    )

    HEIGHT = 820

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;600;700&display=swap" rel="stylesheet">
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
html, body {{ width: 100%; height: {HEIGHT}px; overflow: hidden; font-family: 'Source Sans 3', sans-serif; background: transparent; }}
#wrap {{ position: relative; width: 100%; height: {HEIGHT}px; }}
.markmap {{ width: 100%; height: {HEIGHT}px; }}
.markmap > svg {{ width: 100%; height: {HEIGHT}px; touch-action: none; cursor: grab; }}
#zctrl {{ position: absolute; top: 12px; right: 14px; display: flex; gap: 5px; z-index: 99; }}
.zb {{
  width: 30px; height: 30px; border-radius: 7px;
  font-size: 16px; font-weight: 700; border: 1px solid;
  display: flex; align-items: center; justify-content: center;
  cursor: pointer; user-select: none; transition: opacity .15s;
}}
.zb#zfit {{ font-size: 10px; width: 36px; }}
.zb:hover {{ opacity: 0.6; }}
</style>
</head>
<body>
<div id="wrap">
  <div class="markmap">
    <script type="text/template">{md_escaped}</script>
  </div>
  <div id="zctrl">
    <div class="zb" id="zi">+</div>
    <div class="zb" id="zfit">Fit</div>
    <div class="zb" id="zo">−</div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/markmap-autoloader@latest"></script>
<script>
const dark        = window.matchMedia('(prefers-color-scheme: dark)').matches;
const colMap      = {colour_map_json};
const subParent   = {sub_parent_json};   // {{subBrand: parentLabel}}
const cafeParent  = {cafe_parent_json};  // {{cafeBrand: parentLabel}}
const cafeSet     = new Set({cafe_brands_json});
const text        = dark ? '#e6e3db' : '#111111';
const CAFE_BLUE   = dark ? '#60a5fa' : '#2563eb';

document.body.style.background = dark ? '#0d0f16' : '#f7f7fa';

const zbBg = dark ? 'rgba(20,22,32,0.92)' : 'rgba(255,255,255,0.94)';
const zbBd = dark ? '#2e3248'             : '#d4d4de';
const zbTx = dark ? '#c8c5bc'             : '#222';
document.querySelectorAll('.zb').forEach(b => {{
  b.style.background = zbBg; b.style.borderColor = zbBd; b.style.color = zbTx;
}});

function hsl(h,s,l) {{ return `hsl(${{h}},${{s}}%,${{l}}%)`; }}

const allParents = new Set(Object.keys(colMap));

// ── Node text styling ─────────────────────────────────────────────
// Visual distinction lives on the text underline, not the edges:
//   Cafe chains  → blue bold underline
//   Subsidiaries → parent-hue coloured underline
//   Parent nodes → no underline, semi-bold
function styleNodes(svg) {{
  svg.querySelectorAll('g.markmap-node').forEach(g => {{
    const el = g.querySelector('foreignObject div');
    if (!el) return;
    const label = (el.textContent || '').trim();

    el.style.color      = text;
    el.style.fontFamily = "'Source Sans 3', sans-serif";

    // Inject fin-note span for (TICKER $XX) / (PRIVATE) — only once
    if (!el.querySelector('.fin-note')) {{
      el.innerHTML = el.innerHTML.replace(
        /(\\([^)]+\\))/g,
        '<span class="fin-note" style="font-size:0.7em;opacity:0.52;font-weight:400;letter-spacing:0">$1</span>'
      );
    }}

    el.style.textUnderlineOffset = '3px';
    if (cafeSet.has(label)) {{
      // Actual London cafe — blue bold underline
      el.style.fontWeight             = '700';
      el.style.textDecoration         = 'underline';
      el.style.textDecorationColor    = CAFE_BLUE;
      el.style.textDecorationThickness = '2px';
    }} else if (subParent[label] !== undefined) {{
      // Wikipedia subsidiary — parent-hue coloured underline
      const hue = colMap[subParent[label]] ?? 210;
      el.style.fontWeight             = '400';
      el.style.textDecoration         = 'underline';
      el.style.textDecorationColor    = hsl(hue, 62, dark ? 58 : 44);
      el.style.textDecorationThickness = '1.5px';
    }} else {{
      // Parent node
      el.style.fontWeight    = '600';
      el.style.textDecoration = 'none';
    }}
  }});
}}

// ── Edge styling — neutral, tinted by parent hue ─────────────────
function styleEdges(svg, nodeIdx) {{
  svg.querySelectorAll('path.markmap-link').forEach(path => {{
    const d    = path.getAttribute('d') || '';
    const nums = d.match(/-?[\\d.]+/g);
    if (!nums || nums.length < 4) return;
    const tx = parseFloat(nums[nums.length-2]);
    const ty = parseFloat(nums[nums.length-1]);
    let nearest = null, minD = Infinity;
    nodeIdx.forEach(n => {{ const d2=Math.hypot(n.x-tx,n.y-ty); if(d2<minD){{minD=d2;nearest=n;}} }});
    if (!nearest || minD > 80) return;
    const label = nearest.label;

    // Determine which parent this node belongs to
    const pLbl = cafeParent[label] || subParent[label] || (allParents.has(label) ? label : null);
    const hue  = pLbl ? (colMap[pLbl] ?? 210) : 210;
    let stroke, width;
    if (allParents.has(label)) {{
      stroke = hsl(hue, dark?38:28, dark?34:80); width = '2.0';
    }} else {{
      stroke = hsl(hue, dark?32:22, dark?30:84); width = '1.4';
    }}
    path.setAttribute('stroke',        stroke);
    path.setAttribute('stroke-width',  width);
    path.setAttribute('fill',          'none');
    path.setAttribute('stroke-linecap','round');
    path.setAttribute('opacity',       '0.72');
  }});
}}

// ── Legend — right-middle of map ──────────────────────────────────
function buildLegend() {{
  let leg = document.getElementById('mmlegend');
  if (!leg) {{
    leg = document.createElement('div');
    leg.id = 'mmlegend';
    document.getElementById('wrap').appendChild(leg);
  }}
  const bg  = dark ? 'rgba(13,15,22,0.90)'    : 'rgba(255,255,255,0.93)';
  const bd  = dark ? '#2a2e42'                 : '#e0e0ee';
  const col = dark ? '#e6e3db'                 : '#111';
  const dim = dark ? 'rgba(255,255,255,0.32)'  : 'rgba(0,0,0,0.36)';
  const sep = dark ? 'rgba(255,255,255,0.07)'  : 'rgba(0,0,0,0.07)';

  leg.style.cssText = `
    position:absolute; right:18px; top:50%; transform:translateY(-50%);
    background:${{bg}}; border:1px solid ${{bd}}; border-radius:12px;
    padding:14px 16px; z-index:99; backdrop-filter:blur(10px);
    font-family:'Source Sans 3',sans-serif; min-width:170px; max-width:215px;
  `;

  let html = `<div style="font-size:10px;font-weight:700;text-transform:uppercase;
    letter-spacing:.10em;color:${{dim}};margin-bottom:10px">Legend</div>`;

  // Cafe chain row
  html += `<div style="display:flex;align-items:center;gap:9px;margin-bottom:9px;
    padding-bottom:9px;border-bottom:1px solid ${{sep}}">
    <span style="font-size:13px;color:${{CAFE_BLUE}}">━</span>
    <span style="font-weight:700;color:${{col}};font-size:11px;
      text-decoration:underline;text-decoration-color:${{CAFE_BLUE}};
      text-underline-offset:2px;text-decoration-thickness:2px">London Cafe</span>
  </div>`;

  // One row per parent company
  for (const [lbl, hue] of Object.entries(colMap)) {{
    let name = lbl.replace(/\\s*\\([^)]+\\)\\s*$/, '').trim();
    if (name.length > 27) name = name.substring(0, 25) + '…';
    const c = hsl(hue, 62, dark ? 58 : 44);
    html += `<div style="display:flex;align-items:center;gap:9px;margin-bottom:6px">
      <span style="font-size:13px;color:${{c}}">━</span>
      <span style="color:${{col}};font-size:11px;
        text-decoration:underline;text-decoration-color:${{c}};
        text-underline-offset:2px;text-decoration-thickness:1.5px">${{name}}</span>
    </div>`;
  }}
  leg.innerHTML = html;
}}

function recolour() {{
  const svg = document.querySelector('.markmap svg');
  if (!svg) return;

  svg.querySelectorAll('circle').forEach(c => {{
    c.setAttribute('fill',         dark ? '#151823' : '#ffffff');
    c.setAttribute('stroke',       dark ? '#252a3c' : '#e2e2ea');
    c.setAttribute('stroke-width', '1.5');
  }});

  styleNodes(svg);

  const nodeIdx = Array.from(svg.querySelectorAll('g.markmap-node')).map(g => {{
    const t = g.getAttribute('transform') || '';
    const m = t.match(/translate\\((-?[\\d.]+),\\s*(-?[\\d.]+)\\)/);
    const lbl = (g.querySelector('foreignObject div') || {{}}).textContent || '';
    return m ? {{ x: parseFloat(m[1]), y: parseFloat(m[2]), label: lbl.trim() }} : null;
  }}).filter(Boolean);

  styleEdges(svg, nodeIdx);
  buildLegend();
}}

function waitForMarkmap(n) {{
  const svg  = document.querySelector('.markmap svg');
  const link = svg && svg.querySelector('path.markmap-link');
  if (svg && link) {{
    recolour();
    const g = svg.querySelector('g');

    // Zoom with an 'on zoom' handler — this is what was missing.
    // Without it d3 tracks the transform internally but never moves the viewport,
    // so dragging and scrolling appeared to do nothing.
    const zb = d3.zoom()
      .scaleExtent([0.05, 8])
      .on('zoom', ev => {{ if (g) g.setAttribute('transform', ev.transform); }});

    d3.select(svg).call(zb);

    // Grabbing cursor feedback
    d3.select(svg)
      .on('mousedown.cur', () => {{ svg.style.cursor = 'grabbing'; }})
      .on('mouseup.cur',   () => {{ svg.style.cursor = 'grab';     }});

    function doFit(retries) {{
      // window.innerWidth is always valid inside an iframe; svg.clientWidth
      // often returns 0 until the browser has completed its first layout pass.
      const W = window.innerWidth  || {HEIGHT};
      const H = window.innerHeight || {HEIGHT};
      const b = g ? g.getBBox() : null;
      if (!b || !b.width || !b.height || W < 50) {{
        if ((retries || 0) > 0) setTimeout(() => doFit(retries - 1), 300);
        return;
      }}
      const sc = Math.min(0.90, (W - 60) / b.width, (H - 60) / b.height);
      const tx = (W - b.width  * sc) / 2 - b.x * sc;
      const ty = (H - b.height * sc) / 2 - b.y * sc;
      d3.select(svg).transition().duration(400)
        .call(zb.transform, d3.zoomIdentity.translate(tx, ty).scale(sc));
    }}

    // First attempt after markmap's initial layout, then retry in case it's still animating
    setTimeout(() => doFit(4), 350);

    document.getElementById('zi').onclick   = () => d3.select(svg).transition().duration(260).call(zb.scaleBy, 1.35);
    document.getElementById('zo').onclick   = () => d3.select(svg).transition().duration(260).call(zb.scaleBy, 0.74);
    document.getElementById('zfit').onclick = () => doFit(0);

    new MutationObserver(() => setTimeout(recolour, 50))
      .observe(svg, {{ childList:true, subtree:true }});
  }} else if (n > 0) {{
    setTimeout(() => waitForMarkmap(n-1), 300);
  }}
}}

window.addEventListener('load', () => setTimeout(() => waitForMarkmap(25), 350));
setTimeout(() => waitForMarkmap(25), 500);
</script>
</body>
</html>"""

    components.html(html, height=HEIGHT + 10, scrolling=False)