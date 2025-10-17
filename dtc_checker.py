#!/usr/bin/env python3
"""
DTC Website Checker — CSV in → CSV out
- URL normalization, per-host pacing, smart retries
- Progress + checkpoints
- Playwright fallback for blocked/JS-heavy pages
- Marks TRUE/FALSE; if blocked after both passes → TRUE_FALSE = "BLOCKED"

Defaults (flags optional):
  INPUT_CSV   = "/path/to/file.csv"
  OUTPUT_CSV  = "path/to/folder/.csv"
  CONCURRENCY = 5
  TIMEOUT     = 20
  THRESHOLD   = 5
  CHECKPOINT_EVERY = 25
"""

import sys
import subprocess
import importlib

REQUIRED = ["httpx", "selectolax", "extruct", "w3lib", "tldextract", "playwright"]

def _ensure_deps():
    missing = []
    for pkg in REQUIRED:
        try:
            importlib.import_module(pkg)
        except Exception:
            missing.append(pkg)
    if missing:
        print(f"[setup] Installing missing packages: {', '.join(missing)}")
        cmd = [sys.executable, "-m", "pip", "install"] + missing
        subprocess.check_call(cmd)
        for pkg in missing:
            importlib.import_module(pkg)
    # ensure chromium is available for playwright
    try:
        # try import first
        from playwright.async_api import async_playwright  # noqa: F401
        # try to launch Chromium quickly; if not installed this will fail
        # and we'll install below
    except Exception:
        pass
    try:
        # attempt install chromium browser if missing
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                       check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        # ignore – user can run it manually if needed
        pass

_ensure_deps()

import argparse
import asyncio
import csv
import random
import time
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse
from collections import defaultdict

import httpx
from selectolax.parser import HTMLParser
from extruct.jsonld import JsonLdExtractor
from playwright.async_api import async_playwright

# -------------------- Defaults --------------------
INPUT_CSV_DEFAULT = "/path/to/file.csv"
OUTPUT_CSV_DEFAULT = "/path/to/file.csv"
CONCURRENCY_DEFAULT = 5
TIMEOUT_DEFAULT = 20
THRESHOLD_DEFAULT = 5
CHECKPOINT_EVERY = 25

BLOCK_STATUSES = {401, 403, 409, 429, 503, 504}

# -------------------- URL Helpers -----------------
def normalize_candidates(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parsed = urlparse(raw)
    host = raw if not parsed.scheme else raw.split("://", 1)[1]
    host = host.strip("/")
    first = raw if parsed.scheme else f"https://{host}"
    bare = host.replace("http://", "").replace("https://", "")
    bare_no_www = bare[4:] if bare.startswith("www.") else bare
    with_www = "www." + bare_no_www
    candidates: List[str] = []
    def add(u): 
        if u not in candidates: candidates.append(u)
    add(first)
    add(f"https://{with_www}")
    add(f"http://{bare_no_www}")
    add(f"http://{with_www}")
    return candidates

# -------------------- Fetcher (httpx) ---------------------
_UAS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

def _headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }

_MIN_GAP_SECONDS = 2.0  # per-host gap
_host_last_ts = defaultdict(lambda: 0.0)

async def _pace_host(url: str):
    host = urlparse(url).netloc or ""
    if not host:
        return
    now = time.time()
    gap = now - _host_last_ts[host]
    if gap < _MIN_GAP_SECONDS:
        await asyncio.sleep(_MIN_GAP_SECONDS - gap + random.uniform(0.05, 0.25))
    _host_last_ts[host] = time.time()

async def _fetch_once(client: httpx.AsyncClient, url: str, timeout: int):
    r = await client.get(url, follow_redirects=True, timeout=httpx.Timeout(timeout))
    return r.status_code, str(r.url), {k.lower(): v for k, v in r.headers.items()}, r.text or ""

async def fetch_url(raw_url: str, timeout: int = TIMEOUT_DEFAULT, retries: int = 3) -> Dict[str, Optional[str]]:
    """
    Try multiple normalized URL candidates; per-candidate:
    - per-host pacing
    - HTTP/2 client
    - backoff for 429/5xx; honor Retry-After
    """
    result = {"url": raw_url, "final_url": None, "status": None, "headers": None, "html": None, "error": None, "attempted": []}
    candidates = normalize_candidates(raw_url)
    if not candidates:
        result["error"] = "empty_url"
        return result

    async with httpx.AsyncClient(headers=_headers(), http2=True) as client:
        for cand in candidates:
            result["attempted"].append(cand)
            backoff = 0.75
            for attempt in range(retries + 1):
                try:
                    await _pace_host(cand)
                    status, final_url, headers, text = await _fetch_once(client, cand, timeout)
                    if text:
                        result.update({"status": status, "final_url": final_url, "headers": headers, "html": text})
                    if status and status < 400 and text:
                        return result
                    if status in (429, 502, 503, 504):
                        retry_after = headers.get("retry-after")
                        sleep_s = None
                        if retry_after:
                            try: sleep_s = float(retry_after)
                            except Exception: sleep_s = None
                        if sleep_s is None:
                            sleep_s = backoff + random.uniform(0.1, 0.4)
                            backoff = min(backoff * 2.0, 8.0)
                        await asyncio.sleep(sleep_s)
                        continue
                    break
                except Exception as e:
                    last_err = f"{type(e).__name__}: {e}"
                    result["error"] = last_err
                    if attempt < retries:
                        await asyncio.sleep(backoff + random.uniform(0.1, 0.4))
                        backoff = min(backoff * 2.0, 8.0)
                        continue
            # next candidate
    if not result.get("html"):
        result["error"] = result.get("error") or f"no_html_received_after_{len(candidates)}_candidates"
    return result

# -------------------- Playwright fallback --------------------
async def fetch_with_playwright(url: str, timeout_ms: int = 15000) -> Dict[str, Optional[str]]:
    """
    Render page with Chromium; return HTML + final URL (status not easily available).
    """
    out = {"final_url": None, "html": None, "error": None}
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=random.choice(_UAS), viewport={"width": 1366, "height": 768})
            page = await context.new_page()
            try:
                await page.route("**/*", lambda route: route.continue_())  # pass-through
            except Exception:
                pass
            await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            out["final_url"] = page.url
            out["html"] = await page.content()
            await context.close()
            await browser.close()
            return out
    except Exception as e:
        out["error"] = f"PlaywrightError: {e}"
        return out

# --------------- Parsing & Detectors ---------------
def extract_scripts_text_and_links(html: str) -> Tuple[List[str], str, List[str]]:
    scripts: List[str] = []
    text = ""
    links: List[str] = []
    try:
        tree = HTMLParser(html)
        for n in tree.css("script[src]"):
            src = n.attributes.get("src", "").strip()
            if src:
                scripts.append(src.lower())
        for n in tree.css("a[href]"):
            href = n.attributes.get("href", "").strip()
            if href:
                links.append(href.lower())
        chunks = []
        for sel in ["h1", "h2", "h3", "p", "a", "button", "li"]:
            for n in tree.css(sel):
                t = n.text().strip()
                if t:
                    chunks.append(t)
        text = " ".join(chunks).lower()
    except Exception:
        import re
        scripts = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.I)
        scripts = [s.lower() for s in scripts]
        links = re.findall(r'<a[^>]+href=["\']([^"\']+)["\']', html, flags=re.I)
        links = [l.lower() for l in links]
        text = html.lower()
    return scripts, text, links

def extract_jsonld_types(html: str) -> List[str]:
    types = []
    try:
        items = JsonLdExtractor().extract(html)
        for it in items:
            t = it.get("@type")
            if isinstance(t, list):
                for x in t:
                    types.append(str(x).lower())
            elif isinstance(t, str):
                types.append(t.lower())
    except Exception:
        pass
    return types

def detect_platform(headers: Dict[str, str], scripts: List[str], text: str, html_lower: str, links: List[str]) -> Tuple[str, str]:
    shopify_hits = 0
    for s in scripts:
        if "cdn.shopify.com" in s or "shopifycloud.com" in s: shopify_hits += 1
        if "shopify" in s and ("cart" in s or "theme" in s or "assets" in s): shopify_hits += 1
    for k in headers.keys():
        if k.lower().startswith("x-shopify"): shopify_hits += 1
    if "shop pay" in text or "powered by shopify" in text: shopify_hits += 1
    if 'name="shopify-digital-wallet"' in html_lower or "data-shopify" in html_lower or "window.shopify" in html_lower: shopify_hits += 1
    if any("/products/" in l or "/cart" in l for l in links): shopify_hits += 1
    if shopify_hits >= 2:
        return "Shopify", "shopify_signals"
    for s in scripts:
        if "woocommerce" in s or "wp-content" in s or "wp-includes" in s: return "WooCommerce", "woocommerce_signals"
    for s in scripts:
        if "bigcommerce" in s or "bc-sf-filter" in s: return "BigCommerce", "bigcommerce_signals"
    for s in scripts:
        if "mage/" in s or "magento" in s: return "Magento", "magento_signals"
    return "Other/Unknown", ""

def detect_dtc(text: str, jsonld_types: List[str], links: List[str]) -> Tuple[bool, str]:
    if "product" in jsonld_types: return True, "jsonld_product"
    kws = ["add to cart", "add to bag", "checkout", "shipping", "returns", "store locator", "stockists"]
    for kw in kws:
        if kw in text: return True, "commerce_keywords"
    if any("/products/" in l or "/cart" in l or "/collections/" in l for l in links): return True, "link_patterns"
    return False, ""

def detect_subscriptions(text: str, scripts: List[str], html_lower: str) -> Tuple[bool, str]:
    vendors = ["recharge", "rechargecdn", "getrecharge", "skio", "appstle", "bold-subscriptions", "smartrr", "ordergroove"]
    for s in scripts:
        for v in vendors:
            if v in s: return True, v
    if "subscribe & save" in text or "subscribe and save" in text or "subscription" in text: return True, "text_subscribe"
    if "data-selling-plan" in html_lower or "selling_plan" in html_lower: return True, "selling_plan"
    return False, ""

def detect_bnpl(text: str, scripts: List[str], html_lower: str) -> Tuple[bool, str]:
    vendors = ["klarna", "afterpay", "affirm", "sezzle", "zip", "shop pay installments", "shop pay", "paypal pay in 4"]
    for v in vendors:
        if v in text or v in html_lower: return True, v
    for s in scripts:
        for v in ["klarna", "afterpay", "affirm", "sezzle", "zip"]:
            if v in s: return True, v
    if "afterpay-product-widget" in html_lower or "klarna-placement" in html_lower: return True, "bnpl_widget"
    return False, ""

def detect_email_sms(text: str, scripts: List[str]) -> Tuple[bool, str]:
    vendors = ["klaviyo", "attn.tv", "attentive", "postscript", "smsbump", "omnisend", "mailchimp", "onesignal", "privy", "justuno"]
    for s in scripts:
        for v in vendors:
            if v in s: return True, v
    phrases = ["10% off your first order", "subscribe to our newsletter", "join our newsletter", "sms updates", "sign up and save"]
    for p in phrases:
        if p in text: return True, "text_capture"
    return False, ""

def detect_loyalty(text: str, scripts: List[str]) -> Tuple[bool, str]:
    vendors = ["yotpo", "loyaltylion", "smile.io", "stamped.io", "referralcandy", "friendbuy", "rise.ai"]
    for s in scripts:
        for v in vendors:
            if v in s: return True, v
    for p in ["rewards", "loyalty points", "refer a friend", "earn points"]:
        if p in text: return True, "text_loyalty"
    return False, ""

def detect_quiz(text: str, scripts: List[str]) -> Tuple[bool, str]:
    if "octane ai" in text or "octaneai" in text or "quiz kit" in text or "quizkit" in text: return True, "quiz_vendor"
    for p in ["take the quiz", "find your match", "build your routine", "find your fit"]:
        if p in text: return True, "text_quiz"
    return False, ""

def detect_omnichannel(text: str) -> Tuple[bool, str]:
    for p in ["store locator", "find a store", "stockists", "wholesale"]:
        if p in text: return True, p
    return False, ""

def detect_mission(text: str) -> Tuple[bool, str]:
    for p in ["sustainable", "recyclable", "clean", "vegan", "fair trade", "mission", "ethically", "organic", "b corp", "cruelty-free"]:
        if p in text: return True, p
    return False, ""

def detect_modern_ux(url: str, headers: Dict[str, str], text: str) -> Tuple[bool, str]:
    https = url.lower().startswith("https://")
    if https and ("shipping" in text or "returns" in text or "privacy policy" in text):
        return True, "https_and_policies"
    if https:
        return True, "https"
    return False, ""

# -------------------- CSV I/O ---------------------
def read_urls_from_csv(path: str) -> List[str]:
    urls = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("url") or "").strip()
            if url:
                urls.append(url)
    return urls

def write_results_to_csv(path: str, rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "url",
        "final_url",
        "http_status",
        "platform_guess",
        "score",
        "TRUE_FALSE",
        "matched_features",
        "bnpl_vendor",
        "subscription_vendor",
        "loyalty_vendor",
        "email_sms_vendor",
        "notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

# -------------------- Scoring ---------------------
def score_site(url: str, status: Optional[int], headers: Dict[str, str], html: str) -> Dict[str, str]:
    if not html:
        return {
            "platform_guess": "",
            "score": "0",
            "matched_features": "",
            "bnpl_vendor": "",
            "subscription_vendor": "",
            "loyalty_vendor": "",
            "email_sms_vendor": "",
            "notes": "empty_html",
        }

    html_lower = html.lower()
    scripts, text, links = extract_scripts_text_and_links(html)
    jsonld_types = extract_jsonld_types(html)

    platform_guess, _platform_reason = detect_platform(headers or {}, scripts, text, html_lower, links)
    dtc_ok, _dtc_reason = detect_dtc(text, jsonld_types, links)
    sub_ok, sub_vendor = detect_subscriptions(text, scripts, html_lower)
    bnpl_ok, bnpl_vendor = detect_bnpl(text, scripts, html_lower)
    email_ok, email_vendor = detect_email_sms(text, scripts)
    loyalty_ok, loyalty_vendor = detect_loyalty(text, scripts)
    quiz_ok, _ = detect_quiz(text, scripts)
    omni_ok, _ = detect_omnichannel(text)
    mission_ok, _ = detect_mission(text)
    ux_ok, _ = detect_modern_ux(url, headers or {}, text)

    features = [
        ("platform", platform_guess != "Other/Unknown"),
        ("dtc", dtc_ok),
        ("subscriptions", sub_ok),
        ("bnpl", bnpl_ok),
        ("email_sms", email_ok),
        ("loyalty", loyalty_ok),
        ("quiz", quiz_ok),
        ("omnichannel", omni_ok),
        ("mission", mission_ok),
        ("modern_ux", ux_ok),
    ]

    matched = []
    total = 0
    for name, ok in features:
        if ok:
            total += 1
            matched.append(name)

    return {
        "platform_guess": platform_guess,
        "score": str(total),
        "matched_features": ",".join(matched),
        "bnpl_vendor": bnpl_vendor,
        "subscription_vendor": sub_vendor,
        "loyalty_vendor": loyalty_vendor,
        "email_sms_vendor": email_vendor,
        "notes": "",
    }

# -------------------- Workers ---------------------
async def _worker(url: str, threshold: int, timeout: int) -> Dict[str, str]:
    """
    First pass: httpx
    If blocked (401/403/409/429/503/504) or no HTML → Second pass: Playwright
    If still blocked/no HTML → TRUE_FALSE = "BLOCKED"
    """
    # 1) First pass (httpx)
    fetch = await fetch_url(url, timeout=timeout)
    status = fetch.get("status")
    final_url = fetch.get("final_url") or ""
    headers = fetch.get("headers") or {}
    html = fetch.get("html") or ""
    error = fetch.get("error")
    attempted = fetch.get("attempted") or []

    base_row = {
        "url": url,
        "final_url": final_url,
        "http_status": str(status) if status else "",
        "platform_guess": "",
        "score": "0",
        "TRUE_FALSE": "FALSE",
        "matched_features": "",
        "bnpl_vendor": "",
        "subscription_vendor": "",
        "loyalty_vendor": "",
        "email_sms_vendor": "",
        "notes": "",
    }

    # ---------- YOUR ASK #1: "blocked" guard placement ----------
    # Place this guard RIGHT AFTER the first fetch, BEFORE scoring.
    # But we won't return immediately; we'll try Playwright first.
    blocked_first_pass = (status in BLOCK_STATUSES) or (not html)

    # 2) If blocked or no HTML, try Playwright fallback
    if blocked_first_pass:
        # Attempt rendering the best candidate (prefer final_url if present)
        target = final_url or (attempted[-1] if attempted else url)
        pw = await fetch_with_playwright(target, timeout_ms=15000)
        html2 = pw.get("html") or ""
        final_url2 = pw.get("final_url") or final_url
        if html2:
            # Score from rendered HTML
            details = score_site(final_url2 or url, status, headers, html2)
            base_row.update(details)
            try:
                sc = int(base_row["score"])
            except Exception:
                sc = 0
            base_row["final_url"] = final_url2 or base_row["final_url"]
            base_row["TRUE_FALSE"] = "TRUE" if sc >= threshold else "FALSE"
            base_row["notes"] = (base_row.get("notes") or "") + ("; rendered_with_playwright" if base_row.get("notes") else "rendered_with_playwright")
            return base_row
        else:
            # Still blocked or failed render → mark BLOCKED and return
            base_row["notes"] = f"blocked_or_rate_limited status={status}; tried={attempted[:3]}{'...' if len(attempted)>3 else ''}; pw_error={pw.get('error')}"
            base_row["TRUE_FALSE"] = "BLOCKED"   # <<<<<< your "type blocked" requirement
            return base_row

    # 3) Not blocked and we have HTML → score normally
    details = score_site(final_url or url, status, headers, html)
    base_row.update(details)
    try:
        sc = int(base_row["score"])
    except Exception:
        sc = 0
    base_row["TRUE_FALSE"] = "TRUE" if sc >= threshold else "FALSE"
    return base_row

# -------------------- Orchestration ----------------
async def run(input_csv: str, output_csv: str, concurrency: int, timeout: int, threshold: int):
    urls = read_urls_from_csv(input_csv)
    total = len(urls)
    if total == 0:
        print("No URLs found in input CSV (expect a 'url' header).")
        sys.exit(1)

    print(f"[info] Loaded {total} URLs. concurrency={concurrency}, timeout={timeout}s, threshold={threshold}")
    sem = asyncio.Semaphore(concurrency)
    results: List[Dict[str, str]] = []

    async def sem_task(u: str):
        async with sem:
            return await _worker(u, threshold, timeout)

    tasks = [asyncio.create_task(sem_task(u)) for u in urls]

    completed = 0
    next_checkpoint = CHECKPOINT_EVERY

    for t in asyncio.as_completed(tasks):
        row = await t
        results.append(row)
        completed += 1
        status = row.get("http_status") or "-"
        platform = row.get("platform_guess") or "-"
        note = row.get("notes") or ""
        if note and len(note) > 120:
            note = note[:117] + "..."
        print(f"[{completed}/{total}] {row['url']} -> {row['TRUE_FALSE']} (score={row['score']}, status={status}, platform={platform}) {('['+note+']') if note else ''}")

        if completed >= next_checkpoint:
            tmp_path = output_csv.replace(".csv", f".partial_{completed}.csv")
            write_results_to_csv(tmp_path, results)
            print(f"[checkpoint] Wrote {tmp_path}")
            next_checkpoint += CHECKPOINT_EVERY

    write_results_to_csv(output_csv, results)
    print(f"[done] Results written to: {output_csv}")

# -------------------- Main ------------------------
def main():
    ap = argparse.ArgumentParser(description="DTC Website Checker (single-file). Flags optional.")
    ap.add_argument("--input", default=INPUT_CSV_DEFAULT)
    ap.add_argument("--output", default=OUTPUT_CSV_DEFAULT)
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY_DEFAULT)
    ap.add_argument("--timeout", type=int, default=TIMEOUT_DEFAULT)
    ap.add_argument("--threshold", type=int, default=THRESHOLD_DEFAULT)
    args = ap.parse_args()

    print(f"[run] input={args.input} output={args.output} concurrency={args.concurrency} timeout={args.timeout} threshold={args.threshold}")
    asyncio.run(run(args.input, args.output, args.concurrency, args.timeout, args.threshold))

if __name__ == "__main__":
    main()
