import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import numpy as np
import time
import json
import signal
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import random
from tqdm import tqdm
import glob
import os
import sys

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ═══════════════════════════════════════════════════════════════
#  CONFIGURAZIONE GLOBALE
# ═══════════════════════════════════════════════════════════════

CONFIG = {
    "scraping_workers":    10,
    "enrich_workers":      6,
    "enrich_batch_size":   60,
    "enrich_batch_pause":  1.0,
    "storico_workers":     5,
    "storico_batch_size":  60,
    "storico_batch_pause": 1.0,
    "default_period":      "6mo",
    "benchmark_isin":      "IE00B4L5Y983",
}

# ═══════════════════════════════════════════════════════════════
#  STRUTTURA CARTELLE
# ═══════════════════════════════════════════════════════════════

DIRS = {
    "root":     Path("etf_data"),
    "raw":      Path("etf_data/01_raw"),
    "enriched": Path("etf_data/02_enriched"),
    "storico":  Path("etf_data/03_storico"),
    "analisi":  Path("etf_data/04_analisi"),
    "cache":    Path("etf_data/cache"),
}

def init_dirs():
    for path in DIRS.values():
        path.mkdir(parents=True, exist_ok=True)
    print("✓ Struttura cartelle inizializzata")

def session_label():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ═══════════════════════════════════════════════════════════════
#  STOP FLAG
# ═══════════════════════════════════════════════════════════════

stop_flag = False

def handle_sigint(sig, frame):
    global stop_flag
    print("\n[INFO] CTRL+C ricevuto — stop al prossimo checkpoint...")
    stop_flag = True

signal.signal(signal.SIGINT, handle_sigint)

# ═══════════════════════════════════════════════════════════════
#  EXCHANGE CACHE
# ═══════════════════════════════════════════════════════════════

_exchange_cache: dict = {}
_CACHE_FILE = DIRS["cache"] / "exchange_cache.json"

def _load_exchange_cache():
    global _exchange_cache
    if _CACHE_FILE.exists():
        with open(_CACHE_FILE) as f:
            _exchange_cache = json.load(f)
        print(f"[INFO] Exchange cache: {len(_exchange_cache)} ISIN noti")

def _save_exchange_cache():
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_CACHE_FILE, "w") as f:
        json.dump(_exchange_cache, f)

# ═══════════════════════════════════════════════════════════════
#  HELPERS COMUNI
# ═══════════════════════════════════════════════════════════════

def _ticker_variants(isin: str) -> list[str]:
    """Suffissi exchange in ordine di probabilità per ETF su Borsa Italiana."""
    return [
        f"{isin}.MI",   # Milano          ~747 ETF
        f"{isin}.SG",   # Stuttgart       ETF con exchange=STU
        f"{isin}.DE",   # Xetra
        f"{isin}.F",    # Francoforte
        f"{isin}.PA",   # Euronext Parigi
        f"{isin}.AS",   # Euronext Amsterdam
        f"{isin}.L",    # London
        f"{isin}.SW",   # Swiss Exchange
        f"{isin}.BE",   # Berlino
        isin,           # ISIN nudo — ultimo tentativo
    ]

def extract_isin_from_href(href: str) -> str | None:
    try:
        isin = href.split("/")[-1].split("-")[0]
        return isin if len(isin) == 12 else None
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════
#  PLAYWRIGHT HELPERS
# ═══════════════════════════════════════════════════════════════

def accept_cookies(page):
    try:
        btn = page.locator(
            "button:has-text('Accept'), button:has-text('Accetta'), "
            "button:has-text('Accetto'), #ccc-notify-accept, button.ccc-accept-button"
        ).first
        if btn.is_visible():
            btn.click()
            page.wait_for_timeout(1000)
            print("[INFO] Banner cookie chiuso.")
    except Exception:
        pass


def scrape_page(page_num: int, base_url: str, sfdr_label: str | None = None) -> list:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(base_url.format(page_num), timeout=30000)
            page.wait_for_selector("table tr:nth-child(2)", timeout=15000)
            soup = BeautifulSoup(page.content(), "html.parser")
            table = soup.find("table")
            if not table:
                return []
            rows = []
            for tr in table.find_all("tr")[1:]:
                cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cols:
                    nome_tag = tr.find("td")
                    href = ""
                    if nome_tag and nome_tag.find("a"):
                        cols[0] = nome_tag.find("a").get_text(strip=True)
                        href = nome_tag.find("a").get("href", "")
                    cols.append(href)
                    if sfdr_label:
                        cols.append(sfdr_label)
                    rows.append(cols)
            return rows
        except Exception as e:
            print(f"    [WARN] Pagina {page_num}: {e}")
            return []
        finally:
            browser.close()


def find_last_page(base_url: str) -> int:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto("https://www.borsaitaliana.it/borsa/etf.html", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
            page.wait_for_timeout(1500)
            accept_cookies(page)
            page.goto(base_url.format(1), timeout=30000)
            page.wait_for_selector("table tr:nth-child(2)", timeout=15000)
            page.wait_for_timeout(1500)
            soup = BeautifulSoup(page.content(), "html.parser")
            max_page = 1
            for link in soup.find_all("a", href=True):
                if "page=" in link.get("href", ""):
                    try:
                        n = int(link["href"].split("page=")[-1].split("'")[0].strip())
                        max_page = max(max_page, n)
                    except Exception:
                        pass
            for a in soup.find_all("a"):
                if a.get_text(strip=True).isdigit():
                    max_page = max(max_page, int(a.get_text(strip=True)))
            print(f"[INFO] Ultima pagina trovata: {max_page}")
            page.goto(base_url.format(max_page + 10), timeout=15000)
            page.wait_for_timeout(1000)
            soup_test = BeautifulSoup(page.content(), "html.parser")
            table_test = soup_test.find("table")
            if table_test and table_test.find_all("tr")[1:]:
                print(f"[INFO] Ci sono più di {max_page} pagine — uso fallback 500")
                return 500
            return max_page
        except Exception as e:
            print(f"[WARN] find_last_page: {e} — fallback 500")
            return 500
        finally:
            browser.close()


def get_headers(base_url: str, sfdr_label: str | None = None) -> list | None:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(base_url.format(1), timeout=30000)
            page.wait_for_selector("table", timeout=15000)
            soup = BeautifulSoup(page.content(), "html.parser")
            table = soup.find("table")
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            headers.append("Link")
            if sfdr_label:
                headers.append("SFDR")
            return headers
        except Exception:
            return None
        finally:
            browser.close()

# ═══════════════════════════════════════════════════════════════
#  SCRAPING ETF  →  etf_data/01_raw/
# ═══════════════════════════════════════════════════════════════

def _scrapa_url(base_url: str, sfdr_label: str | None = None, workers: int = 5) -> list:
    last_page = find_last_page(base_url)
    label = sfdr_label or "tutti gli ETF"
    print(f"\n[INFO] Scraping {label} — max {last_page} pagine con {workers} thread...")
    all_rows = []
    page_num = 1
    while not stop_flag and page_num <= last_page:
        block = list(range(page_num, min(page_num + 20, last_page + 1)))
        block_rows = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(scrape_page, n, base_url, sfdr_label): n for n in block}
            for future in as_completed(futures):
                if stop_flag:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                rows = future.result()
                if rows:
                    block_rows.extend(rows)
        all_rows.extend(block_rows)
        print(f"    → pagine {page_num}-{block[-1]}: {len(block_rows)} righe | totale: {len(all_rows)}")
        if not block_rows:
            print(f"    [INFO] Blocco vuoto — fine {label}.")
            break
        page_num += 20
    return all_rows


def search_deep_fast(workers: int | None = None) -> pd.DataFrame:
    global stop_flag
    stop_flag = False
    workers = workers or CONFIG["scraping_workers"]
    base_url = (
        "https://www.borsaitaliana.it/borsa/etf/search.html"
        "?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark="
        "&sectorization=&lang=it&page={}"
    )
    all_rows = _scrapa_url(base_url, workers=workers)
    if not all_rows:
        print("[WARNING] Nessuna riga raccolta.")
        return pd.DataFrame()
    headers = get_headers(base_url)
    df = pd.DataFrame(
        all_rows,
        columns=headers[:len(all_rows[0])] if headers else None
    ).drop_duplicates()
    sl = session_label()
    df.to_csv(DIRS["raw"] / f"tutti_{sl}.csv", index=False)
    df.to_csv(DIRS["raw"] / "tutti_latest.csv", index=False)
    print(f"\n✓ {len(df)} ETF → {DIRS['raw']}/tutti_{sl}.csv")
    return df


def search_greendeep_fast(workers: int | None = None) -> pd.DataFrame:
    global stop_flag
    stop_flag = False
    workers = workers or CONFIG["scraping_workers"]
    sfdr_urls = [
        ("Art. 8", (
            "https://www.borsaitaliana.it/borsa/etf/search.html"
            "?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark="
            "&sectorization=ESG%20ETF%20ART.%208&lang=it&page={}"
        )),
        ("Art. 9", (
            "https://www.borsaitaliana.it/borsa/etf/search.html"
            "?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark="
            "&sectorization=ESG%20ETF%20ART.%209&lang=it&page={}"
        )),
    ]
    all_rows = []
    headers = None
    for sfdr_label, base_url in sfdr_urls:
        if stop_flag:
            break
        rows = _scrapa_url(base_url, sfdr_label=sfdr_label, workers=workers)
        all_rows.extend(rows)
        if headers is None:
            headers = get_headers(base_url, sfdr_label)
    if not all_rows:
        print("[WARNING] Nessun ETF trovato.")
        return pd.DataFrame()
    df = pd.DataFrame(
        all_rows,
        columns=headers[:len(all_rows[0])] if headers else None
    ).drop_duplicates()
    print(f"\n✓ Totale ETF SFDR raccolti: {len(df)}")
    print(df["SFDR"].value_counts().to_string())
    sl = session_label()
    df.to_csv(DIRS["raw"] / f"sfdr_{sl}.csv", index=False)
    df.to_csv(DIRS["raw"] / "sfdr_latest.csv", index=False)
    for label in df["SFDR"].unique():
        slug = label.replace(". ", "").replace(" ", "_").lower()
        df[df["SFDR"] == label].to_csv(DIRS["raw"] / f"{slug}_latest.csv", index=False)
    print(f"✓ Salvato → {DIRS['raw']}/sfdr_{sl}.csv")
    return df

# ═══════════════════════════════════════════════════════════════
#  YFINANCE  →  etf_data/02_enriched/
# ═══════════════════════════════════════════════════════════════

def fetch_price(isin: str) -> tuple:
    for ticker_str in _ticker_variants(isin):
        try:
            price = yf.Ticker(ticker_str).fast_info.last_price
            if price and price > 0:
                return isin, price
        except Exception:
            continue
    return isin, None


def enrich_with_yfinance(
    df: pd.DataFrame,
    workers: int | None = None,
    batch_size: int | None = None,
    batch_pause: float | None = None,
) -> pd.DataFrame:
    global stop_flag
    stop_flag   = False
    workers     = workers     or CONFIG["enrich_workers"]
    batch_size  = batch_size  or CONFIG["enrich_batch_size"]
    batch_pause = batch_pause or CONFIG["enrich_batch_pause"]
    df = df.copy()
    df["ISIN"] = df["Link"].apply(extract_isin_from_href)
    isins = df["ISIN"].dropna().tolist()
    n_batches = -(-len(isins) // batch_size)
    print(f"[INFO] {len(isins)} ISIN — {workers} workers, batch {batch_size}, pausa {batch_pause}s")
    prices: dict = {}
    failed = 0

    with tqdm(total=len(isins), unit="ETF", ncols=80, colour="cyan") as pbar:
        for batch_idx, batch_start in enumerate(range(0, len(isins), batch_size), 1):
            if stop_flag:
                break
            batch = isins[batch_start: batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(fetch_price, isin): isin for isin in batch}
                for future in as_completed(futures):
                    if stop_flag:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    isin, price = future.result()
                    prices[isin] = price
                    if price is None:
                        failed += 1
                    pbar.update(1)
                    pbar.set_postfix(
                        ok=len(isins) - failed,
                        fail=failed,
                        batch=f"{batch_idx}/{n_batches}"
                    )

            df["YF_Price"] = df["ISIN"].map(prices)
            df.to_csv(DIRS["enriched"] / "enriched_latest.csv", index=False)

            if batch_start + batch_size < len(isins) and not stop_flag:
                time.sleep(batch_pause)

    sl = session_label()
    out = DIRS["enriched"] / f"enriched_{sl}.csv"
    df.to_csv(out, index=False)
    df[df["YF_Price"].notna()].to_csv(DIRS["enriched"] / "enriched_con_prezzo.csv", index=False)
    df[df["YF_Price"].isna()].to_csv(DIRS["enriched"] / "enriched_senza_prezzo.csv", index=False)
    print(f"\n✓ {out}")
    print(f"   con prezzo:   {df['YF_Price'].notna().sum()} ETF")
    print(f"   senza prezzo: {df['YF_Price'].isna().sum()} ETF")
    return df

# ═══════════════════════════════════════════════════════════════
#  STORICO  →  etf_data/03_storico/
# ═══════════════════════════════════════════════════════════════

def fetch_storico_raw(isin: str, period: str = "6mo") -> tuple:
    """
    Fallback diretto all'API Yahoo Finance quando yfinance fallisce
    con 'currentTradingPeriod'. Bypassa il parser di yfinance.
    """
    period_map = {
        "1mo": "1mo", "3mo": "3mo", "6mo": "6mo",
        "1y": "1y",   "2y": "2y",   "5y": "5y",
    }
    yf_period = period_map.get(period, "6mo")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    for ticker_str in _ticker_variants(isin):
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_str}"
            f"?interval=1d&range={yf_period}"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            data   = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                continue
            timestamps = result[0].get("timestamp", [])
            closes     = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
            if not timestamps or not closes:
                continue
            df = pd.DataFrame(
                {"Close": closes},
                index=pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None)
            ).dropna()
            if not df.empty:
                df["ISIN"] = isin
                return isin, df
        except Exception:
            continue
    return isin, None


def fetch_storico(isin: str, period: str, max_retries: int = 4) -> tuple:
    """
    1. Se l'ISIN è già in cache come working ticker → va diretto
    2. Prova yfinance su tutti i suffissi con backoff su rate limit
    3. currentTradingPeriod → break solo il loop dei retry, prova suffisso successivo
    4. Fallback finale → API diretta Yahoo (bypassa parser yfinance)
    """
    # ── Shortcut da cache ────────────────────────────────────────
    cached = _exchange_cache.get(isin)
    if cached and cached != "NOT_FOUND":
        ticker_str = f"{isin}.{cached}" if cached != "bare" else isin
        try:
            storico = yf.Ticker(ticker_str).history(period=period, auto_adjust=True)
            if not storico.empty:
                storico["ISIN"] = isin
                return isin, storico
        except Exception:
            pass  # cache stale → ricade nel loop normale

    # ── Loop yfinance su tutti i suffissi ────────────────────────
    for ticker_str in _ticker_variants(isin):
        for attempt in range(max_retries):
            try:
                storico = yf.Ticker(ticker_str).history(period=period, auto_adjust=True)
                if not storico.empty:
                    storico["ISIN"] = isin
                    suffix = ticker_str.split(".")[-1] if "." in ticker_str else "bare"
                    _exchange_cache[isin] = suffix
                    return isin, storico
                break  # empty ma nessun errore → prova suffisso successivo

            except Exception as e:
                msg = str(e)
                if "Invalid ISIN" in msg:
                    break  # questo suffisso non valido → prova il prossimo
                if "currentTradingPeriod" in msg:
                    break  # bug yfinance su questo suffisso → prova il prossimo
                if "Too Many Requests" in msg or "rate limit" in msg.lower():
                    if attempt < max_retries - 1:
                        wait = (2 ** (attempt + 1)) + random.uniform(0, 1.5)
                        time.sleep(wait)
                        continue
                break  # altro errore → prova suffisso successivo

    # ── Fallback: API diretta Yahoo ──────────────────────────────
    isin_out, df_raw = fetch_storico_raw(isin, period)
    if df_raw is not None:
        return isin_out, df_raw

    # Nessuna fonte ha funzionato
    _exchange_cache[isin] = "NOT_FOUND"
    return isin, None


def download_storico(
    df: pd.DataFrame,
    period: str | None = None,
    workers: int | None = None,
    batch_size: int | None = None,
    batch_pause: float | None = None,
) -> pd.DataFrame:
    _load_exchange_cache()
    global stop_flag
    stop_flag   = False
    period      = period      or CONFIG["default_period"]
    workers     = workers     or CONFIG["storico_workers"]
    batch_size  = batch_size  or CONFIG["storico_batch_size"]
    batch_pause = batch_pause or CONFIG["storico_batch_pause"]

    if "ISIN" not in df.columns:
        df = df.copy()
        df["ISIN"] = df["Link"].apply(extract_isin_from_href)

    isins = df["ISIN"].dropna().tolist()
    checkpoint_file = DIRS["storico"] / f"storico_{period}.csv"

    # ── Checkpoint ───────────────────────────────────────────────
    all_storico: dict = {}
    if checkpoint_file.exists():
        try:
            df_cp = pd.read_csv(checkpoint_file, index_col=0, parse_dates=True)
            if "ISIN" in df_cp.columns and not df_cp.empty:
                for isin, g in df_cp.groupby("ISIN"):
                    all_storico[isin] = g
                isins = [i for i in isins if i not in all_storico]
                print(f"[INFO] Checkpoint: {len(all_storico)} già scaricati, {len(isins)} rimanenti")
        except Exception as e:
            print(f"[WARN] Checkpoint non leggibile ({e}) — ricomincio")

    if not isins:
        print("[INFO] Tutti gli ETF già nel checkpoint.")
        return pd.concat(all_storico.values())

    n_batches = -(-len(isins) // batch_size)
    print(f"[INFO] Download {period} — {len(isins)} ISINs, {workers} workers, "
          f"batch {batch_size}, pausa {batch_pause}s")
    failed = 0

    with tqdm(total=len(isins), unit="ETF", ncols=80, colour="green") as pbar:
        for batch_idx, batch_start in enumerate(range(0, len(isins), batch_size), 1):
            if stop_flag:
                print("\n[INFO] Stop — salvo checkpoint.")
                break
            batch = isins[batch_start: batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(fetch_storico, isin, period): isin for isin in batch}
                for future in as_completed(futures):
                    if stop_flag:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    isin, storico = future.result()
                    if storico is not None:
                        all_storico[isin] = storico
                    else:
                        failed += 1
                    pbar.update(1)
                    pbar.set_postfix(ok=len(all_storico), fail=failed,
                                     batch=f"{batch_idx}/{n_batches}")

            # Salva checkpoint + cache dopo ogni batch
            if all_storico:
                pd.concat(all_storico.values()).to_csv(checkpoint_file)
            _save_exchange_cache()

            if batch_start + batch_size < len(isins) and not stop_flag:
                time.sleep(batch_pause)

    if not all_storico:
        print("[WARNING] Nessun storico scaricato.")
        return pd.DataFrame()

    df_storico = pd.concat(all_storico.values())
    sl = session_label()
    out = DIRS["storico"] / f"storico_{period}_{sl}.csv"
    df_storico.to_csv(out)
    df_storico.to_csv(checkpoint_file)
    print(f"\n✓ {out} — {len(all_storico)} ETF, {failed} non trovati su Yahoo")
    return df_storico

# ═══════════════════════════════════════════════════════════════
#  RISK FREE
# ═══════════════════════════════════════════════════════════════

def get_risk_free() -> float:
    for ticker_str, label in [("^EURIBOR3M", "Euribor 3m"), ("^IRX", "T-Bill USA 13w")]:
        try:
            rate = yf.Ticker(ticker_str).fast_info.last_price / 100
            if rate and 0 < rate < 0.15:
                print(f"[INFO] Risk free ({label}): {rate:.2%}")
                return rate
        except Exception:
            continue
    print("[INFO] Risk free fallback: 2.5%")
    return 0.025

# ═══════════════════════════════════════════════════════════════
#  ANALISI  →  etf_data/04_analisi/
# ═══════════════════════════════════════════════════════════════

def analisi_etf(
    df_storico: pd.DataFrame,
    df_enriched: pd.DataFrame | None = None,
    risk_free: float | None = None,
    benchmark_isin: str | None = None,
) -> pd.DataFrame:

    benchmark_isin = benchmark_isin or CONFIG["benchmark_isin"]
    risk_free      = risk_free      or get_risk_free()

    risultati = []
    righe_per_etf = df_storico.groupby("ISIN").size()
    mediana   = righe_per_etf.median()
    min_righe = max(10, int(mediana * 0.5))

    print(f"[INFO] {df_storico['ISIN'].nunique()} ETF | "
          f"mediana righe: {mediana:.0f} | filtro min: {min_righe}")

    # ── Benchmark con retry e più suffissi ───────────────────────
    print(f"\n[INFO] Scarico benchmark {benchmark_isin}...")
    time.sleep(10)
    bench_ret = None
    for ticker_str in _ticker_variants(benchmark_isin):
        for attempt in range(3):
            try:
                bench = yf.Ticker(ticker_str).history(period="2y", auto_adjust=True)
                if not bench.empty:
                    bench_ret = bench["Close"].ffill().pct_change(fill_method=None).dropna()
                    print(f"[INFO] Benchmark ({ticker_str}): {len(bench_ret)} righe ✓")
                    break
            except Exception:
                time.sleep((2 ** (attempt + 1)) + random.uniform(0, 2))
        if bench_ret is not None:
            break

    if bench_ret is None:
        print("[WARN] Benchmark non disponibile — Beta/Alpha/IR saranno NaN")

    # ── Calcolo metriche ─────────────────────────────────────────
    print("[INFO] Calcolo metriche...\n")
    outlier_skip = 0

    for isin, gruppo in df_storico.groupby("ISIN"):
        close = gruppo["Close"].sort_index().dropna()
        if len(close) < min_righe:
            continue

        ret        = close.ffill().pct_change(fill_method=None).dropna()
        rend_annuo = ret.mean() * 252
        vol_annua  = ret.std()  * np.sqrt(252)

        # Filtra outlier — dati chiaramente corrotti
        if abs(rend_annuo) > 5.0 or vol_annua > 2.0:
            outlier_skip += 1
            continue

        # ── Sharpe ───────────────────────────────────────────────
        sharpe = (rend_annuo - risk_free) / vol_annua if vol_annua > 0 else None

        # ── Sortino ──────────────────────────────────────────────
        ret_neg      = ret[ret < 0]
        downside_vol = ret_neg.std() * np.sqrt(252) if len(ret_neg) > 1 else None
        sortino      = (rend_annuo - risk_free) / downside_vol if downside_vol and downside_vol > 0 else None

        # ── Max Drawdown e Calmar ─────────────────────────────────
        max_dd = ((close - close.cummax()) / close.cummax()).min()
        calmar = rend_annuo / abs(max_dd) if max_dd != 0 else None

        # ── Omega ────────────────────────────────────────────────
        soglia   = risk_free / 252
        guadagni = ret[ret > soglia] - soglia
        perdite  = soglia - ret[ret < soglia]
        omega    = guadagni.sum() / perdite.sum() if perdite.sum() > 0 else None

        # ── VaR e CVaR 95% ───────────────────────────────────────
        var_95  = float(np.percentile(ret, 5))
        cvar_95 = float(ret[ret <= var_95].mean()) if len(ret[ret <= var_95]) > 0 else None

        # ── Ulcer Index e Pain Ratio ──────────────────────────────
        dd_pct      = (close - close.cummax()) / close.cummax() * 100
        ulcer_index = float(np.sqrt((dd_pct ** 2).mean()))
        pain_ratio  = rend_annuo * 100 / ulcer_index if ulcer_index > 0 else None

        # ── Winrate ──────────────────────────────────────────────
        winrate = round(float((ret > 0).mean() * 100), 1)

        # ── Beta, Alpha, Tracking Error, Information Ratio, R² ───
        beta = alpha = tracking_error = information_ratio = r_squared = None
        if bench_ret is not None:
            try:
                ret_idx   = ret.index.tz_localize(None) if ret.index.tzinfo else ret.index
                bench_idx = bench_ret.index.tz_localize(None) if bench_ret.index.tzinfo else bench_ret.index
                ret_c     = ret.copy();       ret_c.index   = ret_idx
                bench_c   = bench_ret.copy(); bench_c.index = bench_idx
                comune    = ret_c.index.intersection(bench_c.index)
                if len(comune) > 5:
                    r, b  = ret_c.loc[comune], bench_c.loc[comune]
                    cov   = np.cov(r, b)
                    beta  = cov[0, 1] / cov[1, 1]
                    alpha = (rend_annuo - risk_free) - beta * (b.mean() * 252 - risk_free)
                    diff_ret       = r.values - b.values
                    tracking_error = float(np.std(diff_ret, ddof=1) * np.sqrt(252))
                    if tracking_error > 0:
                        information_ratio = alpha / tracking_error
                    corr      = np.corrcoef(r.values, b.values)[0, 1]
                    r_squared = corr ** 2
            except Exception:
                pass

        # ── Momentum ─────────────────────────────────────────────
        def perf(gg: int) -> float | None:
            return round((close.iloc[-1] / close.iloc[-gg] - 1) * 100, 2) if len(close) >= gg else None

        risultati.append({
            "ISIN":                isin,
            # Rendimento
            "Rendimento_annuo%":   round(rend_annuo * 100, 2),
            "Perf_1mese%":         perf(21),
            "Perf_6mesi%":         perf(126),
            "Perf_1anno%":         perf(252),
            # Rischio
            "Volatilita_annua%":   round(vol_annua * 100, 2),
            "Max_Drawdown%":       round(max_dd * 100, 2),
            "VaR_95%_giorno":      round(var_95 * 100, 2),
            "CVaR_95%_giorno":     round(cvar_95 * 100, 2) if cvar_95 is not None else None,
            "Ulcer_Index":         round(ulcer_index, 4),
            # Rendimento / Rischio
            "Sharpe_Ratio":        round(sharpe, 3)            if sharpe            is not None else None,
            "Sortino_Ratio":       round(sortino, 3)           if sortino           is not None else None,
            "Calmar_Ratio":        round(calmar, 3)            if calmar            is not None else None,
            "Omega_Ratio":         round(omega, 3)             if omega             is not None else None,
            "Pain_Ratio":          round(pain_ratio, 3)        if pain_ratio        is not None else None,
            # Vs Benchmark
            "Beta":                round(beta, 3)              if beta              is not None else None,
            "Alpha%":              round(alpha * 100, 3)       if alpha             is not None else None,
            "R_Squared":           round(r_squared, 3)         if r_squared         is not None else None,
            "Tracking_Error%":     round(tracking_error * 100, 3) if tracking_error is not None else None,
            "Information_Ratio":   round(information_ratio, 3) if information_ratio is not None else None,
            # Altro
            "Winrate%":            winrate,
        })

    if outlier_skip > 0:
        print(f"[INFO] {outlier_skip} ETF scartati per dati anomali")

    if not risultati:
        print("[WARNING] Nessun ETF ha superato il filtro.")
        return pd.DataFrame()

    df_analisi = pd.DataFrame(risultati).sort_values("Sharpe_Ratio", ascending=False)

    # ── Merge metadati ───────────────────────────────────────────
    if df_enriched is not None and "ISIN" in df_enriched.columns:
        meta_cols = ["ISIN"] + [c for c in ["Nome", "TER", "SFDR", "YF_Price"]
                                if c in df_enriched.columns]
        df_analisi = df_analisi.merge(
            df_enriched[meta_cols].drop_duplicates("ISIN"),
            on="ISIN", how="left"
        )

    sl = session_label()

    # ── Salvataggio completo ──────────────────────────────────────
    df_analisi.to_csv(DIRS["analisi"] / f"analisi_completa_{sl}.csv", index=False)
    df_analisi.to_csv(DIRS["analisi"] / "analisi_latest.csv", index=False)

    # Top 50 per ogni indice rendimento/rischio
    for col, slug in [
        ("Sharpe_Ratio",      "top50_sharpe"),
        ("Sortino_Ratio",     "top50_sortino"),
        ("Calmar_Ratio",      "top50_calmar"),
        ("Omega_Ratio",       "top50_omega"),
        ("Pain_Ratio",        "top50_pain"),
        ("Information_Ratio", "top50_information_ratio"),
    ]:
        if col in df_analisi.columns:
            df_analisi.dropna(subset=[col]).sort_values(col, ascending=False).head(50).to_csv(
                DIRS["analisi"] / f"{slug}_{sl}.csv", index=False
            )

    # Filtri qualitativi
    med_vol = df_analisi["Volatilita_annua%"].median()
    filtri = {
        "rend_positivo":     df_analisi["Rendimento_annuo%"] > 0,
        "bassa_volatilita":  df_analisi["Volatilita_annua%"] < med_vol,
        "alto_winrate":      df_analisi["Winrate%"] > 55,
        "basso_drawdown":    df_analisi["Max_Drawdown%"] > -10,
        "basso_ulcer":       df_analisi["Ulcer_Index"] < df_analisi["Ulcer_Index"].median(),
        "difensivi":         (df_analisi["Beta"].fillna(999) < 0.5) &
                             (df_analisi["Rendimento_annuo%"] > 0),
        "migliori_assoluti": (
            (df_analisi["Sharpe_Ratio"].fillna(-999)  > 1.0) &
            (df_analisi["Sortino_Ratio"].fillna(-999) > 1.0) &
            (df_analisi["Rendimento_annuo%"] > 0) &
            (df_analisi["Max_Drawdown%"] > -15)
        ),
    }

    for nome, mask in filtri.items():
        subset = df_analisi[mask]
        if len(subset) > 0:
            subset.to_csv(DIRS["analisi"] / f"filtro_{nome}_{sl}.csv", index=False)

    # Filtro per SFDR
    if "SFDR" in df_analisi.columns:
        for sfdr_label in df_analisi["SFDR"].dropna().unique():
            slug = sfdr_label.replace(". ", "").replace(" ", "_").lower()
            df_analisi[df_analisi["SFDR"] == sfdr_label].to_csv(
                DIRS["analisi"] / f"analisi_{slug}_{sl}.csv", index=False
            )

    # ── Stampa riepilogo ─────────────────────────────────────────
    cols_print = ["ISIN", "Nome", "Sharpe_Ratio", "Sortino_Ratio", "Calmar_Ratio",
                  "Rendimento_annuo%", "Volatilita_annua%", "Max_Drawdown%",
                  "Winrate%", "SFDR"]
    cols_print = [c for c in cols_print if c in df_analisi.columns]

    print("── TOP 10 per Sharpe Ratio ──────────────────────────")
    print(df_analisi[cols_print].head(10).to_string(index=False))
    print(f"\n✓ File salvati in {DIRS['analisi']}/")
    print(f"   analisi_completa_{sl}.csv  ({len(df_analisi)} ETF)")
    for nome, mask in filtri.items():
        n = int(mask.sum())
        if n > 0:
            print(f"   filtro_{nome}: {n} ETF")

    return df_analisi

# ═══════════════════════════════════════════════════════════════
#  SELEZIONE STORICO INTERATTIVA
# ═══════════════════════════════════════════════════════════════

def scegli_o_scarica_storico(df: pd.DataFrame) -> pd.DataFrame:
    storici = sorted(DIRS["storico"].glob("storico_*.csv"))
    if storici:
        print("\n── Storici disponibili ──────────────────────")
        for i, f in enumerate(storici, 1):
            mtime   = datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M")
            size_kb = f.stat().st_size // 1024
            print(f"  {i}. {f.name}  ({mtime}, {size_kb} KB)")
        n = len(storici)
        print(f"  {n+1}. Riprendi download interrotto")
        print(f"  {n+2}. Scarica nuovo (sovrascrive checkpoint)")
        print("─────────────────────────────────────────────")
        scelta = input(f"Scegli [1-{n+2}]: ").strip()
        try:
            idx = int(scelta) - 1
            if 0 <= idx < n:
                print(f"[INFO] Carico {storici[idx].name}...")
                df_s = pd.read_csv(storici[idx], index_col=0, parse_dates=True)
                print(f"✓ {df_s['ISIN'].nunique()} ETF, {len(df_s)} righe")
                return df_s
            elif idx == n:
                period = input(f"Periodo [{CONFIG['default_period']}]: ").strip() or CONFIG["default_period"]
                return download_storico(df, period=period)
            elif idx == n + 1:
                period = input(f"Periodo [{CONFIG['default_period']}]: ").strip() or CONFIG["default_period"]
                cp = DIRS["storico"] / f"storico_{period}.csv"
                if cp.exists():
                    cp.unlink()
                    print("[INFO] Checkpoint rimosso — ricomincio da zero")
                return download_storico(df, period=period)
        except (ValueError, IndexError):
            print("[WARN] Scelta non valida.")

    period = input(f"Periodo [{CONFIG['default_period']}]: ").strip() or CONFIG["default_period"]
    return download_storico(df, period=period)

# ═══════════════════════════════════════════════════════════════
#  DEBUG
# ═══════════════════════════════════════════════════════════════

def debug_falliti(df: pd.DataFrame, n: int = 10) -> None:
    """Testa i primi n ISIN non scaricati per capire quale suffisso funziona."""
    if "ISIN" not in df.columns:
        df = df.copy()
        df["ISIN"] = df["Link"].apply(extract_isin_from_href)
    scaricati: set = set()
    for f in DIRS["storico"].glob("storico_*.csv"):
        try:
            df_cp = pd.read_csv(f, index_col=0, parse_dates=True)
            if "ISIN" in df_cp.columns:
                scaricati.update(df_cp["ISIN"].unique())
        except Exception:
            pass
    falliti = list(dict.fromkeys(i for i in df["ISIN"].dropna() if i not in scaricati))
    print(f"Totale falliti unici: {len(falliti)} — testo i primi {n}:\n")
    trovati = 0
    for isin in falliti[:n]:
        print(f"── {isin} ──")
        # Test yfinance su tutti i suffissi
        yf_ok = False
        for ticker_str in _ticker_variants(isin):
            try:
                storico = yf.Ticker(ticker_str).history(period="5d", auto_adjust=True)
                righe   = len(storico)
                status  = "✅ YF" if righe > 0 else "❌"
                print(f"  {ticker_str}: {status} | righe={righe}")
                if righe > 0:
                    trovati += 1
                    yf_ok = True
                    break
            except Exception as e:
                print(f"  {ticker_str}: ERRORE — {str(e)[:60]}")
        # Test API diretta
        if not yf_ok:
            _, raw = fetch_storico_raw(isin, "5d")
            if raw is not None:
                print(f"  API diretta: ✅ DATI | righe={len(raw)}")
                trovati += 1
            else:
                print(f"  API diretta: ❌ nessun dato")
        print()
    print(f"Recuperabili: {trovati}/{min(n, len(falliti))}")

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    init_dirs()

    print("\n╔══════════════════════════════════════════════╗")
    print("║       ETF Data Analysis Tool                 ║")
    print("║       Fonte: Borsa Italiana + yfinance       ║")
    print("╚══════════════════════════════════════════════╝\n")

    print("Tipo di ETF:")
    print("  1. Solo SFDR Art.8 / Art.9  (Green / Dark Green)")
    print("  2. Tutti gli ETF")
    choice = input("\nScelta [1/2]: ").strip()

    if choice == "1":
        print("\n[INFO] SFDR Art.8 + Art.9 selezionati.")
        df = search_greendeep_fast()
    elif choice == "2":
        print("\n[INFO] Tutti gli ETF selezionati.")
        df = search_deep_fast()
    else:
        print("[ERROR] Scelta non valida.")
        return

    if df.empty:
        print("[ERROR] Nessun ETF trovato.")
        return

    if input("\nArricchire con prezzi yfinance? [y/n]: ").strip().lower() == "y":
        df = enrich_with_yfinance(df)

    if input("\nScaricare storico prezzi? [y/n]: ").strip().lower() == "y":
        df_storico = scegli_o_scarica_storico(df)
        if not df_storico.empty:
            if input("\nCalcolare metriche? [y/n]: ").strip().lower() == "y":
                analisi_etf(df_storico, df_enriched=df)


if __name__ == "__main__":
    # python etf_tool.py debug   → lancia debug_falliti
    # python etf_tool.py         → lancia main
    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        init_dirs()
        raw = DIRS["raw"] / "sfdr_latest.csv"
        if not raw.exists():
            raw = DIRS["raw"] / "tutti_latest.csv"
        if raw.exists():
            debug_falliti(pd.read_csv(raw), n=10)
        else:
            print("[ERROR] Nessun file raw trovato. Esegui prima lo scraping.")
    else:
        main()