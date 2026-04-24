import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import numpy as np
import time
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError
import signal
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ── Flag stop ────────────────────────────────────
stop_flag = False

def handle_sigint(sig, frame):
    global stop_flag
    print("\n[INFO] CTRL+C — stop dopo questa pagina...")
    stop_flag = True

signal.signal(signal.SIGINT, handle_sigint)


# ── Playwright helpers ───────────────────────────

def goto_with_retry(page, url, retries=3, wait=5000):
    for attempt in range(retries):
        try:
            page.goto(url, timeout=30000)
            return True
        except Exception as e:
            if "ERR_ABORTED" in str(e) and attempt < retries - 1:
                print(f"    [WARN] ERR_ABORTED — riprovo tra {wait//1000}s ({attempt+1}/{retries})...")
                page.wait_for_timeout(wait)
            else:
                raise
    return False


def accept_cookies(page):
    try:
        accept_btn = page.locator(
            "button:has-text('Accept'), button:has-text('Accetta'), "
            "button:has-text('Accetto'), #ccc-notify-accept, "
            "button.ccc-accept-button"
        ).first
        if accept_btn.is_visible():
            accept_btn.click()
            page.wait_for_timeout(1000)
            print("[INFO] Banner cookie chiuso.")
    except Exception:
        pass


def extract_isin_from_href(href):
    try:
        filename = href.split("/")[-1]
        isin = filename.split("-")[0]
        return isin if len(isin) == 12 else None
    except Exception:
        return None


# ── Scraping helpers ─────────────────────────────

def scrape_page(page_num, base_url, sfdr_label=None):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            url = base_url.format(page_num)
            page.goto(url, timeout=30000)
            page.wait_for_selector("table tr:nth-child(2)", timeout=15000)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
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
            print(f"    [WARN] Pagina {page_num} errore: {e}")
            return []
        finally:
            browser.close()


def find_last_page(base_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto("https://www.borsaitaliana.it/borsa/etf.html", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
            page.wait_for_timeout(2000)
            accept_cookies(page)

            page.goto(base_url.format(1), timeout=30000)
            page.wait_for_selector("table tr:nth-child(2)", timeout=15000)
            page.wait_for_timeout(3000)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            max_page = 1
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "page=" in href:
                    try:
                        n = int(href.split("page=")[-1].split("'")[0].strip())
                        max_page = max(max_page, n)
                    except Exception:
                        pass
            for a in soup.find_all("a"):
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    max_page = max(max_page, int(txt))

            print(f"[INFO] Ultima pagina trovata: {max_page}")

            test_url = base_url.format(max_page + 10)
            page.goto(test_url, timeout=15000)
            page.wait_for_timeout(2000)
            html_test = page.content()
            soup_test = BeautifulSoup(html_test, "html.parser")
            table_test = soup_test.find("table")
            rows_test = table_test.find_all("tr")[1:] if table_test else []
            if rows_test:
                print(f"[INFO] Ci sono più di {max_page} pagine — uso fallback 500")
                return 500

            return max_page

        except Exception as e:
            print(f"[WARN] find_last_page errore: {e} — uso fallback 500")
            return 500
        finally:
            browser.close()


def get_headers(base_url, sfdr_label=None):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(base_url.format(1), timeout=30000)
            page.wait_for_selector("table", timeout=15000)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
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


# ── Search functions ─────────────────────────────

def search_deep_fast(workers=5):
    global stop_flag
    stop_flag = False

    base_url = "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=&lang=it&page={}"

    last_page = find_last_page(base_url)
    print(f"[INFO] Scraping max {last_page} pagine con {workers} thread... (CTRL+C per fermare)")

    all_rows = []
    page_num = 1

    while not stop_flag and page_num <= last_page:
        block = list(range(page_num, min(page_num + 20, last_page + 1)))
        block_rows = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(scrape_page, n, base_url): n for n in block}
            for future in as_completed(futures):
                if stop_flag:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                rows = future.result()
                if rows:
                    block_rows.extend(rows)

        all_rows.extend(block_rows)
        print(f"    → pagine {page_num}-{block[-1]}: {len(block_rows)} righe | totale: {len(all_rows)}")

        if len(block_rows) == 0:
            print("[INFO] Blocco vuoto — fine scraping.")
            break

        page_num += 20

    if not all_rows:
        print("[WARNING] No rows collected.")
        return pd.DataFrame()

    headers_row = get_headers(base_url)
    df = pd.DataFrame(
        all_rows,
        columns=headers_row[:len(all_rows[0])] if headers_row else None
    ).drop_duplicates()

    print(f"✓ Totale ETF raccolti: {len(df)}")
    df.to_csv("etf_borsa_italiana.csv", index=False)
    print("✓ Salvato in etf_borsa_italiana.csv")
    return df


def search_greendeep_fast(workers=5):
    global stop_flag
    stop_flag = False

    urls = [
        ("Art. 8", "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=ESG%20ETF%20ART.%208&lang=it&page={}"),
        ("Art. 9", "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=ESG%20ETF%20ART.%209&lang=it&page={}"),
    ]

    all_rows = []
    headers_row = None

    for sfdr_label, base_url in urls:
        if stop_flag:
            break

        last_page = find_last_page(base_url)
        print(f"\n[INFO] Scraping {sfdr_label} — max {last_page} pagine con {workers} thread...")

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

            if len(block_rows) == 0:
                print(f"    [INFO] Blocco vuoto — fine {sfdr_label}.")
                break

            page_num += 20

        if headers_row is None:
            headers_row = get_headers(base_url, sfdr_label)

    if not all_rows:
        print("[WARNING] Nessun ETF trovato.")
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=headers_row[:len(all_rows[0])] if headers_row else None
    ).drop_duplicates()

    print(f"\n✓ Totale ETF SFDR raccolti: {len(df)}")
    print(df['SFDR'].value_counts())
    df.to_csv("etf_sfdr.csv", index=False)
    print("✓ Salvato in etf_sfdr.csv")
    return df


# ── yfinance functions ───────────────────────────

def isin_to_tickers(isin):
    """Borsa Italiana ETFs resolve better with .MI suffix on Yahoo Finance."""
    return [f"{isin}.MI", isin]


def fetch_price(isin):
    for ticker_str in isin_to_tickers(isin):
        try:
            price = yf.Ticker(ticker_str).fast_info.last_price
            if price and price > 0:
                return isin, price
        except Exception:
            continue
    return isin, None


def fetch_storico(isin, period, max_retries=4):
    for ticker_str in isin_to_tickers(isin):
        for attempt in range(max_retries):
            try:
                storico = yf.Ticker(ticker_str).history(period=period, auto_adjust=True)
                if not storico.empty:
                    storico["ISIN"] = isin
                    return isin, storico
                break  # empty, no error → wrong ticker format, try next
            except Exception as e:
                msg = str(e)
                if "Invalid ISIN" in msg:
                    break  # bare ISIN rejected → skip straight to .MI
                if "Too Many Requests" in msg or "rate limit" in msg.lower():
                    if attempt < max_retries - 1:
                        wait = (2 ** (attempt + 1)) + random.uniform(0, 1.5)
                        time.sleep(wait)
                        continue
                print(f"[WARN] {ticker_str}: {e}")
                break
    return isin, None


def download_storico(df, period="6mo", workers=2, batch_size=20, batch_pause=4.0):
    global stop_flag
    stop_flag = False

    if "ISIN" not in df.columns:
        df = df.copy()
        df["ISIN"] = df["Link"].apply(extract_isin_from_href)

    isins = df["ISIN"].dropna().tolist()
    print(f"[INFO] Download storico {period} — {len(isins)} ISINs, {workers} workers, "
          f"batch {batch_size}, pausa {batch_pause}s tra batch")

    all_storico = {}
    completati = 0
    failed = 0

    for batch_start in range(0, len(isins), batch_size):
        if stop_flag:
            print("[INFO] Stop — salvo storico parziale.")
            break

        batch = isins[batch_start : batch_start + batch_size]

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
                completati += 1

        print(f"    → {completati}/{len(isins)} completati | "
              f"scaricati: {len(all_storico)} | falliti: {failed}")

        if batch_start + batch_size < len(isins) and not stop_flag:
            time.sleep(batch_pause)

    if not all_storico:
        print("[WARNING] Nessun storico scaricato.")
        return pd.DataFrame()

    df_storico = pd.concat(all_storico.values())
    df_storico.to_csv(f"storico_{period}.csv")
    print(f"✓ Salvato in storico_{period}.csv ({len(all_storico)} ETF, {failed} falliti)")
    return df_storico


def enrich_with_yfinance(df, workers=4, batch_size=30, batch_pause=3.0):
    global stop_flag
    stop_flag = False

    df = df.copy()
    df["ISIN"] = df["Link"].apply(extract_isin_from_href)
    isins_validi = df["ISIN"].dropna().tolist()

    print(f"[INFO] {len(isins_validi)} ISIN trovati su {len(df)} ETF")
    print(f"[INFO] Download prezzi con {workers} workers, batch {batch_size}...")

    prices = {}
    completati = 0
    failed = 0

    for batch_start in range(0, len(isins_validi), batch_size):
        if stop_flag:
            break

        batch = isins_validi[batch_start : batch_start + batch_size]

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
                completati += 1
                print("Download Eseguito per ",completati,"ETF ...")

        if completati % 100 == 0 or batch_start + batch_size >= len(isins_validi):
            print(f"    → {completati}/{len(isins_validi)} completati | falliti: {failed}")

        if batch_start + batch_size < len(isins_validi) and not stop_flag:
            time.sleep(batch_pause)

    df["YF_Price"] = df["ISIN"].map(prices)
    df.to_csv("etf_enriched.csv", index=False)
    print(f"✓ Salvato in etf_enriched.csv ({len(df)} ETF, {failed} senza prezzo)")
    return df

# ── Analisi ──────────────────────────────────────

def analisi_etf(df_storico, df_enriched=None, risk_free=0.03, benchmark_isin="IE00B4L5Y983"):
    risultati = []

    print(f"[DEBUG] Righe storico: {len(df_storico)}")
    print(f"[DEBUG] ISIN unici: {df_storico['ISIN'].nunique()}")

    # ✅ Filtro adattivo basato sui dati reali
    righe_per_etf = df_storico.groupby("ISIN").size()
    mediana = righe_per_etf.median()
    min_righe = max(10, int(mediana * 0.5))
    print(f"[DEBUG] Mediana righe per ETF: {mediana} — filtro minimo: {min_righe}")

    print("[INFO] Attendo 10s per evitare rate limit...")
    time.sleep(10)

    print(f"[INFO] Scarico benchmark {benchmark_isin}...")
    try:
        bench = yf.Ticker(benchmark_isin).history(period="2y", auto_adjust=True)  # ✅ niente session
        bench_ret = bench["Close"].ffill().pct_change(fill_method=None).dropna()
        print(f"[DEBUG] Benchmark righe: {len(bench_ret)}")
    except Exception as e:
        print(f"[WARN] Benchmark non disponibile: {e}")
        bench_ret = None

    print(f"[INFO] Calcolo metriche per {df_storico['ISIN'].nunique()} ETF...")

    for isin, gruppo in df_storico.groupby("ISIN"):
        close = gruppo["Close"].sort_index().dropna()
        if len(close) < min_righe:  # ✅ filtro adattivo
            continue

        rendimenti = close.ffill().pct_change(fill_method=None).dropna()
        rendimento_annuo = rendimenti.mean() * 252
        volatilita_annua = rendimenti.std() * np.sqrt(252)
        sharpe = (rendimento_annuo - risk_free) / volatilita_annua if volatilita_annua > 0 else None

        rolling_max = close.cummax()
        drawdown = (close - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        beta = None
        alpha = None
        if bench_ret is not None:
            try:
                comune = rendimenti.index.intersection(bench_ret.index)
                if len(comune) > 5:  # ✅ abbassato da 30 a 5
                    r = rendimenti.loc[comune]
                    b = bench_ret.loc[comune]
                    cov = np.cov(r, b)
                    beta = cov[0, 1] / cov[1, 1]
                    alpha = (rendimento_annuo - risk_free) - beta * (b.mean() * 252 - risk_free)
            except Exception:
                pass

        def perf(giorni):
            if len(close) >= giorni:
                return (close.iloc[-1] / close.iloc[-giorni] - 1) * 100
            return None

        risultati.append({
            "ISIN":              isin,
            "Rendimento_annuo%": round(rendimento_annuo * 100, 2),
            "Volatilita_annua%": round(volatilita_annua * 100, 2),
            "Sharpe_Ratio":      round(sharpe, 3) if sharpe else None,
            "Max_Drawdown%":     round(max_drawdown * 100, 2),
            "Beta":              round(beta, 3) if beta else None,
            "Alpha%":            round(alpha * 100, 3) if alpha else None,
            "Perf_1mese%":       round(perf(21), 2) if perf(21) else None,
            "Perf_6mesi%":       round(perf(126), 2) if perf(126) else None,
            "Perf_1anno%":       round(perf(252), 2) if perf(252) else None,
        })

    if not risultati:
        print("[WARNING] Nessun ETF ha superato il filtro minimo.")
        return pd.DataFrame()

    df_analisi = pd.DataFrame(risultati).sort_values("Sharpe_Ratio", ascending=False)

    if df_enriched is not None and "ISIN" in df_enriched.columns:
        cols = ["ISIN", "Nome"]
        if "TER" in df_enriched.columns:
            cols.append("TER")
        if "SFDR" in df_enriched.columns:
            cols.append("SFDR")
        df_meta = df_enriched[cols].drop_duplicates(subset="ISIN")
        df_analisi = df_analisi.merge(df_meta, on="ISIN", how="left")

    print("\n── TOP 10 per Sharpe Ratio ──────────────────")
    print(df_analisi.head(10).to_string(index=False))
    df_analisi.to_csv("analisi_etf.csv", index=False)
    print("\n✓ Salvato in analisi_etf.csv")
    return df_analisi


# ── Main ─────────────────────────────────────────

print("Welcome to the ETF Data Analysis Tool!")
print("Please choose the type of ETFs you are interested in:")
print("1. SFDR Legislated ETFs")
print("2. All ETFs")
choice = input("Enter the number corresponding to your choice: ")

if choice == '1':
    print("You have chosen to see only SFDR Legislated ETFs compliant with Article 8 (GREEN) or Article 9 (DARK GREEN).")
    df = search_greendeep_fast(workers=5)
    if not df.empty:
        if input("Vuoi arricchire con dati yfinance? (y/n): ") == 'y':
            df = enrich_with_yfinance(df)
            if input("Vuoi scaricare lo storico prezzi? (y/n): ") == 'y':
                period = input("Periodo (1d/5d/1mo/3mo/6mo/1y/2y/5y): ") or "6mo"
                df_storico = download_storico(df, period=period, workers=3)
                if not df_storico.empty:
                    if input("Vuoi calcolare Sharpe, Drawdown, Beta, Alpha? (y/n): ") == 'y':
                        analisi_etf(df_storico, df_enriched=df)

elif choice == '2':
    print("You have chosen to see all ETFs.")
    df = search_deep_fast(workers=5)
    if not df.empty:
        if input("Vuoi arricchire con dati yfinance? (y/n): ") == 'y':
            df = enrich_with_yfinance(df)
            if input("Vuoi scaricare lo storico prezzi? (y/n): ") == 'y':
                period = input("Periodo (1d/5d/1mo/3mo/6mo/1y/2y/5y): ") or "6mo"
                df_storico = download_storico(df, period=period, workers=3)
                if not df_storico.empty:
                    if input("Vuoi calcolare Sharpe, Drawdown, Beta, Alpha? (y/n): ") == 'y':
                        analisi_etf(df_storico, df_enriched=df)

else:
    print("Invalid choice. Please enter 1 or 2.")