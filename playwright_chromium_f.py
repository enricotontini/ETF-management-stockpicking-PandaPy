import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests
import numpy as np
import time
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError
import signal
import logging
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ── Flag stop ────────────────────────────────────
stop_flag = False

def handle_sigint(sig, frame):
    global stop_flag
    print("\n[INFO] CTRL+C — stop dopo questa pagina...")
    stop_flag = True

signal.signal(signal.SIGINT, handle_sigint)


def goto_with_retry(page, url, retries=3, wait=5000):
    for attempt in range(retries):
        try:
            page.goto(url, timeout=30000)
            return True
        except Exception as e:
            if "ERR_ABORTED" in str(e) and attempt < retries - 1:
                print(f"    [WARN] ERR_ABORTED — riprovo tra {wait//1000}s (tentativo {attempt+1}/{retries})...")
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


def get_scheda_data(soup):
    """Estrae dati dalla scheda ETF."""
    data = {}
    campi = {
        "Codice Isin":            "ISIN",
        "SFDR":                   "SFDR_scheda",
        "Commissioni totali annue": "TER",
        "Emittente":              "Emittente",
        "Dividendi":              "Dividendi",
        "Valuta di Denominazione": "Valuta",
        "Benchmark":              "Benchmark",
        "Stile Benchmark":        "Stile_Benchmark",
        "Area Benchmark":         "Area_Benchmark",
        "Segmento":               "Segmento",
    }
    for td in soup.find_all("td"):
        testo = td.get_text(strip=True)
        if testo in campi:
            next_td = td.find_next_sibling("td")
            if next_td:
                data[campi[testo]] = next_td.get_text(strip=True)
    return data


def search_deep():
    global stop_flag
    stop_flag = False

    base_url = "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=&lang=it&page={}"
    all_rows = []
    headers_row = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("[INFO] Loading page... (premi CTRL+C per fermare)")
        page.goto("https://www.borsaitaliana.it/borsa/etf.html", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(2000)
        accept_cookies(page)

        page_num = 1

        while not stop_flag:
            try:
                url = base_url.format(page_num)
                print(f"[INFO] Scraping page {page_num}...")
                goto_with_retry(page, url)
                page.wait_for_selector("table", timeout=15000)
                page.wait_for_selector("table tr:nth-child(2)", timeout=15000)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                table = soup.find("table")

                if not table:
                    print("[WARNING] No table found.")
                    break

                if not headers_row:
                    headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
                    headers_row.append("Link")

                rows_this_page = []
                for tr in table.find_all("tr")[1:]:
                    cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if cols:
                        nome_tag = tr.find("td")
                        href = ""
                        if nome_tag and nome_tag.find("a"):
                            cols[0] = nome_tag.find("a").get_text(strip=True)
                            href = nome_tag.find("a").get("href", "")
                        cols.append(href)
                        rows_this_page.append(cols)

                if not rows_this_page:
                    print("[INFO] Pagina vuota — fine.")
                    break

                all_rows.extend(rows_this_page)
                print(f"    → righe fin ora: {len(all_rows)}")
                page_num += 1

            except TargetClosedError:
                print("[INFO] Browser chiuso — interruzione.")
                break

        try:
            browser.close()
        except Exception:
            pass

    if not all_rows:
        print("[WARNING] No rows collected.")
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=headers_row[:len(all_rows[0])] if headers_row else None
    )
    print(f"✓ Totale ETF raccolti: {len(df)}")
    df.to_csv("etf_borsa_italiana.csv", index=False)
    print("✓ Salvato in etf_borsa_italiana.csv")
    return df


def search_greendeep():
    all_rows = []
    headers_row = []

    urls = [
        ("Art. 8", "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=ESG%20ETF%20ART.%208&lang=it&page={}"),
        ("Art. 9", "https://www.borsaitaliana.it/borsa/etf/search.html?comparto=ETF&idBenchmarkStyle=&idBenchmark=&indexBenchmark=&sectorization=ESG%20ETF%20ART.%209&lang=it&page={}"),
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto("https://www.borsaitaliana.it/borsa/etf.html", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(2000)
        accept_cookies(page)
        page.wait_for_selector("table tr:nth-child(2)", timeout=15000)
        for sfdr_label, base_url in urls:
            if stop_flag:
                break
            print(f"\n[INFO] Scraping {sfdr_label}...")
            page_num = 1

            while not stop_flag:
                try:
                    url = base_url.format(page_num)
                    print(f"    pagina {page_num}...")
                    goto_with_retry(page, url)
                    page.wait_for_selector("table", timeout=15000)
                    page.wait_for_selector("table tr:nth-child(2)", timeout=15000)

                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    table = soup.find("table")

                    if not table:
                        break

                    if not headers_row:
                        headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
                        headers_row.append("Link")
                        headers_row.append("SFDR")

                    rows_this_page = []
                    for tr in table.find_all("tr")[1:]:
                        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if cols:
                            nome_tag = tr.find("td")
                            href = ""
                            if nome_tag and nome_tag.find("a"):
                                cols[0] = nome_tag.find("a").get_text(strip=True)
                                href = nome_tag.find("a").get("href", "")
                            cols.append(href)
                            cols.append(sfdr_label)
                            rows_this_page.append(cols)

                    if not rows_this_page:
                        print(f"    [INFO] Fine {sfdr_label}.")
                        break

                    all_rows.extend(rows_this_page)
                    print(f"    → righe fin ora: {len(all_rows)}")
                    page_num += 1

                except TargetClosedError:
                    print("[INFO] Browser chiuso — interruzione.")
                    break

        try:
            browser.close()
        except Exception:
            pass

    if not all_rows:
        print("[WARNING] Nessun ETF trovato.")
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=headers_row[:len(all_rows[0])] if headers_row else None
    )
    print(f"\n✓ Totale ETF SFDR raccolti: {len(df)}")
    print(df['SFDR'].value_counts())
    df.to_csv("etf_sfdr.csv", index=False)
    print("✓ Salvato in etf_sfdr.csv")
    return df


def extract_isin_from_href(href):
    try:
        filename = href.split("/")[-1]       # LU1681040496-ETFP.html
        isin = filename.split("-")[0]         # LU1681040496
        return isin if len(isin) == 12 else None
    except Exception:
        return None


def enrich_with_yfinance(df):
    global stop_flag
    stop_flag = False

    df = df.copy()
    df["ISIN"] = df["Link"].apply(extract_isin_from_href)

    isins_validi = df["ISIN"].dropna().tolist()
    print(f"[INFO] {len(isins_validi)} ISIN trovati su {len(df)} ETF")
    print("[INFO] Download prezzi da yfinance... (CTRL+C per fermare)")

    prices = {}
    for i, isin in enumerate(isins_validi):
        if stop_flag:
            print("[INFO] Stop — uso prezzi raccolti fin ora.")
            break
        try:
            ticker = yf.Ticker(isin)
            price = ticker.fast_info.last_price
            prices[isin] = price if price and price > 0 else None
        except Exception:
            prices[isin] = None

        if i % 50 == 0:
            print(f"    → {i}/{len(isins_validi)} completati...")

    df["YF_Price"] = df["ISIN"].map(prices)
    df.to_csv("etf_enriched.csv", index=False)
    print(f"✓ Salvato in etf_enriched.csv ({len(df)} ETF)")
    return df

def download_storico(df, period="1y"):
    global stop_flag
    stop_flag = False

    isins = df["ISIN"].dropna().tolist()
    print(f"[INFO] Download storico {period} per {len(isins)} ETF... (CTRL+C per fermare)")

    all_storico = {}

    for i, isin in enumerate(isins):
        if stop_flag:
            print("[INFO] Stop — salvo storico parziale.")
            break
        try:
            storico = yf.Ticker(isin).history(period=period, auto_adjust=True)
            if not storico.empty:
                storico["ISIN"] = isin
                all_storico[isin] = storico
        except Exception:
            pass

        if i % 50 == 0:
            print(f"    → {i}/{len(isins)} completati...")

    if not all_storico:
        print("[WARNING] Nessun storico scaricato.")
        return pd.DataFrame()

    df_storico = pd.concat(all_storico.values())
    df_storico.to_csv(f"storico_{period}.csv")
    print(f"✓ Salvato in storico_{period}.csv ({len(all_storico)} ETF)")
    return df_storico

'''                 CLIENT CHOICE OF DATA                   '''

print("Welcome to the ETF Data Analysis Tool!")
print("Please choose the type of ETFs you are interested in:")
print("1. SFDR Legislated ETFs")
print("2. All ETFs")
choice = input("Enter the number corresponding to your choice: ")

if choice == '1':
    df = search_greendeep()
    if not df.empty:
        enrich = input("Vuoi arricchire con dati yfinance? (y/n): ")
        if enrich == 'y':
            df = enrich_with_yfinance(df)
            
            storico = input("Vuoi scaricare lo storico prezzi? (y/n): ")
            if storico == 'y':
                period = input("Periodo (1d/5d/1mo/3mo/6mo/1y/2y/5y): ") or "1y"
                download_storico(df, period=period)
elif choice == '2':
    print("You have chosen to see all ETFs.")
    df = search_deep()
    if not df.empty:
        enrich = input("Vuoi arricchire con dati yfinance? (y/n): ")
        if enrich == 'y':
            df = enrich_with_yfinance(df)
            
            storico = input("Vuoi scaricare lo storico prezzi? (y/n): ")
            if storico == 'y':
                period = input("Periodo (1d/5d/1mo/3mo/6mo/1y/2y/5y): ") or "1y"
                download_storico(df, period=period)
else:
    print("Invalid choice. Please enter 1 or 2.")