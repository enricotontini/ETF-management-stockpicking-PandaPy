import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests
import numpy as np
import time
from playwright.sync_api import sync_playwright
from playwright._impl._errors import TargetClosedError
import signal
from playwright._impl._errors import TargetClosedError

# ── Flag stop ────────────────────────────────────
stop_flag = False

def handle_sigint(sig, frame):
    global stop_flag
    print("\n[INFO] CTRL+C — stop dopo questa pagina...")
    stop_flag = True

signal.signal(signal.SIGINT, handle_sigint)


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

        try:
            accept_btn = page.locator(
                "button:has-text('Accept'), button:has-text('Accetta'), "
                "button:has-text('Accetto'), #ccc-notify-accept, "
                "button.ccc-accept-button"
            ).first
            if accept_btn.is_visible():
                accept_btn.click()
                print("[INFO] Banner cookie chiuso.")
                page.wait_for_timeout(1000)
        except Exception:
            pass

        page_num = 1

        while not stop_flag:
            try:
                url = base_url.format(page_num)
                print(f"[INFO] Scraping page {page_num}...")
                page.goto(url, timeout=30000)
                page.wait_for_selector("table", timeout=15000)
                page.wait_for_timeout(2000)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                table = soup.find("table")

                if not table:
                    print("[WARNING] No table found.")
                    break

                if not headers_row:
                    headers_row = [th.get_text(strip=True) for th in table.find_all("th")]

                rows_this_page = []
                for tr in table.find_all("tr")[1:]:
                    cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if cols:
                        nome_tag = tr.find("td")
                        if nome_tag and nome_tag.find("a"):
                            cols[0] = nome_tag.find("a").get_text(strip=True)
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
    print(df)
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

        # Cookie
        page.goto("https://www.borsaitaliana.it/borsa/etf.html", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(2000)
        try:
            accept_btn = page.locator(
                "button:has-text('Accept'), button:has-text('Accetta'), "
                "button:has-text('Accetto'), #ccc-notify-accept, "
                "button.ccc-accept-button"
            ).first
            if accept_btn.is_visible():
                accept_btn.click()
                page.wait_for_timeout(1000)
        except Exception:
            pass

        for sfdr_label, base_url in urls:
            if stop_flag:
                break
            print(f"\n[INFO] Scraping {sfdr_label}...")
            page_num = 1

            while not stop_flag:
                try:
                    url = base_url.format(page_num)
                    print(f"    pagina {page_num}...")
                    page.goto(url, timeout=30000)
                    page.wait_for_selector("table", timeout=15000)
                    page.wait_for_timeout(2000)

                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    table = soup.find("table")

                    if not table:
                        break

                    if not headers_row:
                        headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
                        headers_row.append("SFDR")

                    rows_this_page = []
                    for tr in table.find_all("tr")[1:]:
                        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if cols:
                            nome_tag = tr.find("td")
                            if nome_tag and nome_tag.find("a"):
                                cols[0] = nome_tag.find("a").get_text(strip=True)
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

'''                 CLIENT CHOICE OF DATA                   '''

print("Welcome to the ETF Data Analysis Tool!")
print("Please choose the type of ETFs you are interested in:")
print("1. SFDR Legislated ETFs")
print("2. All ETFs")
choice = input("Enter the number corresponding to your choice: ")

if choice == '1':
    print("You have chosen to see only SFDR Legislated ETFs compliant with Article 8 (GREEN) or Article 9 (DARK GREEN).")
    search_greendeep()
elif choice == '2':
    print("You have chosen to see all ETFs under SFDR Legislation.")
    search_deep()
else:
    print("Invalid choice. Please enter 1 or 2.")
