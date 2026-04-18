
'''                 EXAMPLE                 '''


import pandas as pd

import yfinance as yf

from bs4 import BeautifulSoup

import requests

import numpy as np

import time

from playwright.sync_api import sync_playwright

#MUST EXECUTE 'playwright install' in LINUX terminal in order to load the tool

'''                 USED FUNCTIONS                  '''

def search_greendeep():
    url = "https://www.borsaitaliana.it/borsa/etf.html"
    all_rows = []
    headers_row = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("[INFO] Loading page (JS rendering)...")
        page.goto(url, timeout=30000)

        try:
            page.wait_for_selector("table", timeout=15000)
        except Exception:
            print("[WARNING] Table never appeared.")
            browser.close()
            return pd.DataFrame()

        page_num = 1

        while True:
            print(f"[INFO] Scraping page {page_num}...")
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            table = soup.find("table")
            if not table:
                print("[WARNING] No table found.")
                break

            # Headers solo dalla prima pagina
            if not headers_row:
                headers_row = [th.get_text(strip=True) for th in table.find_all("th")]

            for tr in table.find_all("tr")[1:]:
                cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cols:
                    all_rows.append(cols)

            # ── Cerca bottone next ───────────────────────────
            try:
                next_btn = page.locator(
                    "a[aria-label='Next'], a.next, li.next a, a:has-text('›'), a:has-text('»')"
                ).first

                if next_btn.is_visible() and next_btn.is_enabled():
                    next_btn.click()
                    page.wait_for_timeout(2000)
                    page_num += 1
                else:
                    print("[INFO] Last page reached.")
                    break
            except Exception:
                print("[INFO] No next button found — end of pagination.")
                break

        browser.close()

    if not all_rows:
        print("[WARNING] No rows collected.")
        return pd.DataFrame()

    df = pd.DataFrame(
        all_rows,
        columns=headers_row[:len(all_rows[0])] if headers_row else None
    )
    print(f"✓ Total ETFs collected: {len(df)}")
    print(df.head())
    return df



'''                 CLIENT CHOICE OF DATA                   '''

'''CHOOSING WHETHER THE CLIENT IS INTERESTED IN SEEING SFDR LEGISLATED ETFS OR NOT'''

print("Welcome to the ETF Data Analysis Tool!")
print("Please choose the type of ETFs you are interested in:")
print("1. SFDR Legislated ETFs")
print("2. All ETFs")    
choice = input("Enter the number corresponding to your choice: ")

if choice == '1':
    print("You have chosen to see only SFDR Legislated ETFs compliant with Article 8 (GREEN) or Article 9 (DARK GREEN).")
    search_greendeep()

elif choice == '2':
    print("You have chosen to see all ETFs.")
else:
    print("Invalid choice. Please enter 1 or 2.")   

''''                    DATA PULLING                    '''



'''DATA Management'''

