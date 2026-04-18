
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("[INFO] Loading page (JS rendering)...")
        page.goto(url, timeout=30000)

        # Wait until the table is actually injected by JS
        try:
            page.wait_for_selector("table", timeout=15000)
        except Exception:
            print("[WARNING] Table never appeared — check the URL or selector.")
            browser.close()
            return pd.DataFrame()

        html = page.content() 
        browser.close()

    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        print("[WARNING] No table found even after JS render.")
        print(soup.prettify()[:2000])
        return pd.DataFrame()

    table = tables[0]

    # ── Build DataFrame ──────────────────────────────
    headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    if not rows:
        print("[WARNING] Table found but no rows parsed.")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers_row[:len(rows[0])] if headers_row else None)
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

