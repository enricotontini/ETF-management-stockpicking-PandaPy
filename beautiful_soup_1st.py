
'''                 EXAMPLE                 '''


import pandas as pd

import yfinance as yf

from bs4 import BeautifulSoup

import requests

import numpy as np

import time

'''                 USED FUNCTIONS                  '''

def search_greendeep():

    url = "https://www.borsaitaliana.it/borsa/etf.html"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        page = requests.get(url, headers=headers, timeout=10)
        page.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Could not reach Borsa Italiana: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(page.text, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        print("[WARNING] No table found on the page.")
        print(soup.prettify()[:2000])
        return pd.DataFrame()

    table = tables[0]

    #        Build DataFrame        #
    headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    if not rows:
        print("[WARNING] Table found but no rows parsed — page may be JS-rendered.")
        print(soup.prettify()[:2000])
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

