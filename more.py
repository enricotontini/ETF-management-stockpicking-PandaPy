import pandas as pd

import yfinance as yf

from bs4 import BeautifulSoup

import requests

import numpy as np

'''CLIENT CHOICE OF DATA'''

'''CHOOSING WETHER THE CLIENT IS INTERESTED IN SEEING SFDR LEGISLATED ETFS OR NOT'''

print("Welcome to the ETF Data Analysis Tool!")
print("Please choose the type of ETFs you are interested in:")
print("1. SFDR Legislated ETFs")
print("2. All ETFs")    
choice = input("Enter the number corresponding to your choice: ")

if choice == '1':
    print("You have chosen to see only SFDR Legislated ETFs.")
    
elif choice == '2':
    print("You have chosen to see all ETFs.")
else:
    print("Invalid choice. Please enter 1 or 2.")   



''''DATA PULLING'''




'''DATA Management'''

