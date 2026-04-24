import yfinance as yf

# Prendi 3 ISIN reali dal tuo CSV
test_isins = ["IE00B4L5Y983", "LU1681047079", "IE0031442068"]

# Test 1: singolo con .MI
for isin in test_isins:
    t = yf.Ticker(f"{isin}.MI")
    h = t.history(period="5d")
    print(f"{isin}.MI singolo: {len(h)} righe")

# Test 2: singolo senza .MI  
for isin in test_isins:
    t = yf.Ticker(isin)
    h = t.history(period="5d")
    print(f"{isin} singolo: {len(h)} righe")

# Test 3: bulk con .MI
raw = yf.download(" ".join([f"{i}.MI" for i in test_isins]), period="5d", progress=False)
print(f"bulk .MI shape: {raw.shape}")
print(raw.head())

# Test 4: bulk senza .MI
raw2 = yf.download(" ".join(test_isins), period="5d", progress=False)
print(f"bulk bare ISIN shape: {raw2.shape}")
print(raw2.head())