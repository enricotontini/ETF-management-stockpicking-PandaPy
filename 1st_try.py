import yfinance as yf
import pandas as pd

def get_top_etf_by_countervalue():
    # List of popular ETF tickers (US market)
    etf_tickers = ['SPY', 'QQQ', 'IWM', 'VTI', 'VXUS', 'BND', 'VNQ', 'VWO']
    
    etf_data = []
    for ticker in etf_tickers:
        try:
            data = yf.Ticker(ticker).history(period='5d')
            if not data.empty:
                last_row = data.iloc[-1]
                volume = last_row['Volume']
                close = last_row['Close']
                countervalue = volume * close
                etf_data.append({
                    'Ticker': ticker,
                    'Countervalue': countervalue,
                    'Volume': volume,
                    'Close': close
                })
        except Exception as e:
            print(f"Error retrieving data for {ticker}: {e}")
    
    df = pd.DataFrame(etf_data)
    
    if df.empty:
        print("No ETF data retrieved. Market may be closed or data unavailable.")
        return
    
    # Sort by Countervalue in descending order
    df_sorted = df.sort_values(by='Countervalue', ascending=False)
    print(df_sorted.head(10))  # Print top 10 ETFs

if __name__ == "__main__":
    get_top_etf_by_countervalue()