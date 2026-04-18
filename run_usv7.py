import yfinance as yf
import pandas as pd
import datetime
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ==========================================
# 配置中心 - 已填入你提供的新 URL
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzLtA3bCfW5xdmgc2u5rOPQ0mvNMmpKYmBGJQDYpz3SHD1_bjoHwo5SloeUVaKnB9HD/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

def get_perf(series, days):
    try:
        return ((float(series.iloc[-1]) / float(series.iloc[-(days+1)])) - 1) * 100
    except: return 0.0

def process_ticker(symbol, spy_data):
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period="1y")
        if df.empty: return None
        
        close = df['Close'].astype(float)
        curr_price = float(close.iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ema10 = float(close.ewm(span=10).mean().iloc[-1])
        
        vol_ratio = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        adr = float(((df['High'] - df['Low']) / df['Low']).tail(20).mean() * 100)
        
        p20d = get_perf(close, 20)
        r20 = p20d - get_perf(spy_data, 20)
        r60 = get_perf(close, 60) - get_perf(spy_data, 60)

        # 评分
        score = 0
        if curr_price > ema10 > ma20 > ma50: score += 3
        if r20 > 0: score += 1
        if vol_ratio > 1.1: score += 1
        
        action = "🚀 STRONG BUY" if score >= 5 else ("⚖️ HOLD/ADD" if score >= 3 else "WAIT")
        if curr_price < ma20: action = "⚠️ REDUCE"

        return [
            symbol, tk.info.get('industry', 'N/A'), score, action, 
            "🔥TRIPLE" if (score >= 5 and vol_ratio > 1.2) else "No",
            round(adr, 2), round(vol_ratio, 2), round(((curr_price-ma20)/ma20)*100, 2),
            f"{tk.info.get('marketCap', 0)/1e9:.1f}B", round(score*16.6, 1),
            "Yes" if tk.info.get('optionsExpirationDates') else "No",
            round(curr_price, 2), f"{get_perf(close, 5):.2f}%", f"{p20d:.2f}%", 
            f"{get_perf(close, 60):.2f}%", round(r20, 2), round(r60, 2)
        ]
    except: return None

def run_v20_engine():
    print("📡 开启全美股扫描 (V20.0)...")
    spy = yf.download("SPY", period="1y", progress=False)['Close']
    vix = yf.download("^VIX", period="1d", progress=False)['Close'].iloc[-1]
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy) for t in CORE_TICKERS]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    results.sort(key=lambda x: (x[2], x[15]), reverse=True)
    
    # 构造 V20 仪表盘数据
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    row1 = ["🏰 [V20.0 终极共振对齐版]", "", "", "", "更新时间:", now, "", "", "", "", "", "", "", "", "", "", ""]
    row2 = ["市场天气:", "☀️", "", "", "VIX指数:", f"{vix:.2f}", "", "", "", "", "", "", "", "", "", "", ""]
    row3 = ["策略雷达:", "🚀 爆发 / 🌀 VCP / 💎 核心", "", "", "共振说明:", "≥3 红色", "", "", "", "", "", "", "", "", "", "", ""]
    row4 = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]

    final_matrix = [row1, row2, row3, row4] + results

    try:
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=30)
        print(f"✨ 云端反馈: {resp.text}")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v20_engine()
