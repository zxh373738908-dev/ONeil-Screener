import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (V13.1 稳定版)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxL1JyJN81ZHaLvMKb0VHl8ddYLYpdj0C1qSm7FjTz_DuQvUexI0-L2NnDEWLqYuw3t/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

# ==========================================
# 2. 核心分析插件
# ==========================================
def get_perf(series, days):
    try:
        if len(series) < days + 1: return 0.0
        start_val = float(series.iloc[-(days+1)])
        end_val = float(series.iloc[-1])
        return ((end_val / start_val) - 1) * 100
    except: return 0.0

def process_ticker(symbol, spy_data):
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period="1y")
        if df.empty or len(df) < 65: return None
        
        # 强制格式转换
        df.index = df.index.tz_localize(None)
        close = df['Close'].astype(float)
        high = df['High'].astype(float)
        low = df['Low'].astype(float)
        vol = df['Volume'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # 指标计算
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ema10 = float(close.ewm(span=10, adjust=False).mean().iloc[-1])
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        vol_ratio = float(vol.iloc[-1] / vol.tail(20).mean())
        bias = ((curr_price - ma20) / ma20) * 100
        
        # 涨幅与RS
        p5d, p20d, p60d = get_perf(close, 5), get_perf(close, 20), get_perf(close, 60)
        r20 = p20d - get_perf(spy_data, 20)
        r60 = p60d - get_perf(spy_data, 60)

        # 评分逻辑
        score = 0
        is_s2 = (curr_price > ema10 > ma20 > ma50)
        if is_s2: score += 3
        elif curr_price > ma20: score += 1
        if r20 > 0: score += 1
        if r60 > 0: score += 1
        if vol_ratio > 1.1 and curr_price > float(close.iloc[-2]): score += 1
        
        # 动作判定
        action = "WAIT"
        if score >= 5: action = "🚀STRONG BUY"
        elif score >= 3: action = "⚖️HOLD/ADD"
        if curr_price < ma20: action = "⚠️REDUCE"
        
        resonance = "No"
        if is_s2 and vol_ratio > 1.2 and r20 > 5: resonance = "🔥TRIPLE"

        info = tk.info
        return [
            symbol, info.get('industry', 'N/A'), score, action, resonance,
            round(adr, 2), round(vol_ratio, 2), round(bias, 2),
            f"{info.get('marketCap', 0)/1e9:.1f}B", round(score * 16.6, 1),
            "Yes" if info.get('optionsExpirationDates') else "No",
            round(curr_price, 2), f"{p5d:.2f}%", f"{p20d:.2f}%",
            f"{p60d:.2f}%", round(r20, 2), round(r60, 2)
        ]
    except: return None

# ==========================================
# 3. 主引擎
# ==========================================
def run_v13_terminal():
    print(f"🏰 V13 终端启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    spy_raw = yf.download("SPY", period="1y", progress=False)
    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy_raw.columns = spy_raw.columns.get_level_values(0)
    spy_data = spy_raw['Close'].astype(float)
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy_data) for t in CORE_TICKERS]
        for f in futures:
            res = f.result()
            if res:
                print(f"✅ {res[0]} 处理完成")
                results.append(res)

    results.sort(key=lambda x: (x[2], x[15]), reverse=True)
    header = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
    final_matrix = [header] + results

    try:
        response = requests.post(WEBAPP_URL, json=final_matrix, timeout=20)
        print(f"🎉 云端同步完成 | 反馈: {response.text}")
    except Exception as e:
        print(f"⚠️ 同步失败: {e}")

if __name__ == "__main__":
    run_v13_terminal()
