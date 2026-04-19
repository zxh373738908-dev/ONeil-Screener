import yfinance as yf
import pandas as pd
import datetime
import requests
import warnings
import numpy as np
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# 1. 核心配置 (已填入你最新的 URL)
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwOeFrqRcFb-MzMYe61qMwV36gqRMlyEmI7Mvjn_FdwsBVmNXL805kr0iT7ySr2G2Db/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

YF_HEADERS = {'User-Agent': 'Mozilla/5.0'}

def get_perf(series, days):
    try:
        if len(series) < days + 1: return 0.0
        return ((float(series.iloc[-1]) / float(series.iloc[-(days+1)])) - 1) * 100
    except: return 0.0

def process_ticker(symbol, spy_data):
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period="2y") # 必须 2y 以计算 YTD 和 120D
        if df.empty or len(df) < 130: return None
        
        df.index = df.index.tz_localize(None)
        close = df['Close'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # 1. 指标
        p1d = ((curr_price / float(close.iloc[-2])) - 1) * 100
        p5d, p20d, p60d, p120d = get_perf(close, 5), get_perf(close, 20), get_perf(close, 60), get_perf(close, 120)
        
        # 2. YTD (From 2024-12-31)
        ytd_price = close.asof(pd.Timestamp("2024-12-31"))
        ytd_perf = ((curr_price / ytd_price) - 1) * 100 if ytd_price else 0.0

        # 3. 相对强度与趋势
        r20, r60, r120 = p20d - get_perf(spy_data, 20), p60d - get_perf(spy_data, 60), p120d - get_perf(spy_data, 120)
        ma60 = close.rolling(60).mean()
        slope = (ma60.iloc[-1] - ma60.iloc[-10]) / ma60.iloc[-10] * 100
        trend = "📈Strong Up" if slope > 1.5 else ("📉Down" if slope < -1.5 else "➡️Side")

        # 4. 评分
        ma20, ma50 = close.rolling(20).mean().iloc[-1], close.rolling(50).mean().iloc[-1]
        score = 0
        if curr_price > close.ewm(span=10).mean().iloc[-1] > ma20 > ma50: score += 3
        if r20 > 0: score += 1
        if r60 > 0: score += 1
        vol_ratio = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        if vol_ratio > 1.1: score += 1

        action = "🚀 STRONG BUY" if score >= 5 else ("⚖️ HOLD/ADD" if score >= 3 else "WAIT")
        if curr_price < ma20: action = "⚠️ REDUCE"

        return {
            "symbol": symbol, "industry": tk.info.get('industry', 'N/A'),
            "score": score, "p1d": p1d, "trend": trend, "action": action,
            "resonance": "🔥TRIPLE" if (score >= 5 and vol_ratio >= 1.15) else "No",
            "adr": f"{((df['High']-df['Low'])/df['Low']).tail(20).mean()*100:.2f}%",
            "vol": round(vol_ratio, 2), "bias": f"{((curr_price-ma20)/ma20)*100:.2f}%",
            "cap": f"{tk.info.get('marketCap', 0)/1e9:.1f}B",
            "p5d": p5d, "p20d": p20d, "p60d": p60d, "p120d": p120d,
            "r20": r20, "r60": r60, "r120": r120, "price": round(curr_price, 2), "ytd": ytd_perf,
            "above50": curr_price > ma50
        }
    except: return None

def run_v21_engine():
    print(f"🚀 [V21.1 21列对齐版] 启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    spy = yf.download("SPY", period="2y", progress=False)['Close'].astype(float)
    vix = float(yf.download("^VIX", period="1d", progress=False)['Close'].iloc[-1])
    
    raw = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy) for t in CORE_TICKERS]
        for f in futures:
            res = f.result(); 
            if res: raw.append(res)

    if not raw: return

    # 计算排名
    def get_ranks(key):
        vals = [r[key] for r in raw]
        return {r['symbol']: (sum(1 for v in vals if v < r[key]) / len(raw)) * 100 for r in raw}

    r5, r20, r60, r120 = get_ranks('p5d'), get_ranks('p20d'), get_ranks('p60d'), get_ranks('p120d')

    final_rows = []
    for r in raw:
        s = r['symbol']
        final_rows.append([
            s, r['industry'], r['score'], f"{r['p1d']:.2f}%", r['trend'], r['action'], r['resonance'],
            r['adr'], r['vol'], r['bias'], r['cap'], round(r['score']*16.6, 1),
            round(r5[s], 1), round(r20[s], 1), round(r60[s], 1), round(r120[s], 1),
            round(r['r20'], 2), round(r['r60'], 2), round(r['r120'], 2),
            r['price'], f"{r['ytd']:.2f}%"
        ])

    final_rows.sort(key=lambda x: (x[2], x[16]), reverse=True)
    header = ["Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", "REL5", "REL20", "REL60", "REL120", "R20", "R60", "R120", "Price", "From 2024-12-31"]
    
    breadth = (sum(1 for r in raw if r['above50']) / len(raw)) * 100
    row1 = ["🏰 [V21.1 终极深度对齐版]", "", "", "", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    row2 = ["市场天气:", "☀️" if breadth > 60 else "☁️", "", "", "多头占比:", f"{breadth:.1f}%", "VIX指数:", f"{vix:.2f}", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    row3 = ["策略雷达:", "🚀爆发 / 🌀VCP / 💎核心", "", "", "说明:", "数据已对齐", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    
    try:
        resp = requests.post(WEBAPP_URL, json=[row1, row2, row3, header] + final_rows, timeout=30)
        print(f"✨ 同步反馈: {resp.text}")
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v21_engine()
