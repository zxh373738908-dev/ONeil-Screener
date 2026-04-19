import yfinance as yf
import pandas as pd
import datetime
import requests
import warnings
import numpy as np
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# 1. 核心配置
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzQ2REEAG-DuyhbygkXeNlBvAcmVDjIK1IBauAjoSqLH22chYCZrzf-vBBmYYN7nUU/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

def get_perf(series, days):
    try:
        if len(series) < days + 1: return 0.0
        return ((float(series.iloc[-1]) / float(series.iloc[-(days+1)])) - 1) * 100
    except: return 0.0

def process_ticker(symbol, spy_data):
    try:
        tk = yf.Ticker(symbol)
        # 使用 2y 确保能覆盖 2024-12-31 和 120日均线
        df = tk.history(period="2y")
        if df.empty or len(df) < 130: return None
        
        df.index = df.index.tz_localize(None)
        close = df['Close'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # 1. 1D% 与 多周期表现
        p1d = ((curr_price / float(close.iloc[-2])) - 1) * 100
        p5d, p20d, p60d, p120d = get_perf(close, 5), get_perf(close, 20), get_perf(close, 60), get_perf(close, 120)
        
        # 2. YTD 计算 (From 2024-12-31)
        # 寻找最接近 2024-12-31 的价格
        ytd_target = pd.Timestamp("2024-12-31")
        if ytd_target in close.index:
            ytd_price = close.loc[ytd_target]
        else:
            ytd_price = close.asof(ytd_target)
        
        ytd_perf = ((curr_price / ytd_price) - 1) * 100 if ytd_price else 0.0

        # 3. 相对强度 (vs SPY)
        r20 = p20d - get_perf(spy_data, 20)
        r60 = p60d - get_perf(spy_data, 60)
        r120 = p120d - get_perf(spy_data, 120)

        # 4. 60-Day Trend
        ma60 = close.rolling(60).mean()
        slope = (ma60.iloc[-1] - ma60.iloc[-10]) / ma60.iloc[-10] * 100
        trend = "📈Strong Up" if slope > 1.5 else ("📉Down" if slope < -1.5 else "➡️Side")

        # 5. 评分与动作
        ma20, ma50 = close.rolling(20).mean().iloc[-1], close.rolling(50).mean().iloc[-1]
        ema10 = close.ewm(span=10).mean().iloc[-1]
        score = 0
        is_s2 = (curr_price > ema10 > ma20 > ma50)
        if is_s2: score += 3
        if r20 > 0: score += 1
        if r60 > 0: score += 1
        vol_ratio = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        if vol_ratio > 1.1: score += 1

        action = "🚀 STRONG BUY" if score >= 5 else ("⚖️ HOLD/ADD" if score >= 3 else "WAIT")
        if curr_price < ma20: action = "⚠️ REDUCE"
        resonance = "🔥TRIPLE" if (is_s2 and vol_ratio >= 1.15 and r20 > 0) else "No"

        return {
            "symbol": symbol, "industry": tk.info.get('industry', 'N/A'),
            "score": score, "action": action, "resonance": resonance,
            "p1d": p1d, "trend": trend, "adr": f"{((df['High']-df['Low'])/df['Low']).tail(20).mean()*100:.2f}%",
            "vol_ratio": round(vol_ratio, 2), "bias": f"{((curr_price-ma20)/ma20)*100:.2f}%",
            "mkt_cap": f"{tk.info.get('marketCap', 0)/1e9:.1f}B",
            "price": round(curr_price, 2), "ytd": f"{ytd_perf:.2f}%",
            "r20": r20, "r60": r60, "r120": r120,
            "p5d": p5d, "p20d": p20d, "p60d": p60d, "p120d": p120d,
            "above_ma50": curr_price > ma50
        }
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

def run_v21_engine():
    print(f"🚀 [V21.1 修复版] 启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    # 修复 SPY 数据获取
    spy_df = yf.download("SPY", period="2y", progress=False)
    if isinstance(spy_df.columns, pd.MultiIndex):
        spy_df.columns = spy_df.columns.get_level_values(0)
    spy_close = spy_df['Close'].astype(float)
    
    vix_df = yf.download("^VIX", period="1d", progress=False)
    vix = float(vix_df['Close'].iloc[-1]) if not vix_df.empty else 0.0
    
    raw_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy_close) for t in CORE_TICKERS]
        for f in futures:
            res = f.result()
            if res: raw_results.append(res)

    if not raw_results:
        print("❌ 未获取到任何有效数据，任务终止")
        return

    # --- 计算 REL 排名 ---
    def calculate_ranks(key_name):
        vals = [r[key_name] for r in raw_results]
        # 计算百分比排名
        return {r['symbol']: (sum(1 for v in vals if v < r[key_name]) / len(vals)) * 100 for r in raw_results}

    rel5 = calculate_ranks('p5d')
    rel20 = calculate_ranks('p20d')
    rel60 = calculate_ranks('p60d')
    rel120 = calculate_ranks('p120d')

    # --- 构造数据行 ---
    final_rows = []
    for r in raw_results:
        s = r['symbol']
        final_rows.append([
            s, r['industry'], r['score'], f"{r['p1d']:.2f}%", r['trend'], r['action'], r['resonance'],
            r['adr'], r['vol_ratio'], r['bias'], r['mkt_cap'],
            round(r['score'] * 16.6, 1), # Rank
            round(rel5[s], 1), round(rel20[s], 1), round(rel60[s], 1), round(rel120[s], 1), # REL
            round(r['r20'], 2), round(r['r60'], 2), round(r['r120'], 2),
            r['price'], r['ytd']
        ])

    final_rows.sort(key=lambda x: (x[2], x[16]), reverse=True)
    
    # --- 仪表盘 ---
    header = ["Ticker", "Industry", "Score", "1D%", "60D Trend", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "Rank", "REL5", "REL20", "REL60", "REL120", "R20", "R60", "R120", "Price", "From 2024-12-31"]
    
    breadth = (sum(1 for r in raw_results if r['above_ma50']) / len(raw_results)) * 100
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    
    row1 = ["🏰 [V21.1 终极深度修复版]", "", "", "", "更新时间(BJ):", now, "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    row2 = ["市场天气:", "☀️" if breadth > 60 else "☁️", "", "", "核心池多头:", f"{breadth:.1f}%", "VIX指数:", f"{vix:.2f}", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    row3 = ["策略雷达:", "🚀 爆发 / 🌀 VCP / 💎 核心", "", "", "共振说明:", "≥3 红色 / =2 紫色", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    
    final_matrix = [row1, row2, row3, header] + final_rows

    try:
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=30)
        print(f"✨ 云端同步完成 | 有效标的: {len(raw_results)} | 反馈: {resp.text}")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v21_engine()
