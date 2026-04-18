import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbxL1JyJN81ZHaLvMKb0VHl8ddYLYpdj0C1qSm7FjTz_DuQvUexI0-L2NnDEWLqYuw3t/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

# ==========================================
# 2. 核心逻辑插件 (兼容修复)
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
        # 显式拉取历史数据
        df = tk.history(period="1y")
        if df.empty or len(df) < 65: return None
        
        # --- 修复核心：强制转换 Series 为单列并抹除时区 ---
        df.index = df.index.tz_localize(None)
        close = df['Close'].astype(float)
        high = df['High'].astype(float)
        low = df['Low'].astype(float)
        vol = df['Volume'].astype(float)
        
        curr_price = float(close.iloc[-1])
        
        # --- 1. 技术指标计算 (确保为 Scalar) ---
        ma20_series = close.rolling(window=20).mean()
        ma50_series = close.rolling(window=50).mean()
        ema10_series = close.ewm(span=10, adjust=False).mean()
        
        ma20 = float(ma20_series.iloc[-1])
        ma50 = float(ma50_series.iloc[-1])
        ema10 = float(ema10_series.iloc[-1])
        prev_close = float(close.iloc[-2])
        
        # ADR (20日平均波幅)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        # 量比
        avg_vol = vol.tail(20).mean()
        vol_ratio = float(vol.iloc[-1] / avg_vol) if avg_vol != 0 else 0
        # 乖离率
        bias = ((curr_price - ma20) / ma20) * 100
        
        # --- 2. 相对强度 ---
        p5d = get_perf(close, 5)
        p20d = get_perf(close, 20)
        p60d = get_perf(close, 60)
        
        spy_20p = get_perf(spy_data, 20)
        spy_60p = get_perf(spy_data, 60)
        r20 = p20d - spy_20p
        r60 = p60d - spy_60p

        # --- 3. V13 多因子评分系统 ---
        score = 0
        # S2结构判定
        is_s2 = (curr_price > ema10) and (ema10 > ma20) and (ma20 > ma50)
        if is_s2: score += 3
        elif curr_price > ma20: score += 1
        
        if r20 > 0: score += 1
        if r60 > 0: score += 1
        if vol_ratio > 1.1 and curr_price > prev_close: score += 1
        
        # --- 4. 动作与共振 ---
        action = "WAIT"
        if score >= 5: action = "🚀STRONG BUY"
        elif score >= 3: action = "⚖️HOLD/ADD"
        if curr_price < ma20: action = "⚠️REDUCE"
        
        resonance = "No"
        if is_s2 and vol_ratio > 1.2 and r20 > 5:
            resonance = "🔥TRIPLE"

        # --- 5. 获取Info ---
        info = tk.info
        industry = info.get('industry', 'N/A')
        mkt_cap = info.get('marketCap', 0) / 1e9
        
        return [
            symbol, industry, score, action, resonance,
            round(adr, 2), round(vol_ratio, 2), round(bias, 2),
            f"{mkt_cap:.1f}B", round(score * 16.6, 1),
            "Yes" if info.get('optionsExpirationDates') else "No",
            round(curr_price, 2), f"{p5d:.2f}%", f"{p20d:.2f}%",
            f"{p60d:.2f}%", round(r20, 2), round(r60, 2)
        ]
    except Exception as e:
        # print(f"Error {symbol}: {e}") # 调试用
        return None

# ==========================================
# 3. 执行主引擎
# ==========================================
def run_v13_terminal():
    print(f"🏰 V13 机构终端启动 | {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    # 修复基准数据获取
    spy_raw = yf.download("SPY", period="1y", progress=False)
    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy_raw.columns = spy_raw.columns.get_level_values(0)
    spy_data = spy_raw['Close'].astype(float)
    
    results = []
    # 使用多线程提升效率
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_ticker, t, spy_data) for t in CORE_TICKERS]
        for f in futures:
            res = f.result()
            if res:
                print(f"✅ {res[0]} 分析完成 | 评分: {res[2]}")
                results.append(res)

    # 排序
    results.sort(key=lambda x: (x[2], x[15]), reverse=True)

    # 构造表头 (17列)
    header = [
        "Ticker", "Industry", "Score", "Action", "Resonance", 
        "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", 
        "Options", "Price", "5D", "20D", "60D", "R20", "R60"
    ]
    
    final_matrix = [header] + results

    # 打印预览
    for row in final_matrix[:5]:
        print(row)

    # 发送云端
    try:
        requests.post(WEBAPP_URL, json=final_matrix, timeout=10)
        print("🎉 云端同步完成")
    except:
        print("⚠️ 云端已离线，仅本地输出")
        
# 发送云端
    try:
        response = requests.post(WEBAPP_URL, json=final_matrix, timeout=15)
        print(f"🎉 云端反馈: {response.text}") # 看到 Success 才算真正成功
    except Exception as e:
        print(f"⚠️ 同步失败原因: {e}")if __name__ == "__main__":
    run_v13_terminal()
