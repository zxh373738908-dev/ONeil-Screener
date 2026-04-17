import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import requests
import json
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (V13.0 机构优化版)
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbynfNFyXdV_k5mUtKLgczdYDOxy2BSSbGW1FEOYQ7qypg7FbNCxd5NM6OE4bQA8c2uj/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL", "ARM", "MU", "TSM"
]

# ==========================================
# 2. 机构级逻辑引擎 (量价+乖离+趋势)
# ==========================================
def calculate_v13_logic(df, spy_df):
    try:
        # 处理多重索引
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        # 抹除时区
        df.index = df.index.tz_localize(None)
        df = df.dropna(subset=['Close'])
        
        if len(df) < 60: return None
        
        close = df['Close'].astype(float)
        high = df['High'].astype(float)
        low = df['Low'].astype(float)
        volume = df['Volume'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # --- 均线系统 ---
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean()
        ma20_curr = ma20.iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        
        # --- [新] 乖离率: 偏离20日线幅度 ---
        extension = ((curr_price - ma20_curr) / ma20_curr) * 100
        
        # --- [新] 量比: 今日量 vs 20日均量 ---
        avg_vol = volume.tail(20).mean()
        vol_ratio = volume.iloc[-1] / avg_vol
        
        # --- S2 趋势判定 ---
        is_s2 = curr_price > ema10 > ma20_curr > sma50
        is_bull_side = curr_price > sma50 and not is_s2
        trend_str = "🏆S2主升浪" if is_s2 else ("✅多头震荡" if is_bull_side else "❌破位/弱势")
        
        # --- 综合评分 (0-6) ---
        score = 0
        if is_s2: score += 2
        if curr_price > close.iloc[-2]: score += 1
        if close.iloc[-1] > close.rolling(window=250).max().iloc[-1] * 0.9: score += 1
        
        # RS 强度
        spy_c = spy_df.astype(float)
        spy_aligned = spy_c.reindex(close.index).ffill()
        rs_line = (close / spy_aligned).dropna()
        if not rs_line.empty and rs_line.iloc[-1] > rs_line.tail(20).max() * 0.98: 
            score += 2
        
        # ADR 计算
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Price": curr_price,
            "Score": score,
            "Trend": trend_str,
            "ADR": adr,
            "Ext": round(extension, 1),
            "VolR": round(vol_ratio, 1),
            "RS": round(float(score * 16.6), 1)
        }
    except:
        return None

# ==========================================
# 3. 执行主引擎
# ==========================================
def run_v13_engine():
    print(f"🚀 V1000 13.0 增强版启动 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    try:
        spy_raw = yf.download("SPY", period="2y", progress=False)
        if isinstance(spy_raw.columns, pd.MultiIndex): spy_raw.columns = spy_raw.columns.get_level_values(0)
        spy_raw.index = spy_raw.index.tz_localize(None)
        spy_df = spy_raw['Close']
    except:
        print("❌ SPY获取失败"); return

    results = []
    for t in CORE_TICKERS:
        try:
            df_t = yf.download(t, period="2y", progress=False)
            res = calculate_v13_logic(df_t, spy_df)
            if res:
                print(f"✅ {t} 分析完成 (评分:{res['Score']} | 量比:{res['VolR']}x)")
                results.append([
                    t, round(res['Price'], 2), res['Score'], res['Trend'], 
                    f"{res['ADR']:.2f}%", f"{res['VolR']}x", f"{res['Ext']}%", res['RS']
                ])
        except: continue

    if not results: return

    # 排序：评分 > RS强度
    results.sort(key=lambda x: (x[2], x[7]), reverse=True)
    
    # 构造 8 列矩阵 (与 Google 脚本匹配)
    header = [
        ["🏰 V13 机构终端", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "", "", "", "", ""],
        ["代码", "现价", "评分", "趋势状态", "ADR%", "量比(20d)", "乖离率(20d)", "RS强度"]
    ]
    
    try:
        requests.post(WEBAPP_URL, json=header + results, timeout=15)
        print(f"🎉 同步成功！当前 S2 标的数量: {len([r for r in results if '🏆' in r[3]])}")
    except:
        print("❌ 同步云端失败")

if __name__ == "__main__":
    run_v13_engine()
