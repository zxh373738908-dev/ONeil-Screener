import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbx0DtHo2LQG0AXe-k2N_es6Fk1U_0FQ20Em3853FIwTKguy_reYihVBbzBNnO3swVFd/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL"
]

def calculate_v12_logic(df, spy_df):
    try:
        # --- 核心修复：处理多重索引和时区 ---
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = df.index.tz_localize(None)
        
        df = df.dropna(subset=['Close'])
        if len(df) < 60: return None
        
        close = df['Close'].astype(float)
        high = df['High'].astype(float)
        low = df['Low'].astype(float)
        curr_price = float(close.iloc[-1])
        
        # --- 均线计算 ---
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        
        # --- 趋势判定 ---
        is_s2 = curr_price > ema10 > ma20 > sma50
        is_bull_side = curr_price > sma50 and not is_s2
        trend_str = "🏆S2主升浪" if is_s2 else ("✅多头震荡" if is_bull_side else "❌破位/弱势")
        
        # --- 评分系统 ---
        score = 0
        if is_s2: score += 2
        if len(close) > 1 and curr_price > close.iloc[-2]: score += 1
        if close.iloc[-1] > close.rolling(window=250).max().iloc[-1] * 0.9: score += 1
        
        # --- RS 强度 (对齐时区) ---
        spy_c = spy_df.astype(float)
        spy_aligned = spy_c.reindex(close.index).ffill()
        rs_line = (close / spy_aligned).dropna()
        if not rs_line.empty and rs_line.iloc[-1] > rs_line.tail(20).max() * 0.98: 
            score += 2
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Price": curr_price, "Score": score, "Trend": trend_str, "ADR": adr,
            "RS_Score": round(float(score * 16.6), 1) 
        }
    except Exception as e:
        # print(f"DEBUG: 逻辑报错 {e}") # 调试用
        return None

def run_v12_engine():
    print(f"🔥 V1000 12.0 加固版 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    try:
        spy_raw = yf.download("SPY", period="2y", progress=False)
        if isinstance(spy_raw.columns, pd.MultiIndex):
            spy_raw.columns = spy_raw.columns.get_level_values(0)
        spy_raw.index = spy_raw.index.tz_localize(None)
        spy_df = spy_raw['Close']
    except:
        print("❌ SPY 数据失败"); return

    results_data = []
    for t in CORE_TICKERS:
        try:
            df_t = yf.download(t, period="2y", progress=False)
            res = calculate_v12_logic(df_t, spy_df)
            if res:
                print(f"✅ {t} 解析成功 | 评分: {res['Score']} | {res['Trend']}")
                results_data.append([t, round(res['Price'], 2), res['Score'], res['Trend'], f"{res['ADR']:.2f}%", res['RS_Score']])
            else:
                print(f"❓ {t} 数据未对齐/计算跳过")
        except:
            print(f"❌ {t} 下载失败")

    if not results_data:
        print("📭 无可用数据"); return

    results_data.sort(key=lambda x: x[2], reverse=True)
    header = [
        ["🏰 V12 机构枢纽", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "", "", ""],
        ["代码", "现价", "评分", "趋势状态", "ADR%", "RS强度"]
    ]
    
    try:
        resp = requests.post(WEBAPP_URL, json=header + results_data, timeout=15)
        print(f"🚀 同步成功！写入数量: {len(results_data)}")
    except Exception as e:
        print(f"❌ 云端失败: {e}")

if __name__ == "__main__":
    run_v12_engine()
