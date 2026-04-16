import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import math
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (V12.0 机构增强版)
# ==========================================
# ！！！请在此处粘贴您的 Google 脚本 URL ！！！
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwP3SYIaCsxCRSWe-sI0wUcB3E9QGKH3vFZsvJUB8gh4oih_lktGBZv-WXGSHjylazG/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL"
]

# ==========================================
# 2. 机构级趋势与评分引擎
# ==========================================
def calculate_v12_logic(df, spy_df):
    try:
        if len(df) < 60: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
        curr_price = float(close.iloc[-1])
        
        # --- 指标计算 ---
        ema10 = close.ewm(span=10, adjust=False).mean()
        ma20 = close.rolling(window=20).mean()
        sma50 = close.rolling(window=50).mean()
        
        # --- S2主升浪判定逻辑 ---
        # 🏆 S2 = 价格在10日线上 + 10日线>20日线 + 20日线>50日线
        is_s2 = curr_price > ema10.iloc[-1] > ma20.iloc[-1] > sma50.iloc[-1]
        # ✅ 多头震荡 = 价格在50日线上，但均线未完全多头排列
        is_bull_side = curr_price > sma50.iloc[-1] and not is_s2
        
        trend_str = "🏆S2主升浪" if is_s2 else ("✅多头震荡" if is_bull_side else "❌破位/弱势")
        
        # --- 机构评分系统 (0-6分) ---
        score = 0
        if is_s2: score += 2
        if curr_price > close.shift(1).iloc[-1]: score += 1 # 今日上涨
        if close.iloc[-1] > close.rolling(window=250).max().iloc[-1] * 0.9: score += 1 # 近年高点附近
        
        # 相对强度 (RS)
        spy_aligned = spy_df.reindex(close.index).ffill()
        rs_line = (close / spy_aligned)
        if rs_line.iloc[-1] > rs_line.tail(20).max() * 0.98: score += 2 # RS强度
        
        # ADR 过滤 (V12版硬指标)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Price": curr_price,
            "Score": score,
            "Trend": trend_str,
            "ADR": adr,
            "RS_Score": round(float(score * 16.6), 1) # 转换成百分制供参考
        }
    except:
        return None

# ==========================================
# 3. 执行主引擎
# ==========================================
def run_v12_engine():
    print(f"🔥 V1000 12.0 机构高胜率版启动 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    try:
        data = yf.download(CORE_TICKERS, period="2y", group_by='ticker', progress=False)
        spy_df = yf.download("SPY", period="2y", progress=False)['Close']
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    results_for_upload = []
    
    # 构建表头
    header = [
        ["🏰 V12.0 机构枢纽", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
        ["代码", "现价", "评分", "趋势状态", "ADR%", "RS强度"]
    ]

    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            res = calculate_v12_logic(df_t, spy_df)
            if res:
                print(f"✅ {t} 完成 | 现价: {res['Price']:.2f} | 评分: {res['Score']} | 趋势: {res['Trend']}")
                results_for_upload.append([
                    t, 
                    round(res['Price'], 2), 
                    res['Score'], 
                    res['Trend'], 
                    f"{res['ADR']:.2f}%", 
                    res['RS_Score']
                ])
        except:
            continue

    # 按照评分从高到低排序
    results_for_upload.sort(key=lambda x: x[2], reverse=True)

    # 发送到 Google Sheets
    final_matrix = header + results_for_upload
    try:
        if "你的真实链接" in WEBAPP_URL:
            print("⚠️ 警告: 尚未配置 WEBAPP_URL，跳过同步。")
        else:
            resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=10)
            if resp.status_code == 200:
                print("🚀 云端同步成功！")
            else:
                print(f"❌ 同步异常: {resp.text}")
    except Exception as e:
        print(f"❌ 同步失败: {e}")

if __name__ == "__main__":
    run_v12_engine()
