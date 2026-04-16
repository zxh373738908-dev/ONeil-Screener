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
# 1. 配置中心 (V12.0 机构高胜率版)
# ==========================================
# 已更新为您提供的最新 URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbx9CjePQPtOChHWt69CwjHZdt0uzWqEZlprdl6JKFju3ht4XELyVn7ByjAizvn6eUu1/exec"

# 核心监控池：涵盖 AI、半导体、加密货币、权重蓝筹
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
        
        # --- 均线计算 ---
        ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
        ma20 = close.rolling(window=20).mean().iloc[-1]
        sma50 = close.rolling(window=50).mean().iloc[-1]
        
        # --- S2主升浪判定逻辑 ---
        # 🏆 S2 = 价格在10日线上 + 10日线>20日线 + 20日线>50日线
        is_s2 = curr_price > ema10 > ma20 > sma50
        # ✅ 多头震荡 = 价格在50日线上，但均线未完全多头排列
        is_bull_side = curr_price > sma50 and not is_s2
        
        trend_str = "🏆S2主升浪" if is_s2 else ("✅多头震荡" if is_bull_side else "❌破位/弱势")
        
        # --- 机构评分系统 (0-6分) ---
        score = 0
        if is_s2: score += 2
        if curr_price > close.shift(1).iloc[-1]: score += 1 # 今日上涨
        if close.iloc[-1] > close.rolling(window=250).max().iloc[-1] * 0.9: score += 1 # 处于高点附近
        
        # 相对强度 (RS) 判断
        spy_aligned = spy_df.reindex(close.index).ffill()
        rs_line = (close / spy_aligned)
        if rs_line.iloc[-1] > rs_line.tail(20).max() * 0.98: score += 2 # RS处于近期高位
        
        # ADR 计算 (波动率)
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Price": curr_price,
            "Score": score,
            "Trend": trend_str,
            "ADR": adr,
            "RS_Score": round(float(score * 16.6), 1) 
        }
    except:
        return None

# ==========================================
# 3. 主引擎：抓取、计算、同步
# ==========================================
def run_v12_engine():
    now_str = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"🔥 V1000 12.0 机构高胜率版启动 | 时间: {now_str}")
    
    try:
        # 批量获取数据，提高速度
        data = yf.download(CORE_TICKERS, period="2y", group_by='ticker', progress=False)
        spy_df = yf.download("SPY", period="2y", progress=False)['Close']
    except Exception as e:
        print(f"❌ 雅虎数据源获取失败: {e}"); return

    results_data = []
    
    # 遍历所有代码进行逻辑分析
    for t in CORE_TICKERS:
        try:
            df_t = data[t].dropna()
            if df_t.empty: continue
            
            res = calculate_v12_logic(df_t, spy_df)
            if res:
                print(f"✅ {t} 完成 | 评分: {res['Score']} | 趋势: {res['Trend']}")
                results_data.append([
                    t, 
                    round(res['Price'], 2), 
                    res['Score'], 
                    res['Trend'], 
                    f"{res['ADR']:.2f}%", 
                    res['RS_Score']
                ])
        except Exception as e:
            print(f"⚠️ {t} 分析出错: {e}")
            continue

    # --- 排序逻辑：按评分降序排列 ---
    results_data.sort(key=lambda x: x[2], reverse=True)

    # --- 构造发往 Google 的矩阵 ---
    # 强制每一行都有 6 列（与结果数据列数一致）
    header = [
        ["🏰 V12 机构枢纽", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "", "", ""],
        ["代码", "现价", "评分", "趋势状态", "ADR%", "RS强度"]
    ]
    
    final_matrix = header + results_data

    # --- 同步到云端 ---
    try:
        # 使用 json.dumps 确保格式严谨
        payload = json.loads(json.dumps(final_matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        
        if resp.status_code == 200:
            print(f"🚀 云端同步成功！已更新至 us Screener | 标的数量: {len(results_data)}")
        else:
            print(f"❌ 同步失败，响应码: {resp.status_code} | 内容: {resp.text}")
    except Exception as e:
        print(f"❌ 无法连接到 Google App Script: {e}")

if __name__ == "__main__":
    run_v12_engine()
