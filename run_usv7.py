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
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbx9CjePQPtOChHWt69CwjHZdt0uzWqEZlprdl6JKFju3ht4XELyVn7ByjAizvn6eUu1/exec"

CORE_TICKERS = [
    "NVDA", "TSLA", "PLTR", "MSTR", "AMD", "AVGO", "SMCI", "META", 
    "AMZN", "AAPL", "MSFT", "GOOGL", "COIN", "MARA", "CLSK", "VRT", 
    "ANET", "HOOD", "BITF", "LLY", "SOXL"
]

# ==========================================
# 2. 核心算法逻辑
# ==========================================
def calculate_v12_logic(df, spy_df):
    try:
        # 基础数据清洗
        df = df.dropna()
        if len(df) < 60: return None
        
        close = df['Close']
        high = df['High']
        low = df['Low']
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
        if curr_price > close.shift(1).iloc[-1]: score += 1
        if close.iloc[-1] > close.rolling(window=250).max().iloc[-1] * 0.9: score += 1
        
        # RS 强度
        spy_aligned = spy_df.reindex(close.index).ffill()
        rs_line = (close / spy_aligned)
        if rs_line.iloc[-1] > rs_line.tail(20).max() * 0.98: score += 2
        
        adr = float(((high - low) / low).tail(20).mean() * 100)
        
        return {
            "Price": curr_price,
            "Score": score,
            "Trend": trend_str,
            "ADR": adr,
            "RS_Score": round(float(score * 16.6), 1) 
        }
    except Exception as e:
        # print(f"计算出错: {e}")
        return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_v12_engine():
    print(f"🔥 V1000 12.0 启动 | 时间: {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    # 1. 先下载 SPY 指标
    try:
        spy_data = yf.download("SPY", period="2y", progress=False)
        if spy_data.empty:
            print("❌ 关键错误: 无法获取 SPY 数据，程序终止")
            return
        spy_df = spy_data['Close']
    except Exception as e:
        print(f"❌ SPY 下载异常: {e}"); return

    results_data = []

    # 2. 逐个下载股票数据 (这种方式最稳定)
    for t in CORE_TICKERS:
        try:
            # 这里的获取方式更鲁棒
            df_t = yf.download(t, period="2y", progress=False)
            if df_t.empty or len(df_t) < 60:
                print(f"⚠️ {t} 数据不足，跳过")
                continue
            
            res = calculate_v12_logic(df_t, spy_df)
            if res:
                print(f"✅ {t} 解析成功 | 评分: {res['Score']}")
                results_data.append([
                    t, 
                    round(res['Price'], 2), 
                    res['Score'], 
                    res['Trend'], 
                    f"{res['ADR']:.2f}%", 
                    res['RS_Score']
                ])
            else:
                print(f"❓ {t} 不符合逻辑计算条件")
        except Exception as e:
            print(f"❌ {t} 下载/计算失败: {e}")

    # 3. 排序与上传
    if not results_data:
        print("📭 最终无可用标的数据，请检查网络或 yfinance 状态")
        return

    results_data.sort(key=lambda x: x[2], reverse=True)
    
    header = [
        ["🏰 V12 机构枢纽", "更新时间:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), "", "", ""],
        ["代码", "现价", "评分", "趋势状态", "ADR%", "RS强度"]
    ]
    
    final_matrix = header + results_data

    try:
        resp = requests.post(WEBAPP_URL, json=final_matrix, timeout=15)
        if resp.status_code == 200:
            print(f"🚀 同步成功！标的数量: {len(results_data)}")
        else:
            print(f"❌ 同步失败: {resp.text}")
    except Exception as e:
        print(f"❌ 无法连接云端: {e}")

if __name__ == "__main__":
    run_v12_engine()
